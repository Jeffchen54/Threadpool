#include "threadpool.h"
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <semaphore.h>
#include "list.h"
struct worker
{
        struct list local_queue;          // Queue held locally by worker thread
        pthread_mutex_t local_queue_lock; // Lock for local queue
        pthread_t thread;                 // Worker's thread (may change later on)
};
struct thread_pool
{
        uintptr_t id;                      // ID to set next worker as
        struct worker *workers;            // Worker threads working for threadpool, unknown number of workers
        struct list global_queue;          // Global queue accessed by all workers
        pthread_mutex_t global_queue_lock; // Lock for global queue
        int worker_count;                  // Number of workers
        bool shutdown;                     // Shutdown flag, true to shut down, false to not
        pthread_mutex_t shutdown_lock;     // Lock that goes together with shutdown flag
        sem_t t_continue;                  // To allow broadcast to go through
};
struct future
{
        struct list_elem elem;        // List tag element
        int curr_queue;              // Current queue future is in, -1 for global queue, <-1 for not set
        fork_join_task_t task;       // Task future will execute
        void *args;                  // arguments for task
        void *result;                // Results from running task
        int state;                   // Current state of task, 0 = queued, 1 = in progress, 2 = finished, -1 for unset
        pthread_mutex_t future_lock; // Lock used to prevent concurrent access to future
        struct thread_pool *pool;    // Threadpool instance that contains/contained future
};
struct workerID
{
        uintptr_t id; // ID of worker
};


static _Thread_local struct workerID *worker_id; // External threads == NULL, Worker threads
                                                 // ID corresponds to worker placement in struct
                                                 // thread_pool's worker* workers.

// static struct queue_elem *queue_static;

static void *worker_thread_job(void *args);

/**
 * Initializes struct worker. This function will only initialize struct fields,
 * no actual worker threads are created.
 * Params:
 *      w: Worker to initialize
 * EXIT FAILURE if arguments failed to initialize
 */
static void worker_init(struct worker *w)
{
    list_init(&w->local_queue);
    pthread_mutex_init(&w->local_queue_lock, NULL);
}

/**
 * Initializes threadpool with nthreads.
 * - Only struct members are initialized
 * - Worker threads are created and started
 *
 * Params:
 *      nthreads: Number of threads for the threadpool
 *      pool: thread_pool to initialize
 * EXIT FAILURE if arguments failed to initialize
 */
static void thread_pool_init(struct thread_pool *pool, int nthreads)
{
    struct worker *w = malloc(nthreads * sizeof(struct worker));
    pool->workers = w;
    if (pool->workers == NULL)
    {
        perror("malloc dynamic allocation failure");
        exit(EXIT_FAILURE);
    }
    list_init(&pool->global_queue);
    pthread_mutex_init(&pool->global_queue_lock, NULL);
    pthread_mutex_init(&pool->shutdown_lock, NULL);
    sem_init(&pool->t_continue, 0, 0);

    pool->worker_count = nthreads;
    pool->shutdown = false;

    // Initializing workers
    for (int i = 0; i < nthreads; i++)
    {
        worker_init(&pool->workers[i]);
    }

    // Starting wrokers
    for (int i = 0; i < nthreads; i++)
    {
        pool->id = i;
        if (pthread_create(&pool->workers[i].thread, NULL, worker_thread_job, pool) != 0)
        {
            perror("Worker thread failed to be created");
            exit(EXIT_FAILURE);
        }
    }
}

/**
 * Initializes future.
 * Struct members (1)curr_queue and (2)state set to default values.
 *      (3)result is set to NULL.
 *
 * Params:
 *      fut: Future to initialize
 *      t: Task future will execute
 *      args: task arguments
 *      pool: threadpool instance that will contain the future
 * EXIT_FAILURE if arguments failed to initialize
 */
static void future_init(struct future *fut, fork_join_task_t t, void *args, struct thread_pool *pool)
{
    fut->curr_queue = -2;
    fut->task = t;
    fut->args = args;
    fut->result = NULL;
    fut->state = -1;
    fut->pool = pool;
    if (pthread_mutex_init(&fut->future_lock, NULL) != 0)
    {
        perror("Lock was not successfully initialized.");
        exit(EXIT_FAILURE);
    }
}

/**
 * Grabs future at the front of the queue while removing it
 *
 * Params:
 *      queue: Queue to get future from
 * Pre: If there is a lock associated with queue, it must be active
 *      before calling this function.
 * Post: Lock is not released upon return if used
 * Return: future at front of queue. NULL if list is empty
 */
static struct future *get_future_front(struct list *queue)
{
    if (list_empty(queue))
    {
        return NULL;
    }
    return list_entry(list_pop_front(queue), struct future, elem);
}

/**
 * Grabs future at the back of the queue while removing it and freeing it
 *
 * Params:
 *      queue: Queue to get future from
 * Pre: If there is a lock associated with queue, it most be active
 *      before calling this function.
 * Post: Lock is not released upon return if used
 * Return: future at back of queue. NULL if list is empty
 */
static struct future *get_future_back(struct list *queue)
{
    if (list_empty(queue))
    {
        return NULL;
    }
    return list_entry(list_pop_back(queue), struct future, elem);
}

/**
 * Frees all queue elements from queue. Does not free
 * struct members from the queue elements.
 *
 * Params:
 *      queue: Queue to erase data from
 * Pre: If queue is associated with a lock, it must be locked before calling
 * Post: queue is now empty and all queue elements free
 */
static void queue_clean_all(struct list *queue)
{
    while (!list_empty(queue))
    {
        list_pop_front(queue);
    }
}

/**
 * THIS FUNCTION COULD PROBABLY BE SPED UP IF LOCATION OF
 * QUEUED FUTURE IS KNOWN
 *
 * Searches all queues for a tasks in the order:
 *      Local queue -> global queue -> other queues from low to high ID
 * Params:
 *      pool: threadpool worker belongs to
 * Pre: no queue locks are held, called by worker thread
 * Post: lock of queue that has an available task is held. No locks held if not found.
 * Returns: queue with available task, <-1 is returned if not found
 */
static int search_all_queues(struct thread_pool *pool)
{
    // Worker verification check
    if (worker_id == NULL)
    {
        perror("External thread attempted searching for task");
        return -2;
    }

    // Look at own queue and global queue first
    pthread_mutex_lock(&pool->workers[worker_id->id].local_queue_lock);
    if (list_empty(&pool->workers[worker_id->id].local_queue) == false)
    {
        return worker_id->id;
    }
    pthread_mutex_unlock(&pool->workers[worker_id->id].local_queue_lock);

    pthread_mutex_lock(&pool->global_queue_lock);
    if (!list_empty(&pool->global_queue))
    {
        return -1;
    }
    pthread_mutex_unlock(&pool->global_queue_lock);

    // Now look at all other queues
    for (int i = 0; i < pool->worker_count; i++)
    {
        pthread_mutex_lock(&pool->workers[i].local_queue_lock);
        if (!list_empty(&pool->workers[i].local_queue))
        {
            return i;
        }
        pthread_mutex_unlock(&pool->workers[i].local_queue_lock);
    }

    // No queues to look at
    return -2;
}

/**
 * Executes a task.
 * - If q matches worker_id->id, task is executed from the front
 * of queue, otherwise, from the back of the queue.
 * - Task is removed from the queue and freed, future is not affected.
 * - Future is updated appropriately.
 * - Locks are used appropriately.
 * Param:
 *      pool: threadpool where task will be run from
 *      q: which queue to get task from. -1 for global queue, >=0 for worker
 * Pre: queue is already locked. worker_id is not NULL (aka must be called only by worker threads)
 * Post: queue is unlocked. No additional locks are set after returning.
 */
static void run_task(struct thread_pool *pool, int q)
{
    // Getting queue and future, unlocks queue lock
    struct future *fut;

    // Global queue
    if (q == -1)
    {
        fut = get_future_back(&pool->global_queue);
        pthread_mutex_unlock(&pool->global_queue_lock);
    }
    else
    {
        // Local queue
        if (q == worker_id->id)
        {
            fut = get_future_front(&pool->workers[worker_id->id].local_queue);
        }
        // Other worker's local queue
        else
        {
            fut = get_future_back(&pool->workers[q].local_queue);
        }
        pthread_mutex_unlock(&pool->workers[q].local_queue_lock);
    }
    // Begin executing future
    pthread_mutex_lock(&fut->future_lock);
    fut->state = 1;
    fut->curr_queue = q;
    fut->result = fut->task(pool, fut->args); // fut invoked as a task
                                              // This means no extra threads used
    fut->state = 2;
    pthread_mutex_unlock(&fut->future_lock);
}

/**
 * Job for worker thread. The worker will have several responsibilities:
 * 1) No tasks in fellow worker queues, local queue, or global queue
 *      -> Wait for queued_cond
 * 2) Received queued_cond signal
 *      -> Check if shutdown flag is triggered first
 *      -> Searches all queues for available tasks (will be good if
 *          signal could be sent with queue location)
 * 3) Finishes task
 *      -> Waits for another task to be added onto the queue or
 *          works on another task.
 * 4) Shutdown on signal. Worker's struct members are freed but worker itself
 *      is not
 *
 * Params:
 *      w:  Worker
 *      pool: threadpool where worker belongs to
 *      id: Worker's position in pool's worker*
 *
 * Returns: NULL
 */
static void *worker_thread_job(void *args)
{
    struct thread_pool *p = (struct thread_pool *)args;

    // Initializing id
    worker_id = malloc(1 * sizeof(struct workerID));
    if (worker_id == NULL)
    {
        perror("worker_id failed to be allocated");
        exit(EXIT_FAILURE);
    }

    worker_id->id = p->id;
    bool sd = false;
    // Loop until shutdown is signalled
    while (!sd)
    {
        // Wait until signal arrives
        sem_wait(&p->t_continue);

        // Check if shutdown is triggered
        pthread_mutex_lock(&p->shutdown_lock);
        if (p->shutdown == true)
        {
            sd = true;
            pthread_mutex_unlock(&p->shutdown_lock);
        }
        else
        {
            // Done here instead of end for faster unlocks, relocks
            pthread_mutex_unlock(&p->shutdown_lock);

            // Once arrived, check if tasks are available, keep working on tasks until none are left
            // search_all_queues auto acquires lock of queue to get task from
            int queue = search_all_queues(p);

            while (queue >= -1)
            {
                // Run acquired task
                run_task(p, queue);

                // Acquire a new task
                queue = search_all_queues(p);
            }
        }
    }

    // Shutdown and clean worker

    // This unlock relock cycle is needed since this function may be active when
    // Workers are still being created
    // pthread_mutex_lock(pool->worker_lock);
    // pthread_mutex_unlock(pool->worker_lock);
    pthread_mutex_lock(&p->workers[worker_id->id].local_queue_lock);
    queue_clean_all(&p->workers[worker_id->id].local_queue);
    pthread_mutex_unlock(&p->workers[worker_id->id].local_queue_lock);

    free(worker_id);
    return NULL;
}

/* Create a new thread pool with no more than n threads. */
struct thread_pool *thread_pool_new(int nthreads)
{
    struct thread_pool *pool = malloc(1 * sizeof(struct thread_pool));
    if (pool == NULL)
    {
        perror("threadpool failed to be allocated");
        exit(EXIT_FAILURE);
    }

    thread_pool_init(pool, nthreads);
    return pool;
}

/*
 * Shutdown this thread pool in an orderly fashion.
 * Tasks that have been submitted but not executed may or
 * may not be executed.
 *
 * Deallocate the thread pool object before returning.
 */
void thread_pool_shutdown_and_destroy(struct thread_pool *pool)
{
    // Flagging
    pthread_mutex_lock(&pool->shutdown_lock);
    pool->shutdown = true;
    pthread_mutex_unlock(&pool->shutdown_lock);

    // Broadcast to threads (Threads should check flag immediately)
    for (int i = 0; i < pool->worker_count; i++)
    {
        sem_post(&pool->t_continue);
    }

    // Destroy queue
    pthread_mutex_lock(&pool->global_queue_lock);
    queue_clean_all(&pool->global_queue);
    pthread_mutex_unlock(&pool->global_queue_lock);

    // Waiting for workers then cleaning their queues
    for (int i = 0; i < pool->worker_count; i++)
    {
        pthread_join(pool->workers[i].thread, NULL);
        queue_clean_all(&pool->workers[i].local_queue);
        pthread_mutex_destroy(&pool->workers[i].local_queue_lock);
    }

    // Freeing threadpool and worker array
    // free(worker_id);
    // free(queue_static);
    free(pool->workers);
    free(pool);
}

/*
 * Submit a fork join task to the thread pool and return a
 * future.  The returned future can be used in future_get()
 * to obtain the result.
 * 'pool' - the pool to which to submit
 * 'task' - the task to be submitted.
 * 'data' - data to be passed to the task's function
 *
 * Returns a future representing this computation.
 */
struct future *thread_pool_submit(
    struct thread_pool *pool,
    fork_join_task_t task,
    void *data)
{
    // No future locks should be needed at this point until it is queued
    struct future *fut = malloc(1 * sizeof(struct future));
    future_init(fut, task, data, pool);
    fut->state = 0;

    // inititalizes and allocates the queue element
    // queue_static = malloc(1 * sizeof(struct queue_elem));

    // If internal task, submit to front of worker's queue
    if (worker_id != NULL)
    {
        fut->curr_queue = worker_id->id;
        pthread_mutex_lock(&pool->workers[worker_id->id].local_queue_lock);
        list_push_front(&pool->workers[worker_id->id].local_queue, &fut->elem);
        pthread_mutex_unlock(&pool->workers[worker_id->id].local_queue_lock);
    }
    // If external, submit to back of global queue
    else
    {
        fut->curr_queue = -1;
        pthread_mutex_lock(&pool->global_queue_lock);
        list_push_front(&pool->global_queue, &fut->elem);
        pthread_mutex_unlock(&pool->global_queue_lock);
    }

    // Broadcast to workers to wake up
    sem_post(&pool->t_continue);

    return fut;
}

/**
 * Removes the target from the queue starting from the front
 * of the queue to the back of the queue. If multiple of the same
 * task exists in the queue, only 1 is removed.
 *
 * Param:
 *      queue: Queue to remove target from
 *      target: future to remove from the queue
 * Pre: If queue is associated with a lock, it must be held before calling.
 *      future lock must be held before calling.
 * Post: future and list elem holding it is removed and list elem is freed. No
 *      locks are removed.
 * Return: true if removed, false if not removed (rare but will happen if task is
 *          removed from queue before queue lock is held).
 */
static bool remove_from_queue(struct list *queue, struct future *target)
{
    for (struct list_elem *e = list_begin(queue); e != list_end(queue); e = list_next(e))
    {
        struct future *item = list_entry(e, struct future, elem);
        if (item == target)
        {
            list_remove(e);
            return true;
        }
    }
    return false;
}

/* Make sure that the thread pool has completed the execution
 * of the fork join task this future represents.
 *
 * Returns the value returned by this task.
 */
void *future_get(struct future *fut)
{
    // If worker and task is still queued, work on the task
    // If task is in progress, work on another task
    if (worker_id != NULL)
    {
        pthread_mutex_lock(&fut->future_lock);

        // Until future is complete, keep running
        while (fut->state != 2)
        {
            // If future not in progress, work on another
            if (fut->state == 0)
            {
                pthread_mutex_unlock(&fut->future_lock);

                // Lock the queue where the task is currently
                pthread_mutex_lock(&fut->pool->workers[fut->curr_queue].local_queue_lock);

                // Remove it from the queue
                bool status = remove_from_queue(&fut->pool->workers[fut->curr_queue].local_queue, fut);
                pthread_mutex_unlock(&fut->pool->workers[fut->curr_queue].local_queue_lock);
                // start working on that task
                if (status == true)
                {
                    fut->state = 1;
                    fut->curr_queue = worker_id->id;
                    fut->result = fut->task(fut->pool, fut->args); // fut invoked as a task
                                                                   // This means no extra threads used
                    fut->state = 2;
                }
            }

            // If task is in progress, work on another task
            else if (fut->state == 1)
            {
                // Unlock future
                pthread_mutex_unlock(&fut->future_lock);

                // Find another task
                int q = search_all_queues(fut->pool);

                // Work on that task
                run_task(fut->pool, q);
            }

            pthread_mutex_lock(&fut->future_lock);
        }
    }

    // If external thread, just wait
    else
    {
        // If future is queued, in progress, wait until it finished
        pthread_mutex_lock(&fut->future_lock);
        while (fut->state != 2)
        {
            pthread_mutex_unlock(&fut->future_lock);
            sleep(0.1);
            pthread_mutex_lock(&fut->future_lock);
        }
    }
    void *result = fut->result;
    pthread_mutex_unlock(&fut->future_lock);

    return result;
}

/* Deallocate this future.  Must be called after future_get() */
void future_free(struct future *fut)
{
    pthread_mutex_destroy(&fut->future_lock);
    free(fut);
}
