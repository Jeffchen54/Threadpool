/**
 * threadpool.h
 *
 * A work-stealing, fork-join thread pool.
 */
#include "list.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <semaphore.h>

/**
 * Used as worker threads.
 */
struct worker
{
        struct list local_queue;          // Queue held locally by worker thread
        pthread_mutex_t local_queue_lock; // Lock for local queue
        pthread_t thread;                 // Worker's thread
};

/*
 * Opaque forward declarations. The actual definitions of these
 * types will be local to your threadpool.c implementation.
 *
 * Has all kinds of locks, however, is necessary since there is a condition
 * and having a universal lock for threadpool and waiting on queued_cond is bad news
 */
struct thread_pool
{
        uintptr_t id;             // ID of worker
        struct worker *workers;            // Worker threads working for threadpool, unknown number of workers
                                           // Worker lock might not be needed as only accessed for queue and queue has own lock
        struct list global_queue;          // Global queue accessed by all workers
        pthread_mutex_t global_queue_lock; // Lock for global queue
        int worker_count;                  // Number of workers
        bool shutdown;                     // Shutdown flag, true to shut down, false to not
        pthread_mutex_t shutdown_lock;     // Lock that goes together with shutdown flag
        sem_t t_continue;                  // Number of available tasks
};

/* A function pointer representing a 'fork/join' task.
 * Tasks are represented as a function pointer to a
 * function.
 * 'pool' - the thread pool instance in which this task
 *          executes
 * 'data' - a pointer to the data provided in thread_pool_submit
 *
 * Returns the result of its computation.
 */
typedef void *(*fork_join_task_t)(struct thread_pool *pool, void *data);

/**
 * Worker ID, used as _Thread_local
 */
struct workerID
{
        uintptr_t id; // ID of worker
};


/**
 * Use as list entries in the queues for both local and global queues
 */
struct queue_elem
{
        struct future *future; // Future stored within the list
        struct list_elem elem; // List tag element
};

struct future
{
        int curr_queue;              // Current queue future is in, -1 for global queue, <-1 for not set
        fork_join_task_t task;       // Task future will execute
        void *args;                  // arguments for task
        void *result;                // Results from running task
        int state;                   // Current state of task, 0 = queued, 1 = in progress, 2 = finished, -1 for unset
        pthread_mutex_t future_lock; // Lock used to prevent concurrent access to future
        struct thread_pool *pool;    // Threadpool instance that contains/contained future
};

/* Create a new thread pool with no more than n threads. */
struct thread_pool *thread_pool_new(int nthreads);

/*
 * Shutdown this thread pool in an orderly fashion.
 * Tasks that have been submitted but not executed may or
 * may not be executed.
 *
 * Deallocate the thread pool object before returning.
 */
void thread_pool_shutdown_and_destroy(struct thread_pool *pool);

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
    void *data);

/* Make sure that the thread pool has completed the execution
 * of the fork join task this future represents.
 *
 * Returns the value returned by this task.
 */
void *future_get(struct future *fut);

/* Deallocate this future.  Must be called after future_get() */
void future_free(struct future *fut);
