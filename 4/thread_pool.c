#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>


enum task_state { TASK_NEW, TASK_QUEUED, TASK_RUNNING, TASK_DONE };


struct thread_task {
    thread_task_f function;
    void *arg;

    void *result;
    struct thread_pool *owner;
    struct thread_task *next;
    enum task_state state;
    pthread_mutex_t lock;
    pthread_cond_t cond;
};

struct thread_pool {
    pthread_t *threads;

    int max_threads;
    int threads_created;
    int threads_busy;

    struct thread_task *task_first;
    struct thread_task *task_last;
    int task_total;

    pthread_mutex_t sync;
    pthread_cond_t task_available;
    pthread_cond_t all_tasks_done;

    bool shutting_down;
};


static void *worker_loop(void *data) {
    struct thread_pool *pool = (struct thread_pool *)data;

    while (1) {
        pthread_mutex_lock(&pool->sync);

        while (!pool->shutting_down && pool->task_first == NULL) {
            pthread_cond_wait(&pool->task_available, &pool->sync);
        }

        if (pool->shutting_down) {
            pool->threads_created--;
            pthread_mutex_unlock(&pool->sync);
            break;
        }

        struct thread_task *task = pool->task_first;
        if (task) {
            pool->task_first = task->next;
            if (pool->task_first == NULL)
                pool->task_last = NULL;
            pool->task_total--;
            pool->threads_busy++;
        }

        pthread_mutex_unlock(&pool->sync);

        if (task) {
            task->state = TASK_RUNNING;
            void *res = task->function(task->arg);

            pthread_mutex_lock(&task->lock);
            task->result = res;
            task->state = TASK_DONE;
            pthread_cond_signal(&task->cond);
            pthread_mutex_unlock(&task->lock);

            pthread_mutex_lock(&pool->sync);
            pool->threads_busy--;
            if (pool->task_total == 0 && pool->threads_busy == 0) {
                pthread_cond_signal(&pool->all_tasks_done);
            }
            pthread_mutex_unlock(&pool->sync);
        }
    }

    return NULL;
}

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
    if (!pool || max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_pool *p = calloc(1, sizeof(*p));
    if (!p) return -1;

    p->threads = calloc(max_thread_count, sizeof(pthread_t));
    if (!p->threads) {
        free(p);
        return -1;
    }

    p->max_threads = max_thread_count;

    pthread_mutex_init(&p->sync, NULL);
    pthread_cond_init(&p->task_available, NULL);
    pthread_cond_init(&p->all_tasks_done, NULL);

    *pool = p;
    return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
    if (!pool) return 0;
    return pool->threads_created;
}

int thread_pool_delete(struct thread_pool *pool) {
    if (!pool) return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&pool->sync);
    if (pool->task_total > 0 || pool->threads_busy > 0) {
        pthread_mutex_unlock(&pool->sync);
        return TPOOL_ERR_HAS_TASKS;
    }

    pool->shutting_down = true;
    pthread_cond_broadcast(&pool->task_available);
    pthread_mutex_unlock(&pool->sync);

    for (int i = 0; i < pool->threads_created; ++i) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->sync);
    pthread_cond_destroy(&pool->task_available);
    pthread_cond_destroy(&pool->all_tasks_done);
    free(pool->threads);
    free(pool);

    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    if (!pool || !task || pool->shutting_down)
        return TPOOL_ERR_INVALID_ARGUMENT;

    if (pool->task_total >= TPOOL_MAX_TASKS)
        return TPOOL_ERR_TOO_MANY_TASKS;

    if (task->state != TASK_NEW && task->state != TASK_DONE)
        return TPOOL_ERR_TASK_IN_POOL;

    pthread_mutex_lock(&pool->sync);

    task->state = TASK_QUEUED;
    task->owner = pool;
    task->next = NULL;

    if (!pool->task_first) {
        pool->task_first = task;
        pool->task_last = task;
    } else {
        pool->task_last->next = task;
        pool->task_last = task;
    }

    pool->task_total++;

    if (pool->threads_created < pool->max_threads &&
        pool->threads_busy == pool->threads_created) {
        if (pthread_create(&pool->threads[pool->threads_created], NULL, worker_loop, pool) == 0) {
            pool->threads_created++;
        }
    }

    pthread_cond_signal(&pool->task_available);
    pthread_mutex_unlock(&pool->sync);
    return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
    if (!task || !function) return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_task *t = calloc(1, sizeof(*t));
    if (!t) return -1;

    t->function = function;
    t->arg = arg;
    t->state = TASK_NEW;

    pthread_mutex_init(&t->lock, NULL);
    pthread_cond_init(&t->cond, NULL);

    *task = t;
    return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
    return task && task->state == TASK_DONE;
}

bool thread_task_is_running(const struct thread_task *task) {
    return task && task->state == TASK_RUNNING;
}

int thread_task_join(struct thread_task *task, void **result) {
    if (!task || !result) return TPOOL_ERR_INVALID_ARGUMENT;
    if (task->state == TASK_NEW || !task->owner) return TPOOL_ERR_TASK_NOT_PUSHED;

    pthread_mutex_lock(&task->lock);
    while (task->state != TASK_DONE)
        pthread_cond_wait(&task->cond, &task->lock);
    *result = task->result;
    pthread_mutex_unlock(&task->lock);

    return 0;
}

#if NEED_TIMED_JOIN

int thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
    (void)task;
    (void)timeout;
    (void)result;
    return TPOOL_ERR_NOT_IMPLEMENTED;
}

#endif

int thread_task_delete(struct thread_task *task) {
    if (!task) return TPOOL_ERR_INVALID_ARGUMENT;
    if (task->state == TASK_QUEUED || task->state == TASK_RUNNING)
        return TPOOL_ERR_TASK_IN_POOL;

    pthread_mutex_destroy(&task->lock);
    pthread_cond_destroy(&task->cond);
    free(task);
    return 0;
}

#if NEED_DETACH

int thread_task_detach(struct thread_task *task) {
    (void)task;
    return TPOOL_ERR_NOT_IMPLEMENTED;
}

#endif
