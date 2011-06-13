#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <sys/queue.h>
#include <pthread.h>

#include <event.h>
#include <event2/thread.h>

#include "evthr.h"

#define _EVTHR_MAGIC 0x4d52

typedef struct evthr_cmd        evthr_cmd_t;
typedef struct evthr_pool_slist evthr_pool_slist_t;

struct evthr_cmd {
    uint16_t magic;
    uint8_t  stop;
    void   * args;
    evthr_cb cb;
};

TAILQ_HEAD(evthr_pool_slist, evthr);

struct evthr_pool {
    int                nthreads;
    int                nprocs;
    evthr_pool_slist_t threads;
};

struct evthr {
    int               cur_backlog;
    int               proc_to_use;
    int               rdr;
    int               wdr;
    char              err;
    ev_t            * event;
    evbase_t        * evbase;
    pthread_mutex_t * lock;
    pthread_mutex_t * stat_lock;
    pthread_mutex_t * rlock;
    pthread_t       * thr;
    void            * args;

    TAILQ_ENTRY(evthr) next;
};

void
evthr_inc_backlog(evthr_t * evthr) {
    __sync_fetch_and_add(&evthr->cur_backlog, 1);
}

void
evthr_dec_backlog(evthr_t * evthr) {
    __sync_fetch_and_sub(&evthr->cur_backlog, 1);
}

int
evthr_get_backlog(evthr_t * evthr) {
    return __sync_add_and_fetch(&evthr->cur_backlog, 0);
}

static void
_evthr_read_cmd(int sock, short which, void * args) {
    evthr_t   * thread;
    evthr_cmd_t cmd;
    int         avail = 0;
    ssize_t     recvd;

    if (!(thread = (evthr_t *)args)) {
        return;
    }

    if (pthread_mutex_trylock(thread->lock) != 0) {
        return;
    }

    if (ioctl(sock, FIONREAD, &avail) < 0) {
        goto error;
    }

    if (avail <= 0) {
        goto end;
    }

    if (avail < (int)sizeof(evthr_cmd_t)) {
        goto end;
    }

    pthread_mutex_lock(thread->rlock);

    if ((recvd = recv(sock, &cmd, sizeof(evthr_cmd_t), 0)) <= 0) {
        pthread_mutex_unlock(thread->rlock);
        if (errno == EAGAIN) {
            goto end;
        } else {
            goto error;
        }
    }

    pthread_mutex_unlock(thread->rlock);

    if (recvd != sizeof(evthr_cmd_t)) {
        goto error;
    }

    if (cmd.magic != _EVTHR_MAGIC) {
        goto error;
    }

    if (cmd.stop == 1) {
        goto stop;
    }

    if (cmd.cb != NULL) {
        cmd.cb(thread, cmd.args, thread->args);
        goto done;
    } else {
        goto done;
    }

stop:
    event_base_loopbreak(thread->evbase);
done:
    evthr_dec_backlog(thread);
end:
    pthread_mutex_unlock(thread->lock);
    return;
error:
    pthread_mutex_lock(thread->stat_lock);
    thread->cur_backlog = -1;
    thread->err         = 1;
    pthread_mutex_unlock(thread->stat_lock);
    pthread_mutex_unlock(thread->lock);
    event_base_loopbreak(thread->evbase);
    return;
} /* _evthr_read_cmd */

static int
_evthr_get_num_procs(void) {
    return sysconf(_SC_NPROCESSORS_ONLN);
}

static void *
_evthr_loop(void * args) {
    evthr_t * thread;
    cpu_set_t set;
    pid_t     pid;

    if (!(thread = (evthr_t *)args)) {
        return NULL;
    }

    if (thread == NULL || thread->thr == NULL) {
        pthread_exit(NULL);
    }

#if 0
    CPU_ZERO(&set);
    CPU_SET(thread->proc_to_use, &set);

    pid = syscall(__NR_gettid);
    sched_setaffinity(pid, sizeof(cpu_set_t), &set);

    printf("Running on proc %d\n", thread->proc_to_use);
#endif

    thread->evbase = event_base_new();
    thread->event  = event_new(thread->evbase, thread->rdr,
        EV_READ | EV_PERSIST, _evthr_read_cmd, args);

    event_add(thread->event, NULL);
    event_base_loop(thread->evbase, 0);

    if (thread->err == 1) {
        fprintf(stderr, "FATAL ERROR!\n");
    }

    evthr_free(thread);
    pthread_exit(NULL);
}

evthr_res
evthr_defer(evthr_t * thread, evthr_cb cb, void * arg) {
    int         cur_backlog;
    evthr_cmd_t cmd = { 0 };

    cur_backlog = evthr_get_backlog(thread);

    if (cur_backlog == -1) {
        return EVTHR_RES_FATAL;
    }

    evthr_inc_backlog(thread);

    cmd.magic = _EVTHR_MAGIC;
    cmd.cb    = cb;
    cmd.args  = arg;
    cmd.stop  = 0;

    pthread_mutex_lock(thread->rlock);

    if (send(thread->wdr, &cmd, sizeof(evthr_cmd_t), 0) <= 0) {
        pthread_mutex_unlock(thread->rlock);
        return EVTHR_RES_RETRY;
    }

    pthread_mutex_unlock(thread->rlock);

    return EVTHR_RES_OK;
}

evthr_res
evthr_stop(evthr_t * thread) {
    evthr_cmd_t cmd = { 0 };

    cmd.magic = _EVTHR_MAGIC;
    cmd.cb    = NULL;
    cmd.args  = NULL;
    cmd.stop  = 1;

    pthread_mutex_lock(thread->rlock);

    if (write(thread->wdr, &cmd, sizeof(evthr_cmd_t)) < 0) {
        pthread_mutex_unlock(thread->rlock);
        return EVTHR_RES_RETRY;
    }

    pthread_mutex_unlock(thread->rlock);

    return EVTHR_RES_OK;
}

evbase_t *
evthr_get_base(evthr_t * thr) {
    return thr->evbase;
}

evthr_t *
evthr_new(void * args, int proc_to_use) {
    evthr_t * thread;
    int       fds[2];

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == -1) {
        return NULL;
    }

    if (!(thread = calloc(sizeof(evthr_t), sizeof(char)))) {
        return NULL;
    }

    thread->stat_lock   = malloc(sizeof(pthread_mutex_t));
    thread->rlock       = malloc(sizeof(pthread_mutex_t));
    thread->lock        = malloc(sizeof(pthread_mutex_t));
    thread->thr         = malloc(sizeof(pthread_t));
    thread->args        = args;
    thread->rdr         = fds[0];
    thread->wdr         = fds[1];
    thread->proc_to_use = proc_to_use;

    if (pthread_mutex_init(thread->lock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_mutex_init(thread->stat_lock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_mutex_init(thread->rlock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    fcntl(thread->rdr, F_SETFL, O_NONBLOCK);
    fcntl(thread->wdr, F_SETFL, O_NONBLOCK);

    return thread;
} /* evthr_new */

int
evthr_start(evthr_t * thread) {
    if (thread == NULL || thread->thr == NULL) {
        return -1;
    }

    if (pthread_create(thread->thr, NULL, _evthr_loop, (void *)thread)) {
        return -1;
    }

    return pthread_detach(*thread->thr);
}

void
evthr_free(evthr_t * thread) {
    if (thread == NULL) {
        return;
    }

    if (thread->rdr > 0) {
        close(thread->rdr);
    }

    if (thread->wdr > 0) {
        close(thread->wdr);
    }

    if (thread->lock) {
        pthread_mutex_destroy(thread->lock);
        free(thread->lock);
    }

    if (thread->stat_lock) {
        pthread_mutex_destroy(thread->stat_lock);
    }

    if (thread->rlock) {
        pthread_mutex_destroy(thread->rlock);
    }

    if (thread->thr) {
        free(thread->thr);
    }

    if (thread->event) {
        event_free(thread->event);
    }

    if (thread->evbase) {
        event_base_free(thread->evbase);
    }

    free(thread);
}

void
evthr_pool_free(evthr_pool_t * pool) {
    evthr_t * thread;
    evthr_t * save;

    if (pool == NULL) {
        return;
    }

    for (thread = TAILQ_FIRST(&pool->threads); thread != NULL; thread = save) {
        save = TAILQ_NEXT(thread, next);

        TAILQ_REMOVE(&pool->threads, thread, next);

        evthr_free(thread);
    }

    free(pool);
}

evthr_res
evthr_pool_stop(evthr_pool_t * pool) {
    evthr_t * thr;

    if (pool == NULL) {
        return EVTHR_RES_FATAL;
    }

    TAILQ_FOREACH(thr, &pool->threads, next) {
        evthr_stop(thr);
    }

    memset(&pool->threads, 0, sizeof(pool->threads));

    return EVTHR_RES_OK;
}

evthr_res
evthr_pool_defer(evthr_pool_t * pool, evthr_cb cb, void * arg) {
    evthr_t * min_thr = NULL;
    evthr_t * thr     = NULL;

    if (pool == NULL) {
        return EVTHR_RES_FATAL;
    }

    if (cb == NULL) {
        return EVTHR_RES_NOCB;
    }

    /* find the thread with the smallest backlog */
    TAILQ_FOREACH(thr, &pool->threads, next) {
        evthr_t * m_save;
        evthr_t * t_save;
        int       thr_backlog = 0;
        int       min_backlog = 0;

        thr_backlog = evthr_get_backlog(thr);

        if (min_thr) {
            min_backlog = evthr_get_backlog(min_thr);
        }

        m_save = min_thr;
        t_save = thr;

        if (min_thr == NULL) {
            min_thr = thr;
        } else if (thr_backlog == 0) {
            min_thr = thr;
        } else if (thr_backlog < min_backlog) {
            min_thr = thr;
        }

        if (evthr_get_backlog(min_thr) == 0) {
            break;
        }
    }

    return evthr_defer(min_thr, cb, arg);
} /* evthr_pool_defer */

evthr_pool_t *
evthr_pool_new(int nthreads, void * shared) {
    evthr_pool_t * pool;
    int            i;

    if (nthreads == 0) {
        return NULL;
    }

    if (!(pool = calloc(sizeof(evthr_pool_t), sizeof(char)))) {
        return NULL;
    }

    pool->nprocs   = _evthr_get_num_procs();
    pool->nthreads = nthreads;
    TAILQ_INIT(&pool->threads);

    for (i = 0; i < nthreads; i++) {
        evthr_t * thread;
        int       proc = i % pool->nprocs;

        if (!(thread = evthr_new(shared, proc))) {
            evthr_pool_free(pool);
            return NULL;
        }

        TAILQ_INSERT_TAIL(&pool->threads, thread, next);
    }

    return pool;
}

int
evthr_pool_start(evthr_pool_t * pool) {
    evthr_t * evthr = NULL;

    if (pool == NULL) {
        return -1;
    }

    TAILQ_FOREACH(evthr, &pool->threads, next) {
        if (evthr_start(evthr) < 0) {
            return -1;
        }

        usleep(300);
    }

    return 0;
}

