#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
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
    evthr_pool_slist_t threads;
};

struct evthr {
    int               cur_backlog;
    int               rdr;
    int               wdr;
    char              err;
    ev_t            * event;
    evbase_t        * evbase;
    evbase_t        * cb_base;
    pthread_mutex_t * lock;
    pthread_mutex_t * stat_lock;
    pthread_t       * thr;
    void            * args;

    TAILQ_ENTRY(evthr) next;
};

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

    if ((recvd = recv(sock, &cmd, sizeof(evthr_cmd_t), 0)) <= 0) {
        if (errno == EAGAIN) {
            goto end;
        } else {
            goto error;
        }
    }

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
        cmd.cb(thread->cb_base, cmd.args, thread->args);
        goto done;
    } else {
        goto done;
    }

stop:
    event_base_loopbreak(thread->evbase);
done:
    pthread_mutex_lock(thread->stat_lock);
    thread->cur_backlog -= 1;
    pthread_mutex_unlock(thread->stat_lock);
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

static void *
_evthr_loop(void * args) {
    evthr_t * thread;

    if (!(thread = (evthr_t *)args)) {
        return NULL;
    }

    thread->evbase  = event_base_new();
    thread->cb_base = event_base_new();
    thread->event   = event_new(thread->evbase, thread->rdr,
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
    evthr_cmd_t cmd = { 0 };

    pthread_mutex_lock(thread->stat_lock);

    if (thread->cur_backlog == -1) {
        return EVTHR_RES_FATAL;
    }

    thread->cur_backlog += 1;

    pthread_mutex_unlock(thread->stat_lock);

    cmd.magic = _EVTHR_MAGIC;
    cmd.cb    = cb;
    cmd.args  = arg;
    cmd.stop  = 0;

    if (send(thread->wdr, &cmd, sizeof(evthr_cmd_t), 0) <= 0) {
        return EVTHR_RES_RETRY;
    }

    return EVTHR_RES_OK;
}

evthr_res
evthr_stop(evthr_t * thread) {
    evthr_cmd_t cmd = { 0 };

    cmd.magic = _EVTHR_MAGIC;
    cmd.cb    = NULL;
    cmd.args  = NULL;
    cmd.stop  = 1;

    if (write(thread->wdr, &cmd, sizeof(evthr_cmd_t)) < 0) {
        return EVTHR_RES_RETRY;
    }

    return EVTHR_RES_OK;
}

evthr_t *
evthr_new(void * args) {
    evthr_t * thread;
    int       fds[2];

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == -1) {
        return NULL;
    }

    if (!(thread = calloc(sizeof(evthr_t), sizeof(char)))) {
        return NULL;
    }

    thread->stat_lock = malloc(sizeof(pthread_mutex_t));
    thread->lock      = malloc(sizeof(pthread_mutex_t));
    thread->thr       = malloc(sizeof(pthread_t));
    thread->args      = args;
    thread->rdr       = fds[0];
    thread->wdr       = fds[1];

    if (pthread_mutex_init(thread->lock, NULL)) {
        evthr_free(thread);
        return NULL;
    }

    if (pthread_mutex_init(thread->stat_lock, NULL)) {
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
        int       break_early = 0;

        pthread_mutex_lock(thr->stat_lock);

        if (min_thr) {
            pthread_mutex_lock(min_thr->stat_lock);
        }

        m_save = min_thr;
        t_save = thr;

        if (min_thr == NULL) {
            min_thr = thr;
        } else if (thr->cur_backlog == 0) {
            min_thr = thr;
        } else if (thr->cur_backlog < min_thr->cur_backlog) {
            min_thr = thr;
        }

        if (min_thr->cur_backlog == 0) {
            break_early = 1;
        }

        if (m_save) {
            pthread_mutex_unlock(m_save->stat_lock);
        }

        pthread_mutex_unlock(t_save->stat_lock);

        if (break_early == 1) {
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

    pool->nthreads = nthreads;
    TAILQ_INIT(&pool->threads);

    for (i = 0; i < nthreads; i++) {
        evthr_t * thread;

        if (!(thread = evthr_new(shared))) {
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

