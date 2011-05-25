#ifndef __EVTHR_H__
#define __EVTHR_H__

#include <pthread.h>
#include <sys/queue.h>
#include <event.h>
#include <event2/thread.h>

struct evthr_pool;
struct evthr;

typedef struct event_base evbase_t;
typedef struct event      ev_t;

typedef struct evthr_pool evthr_pool_t;
typedef struct evthr      evthr_t;
typedef enum evthr_res    evthr_res;

typedef void (*evthr_cb)(evbase_t * base, void * cmd_arg, void * shared);

enum evthr_res {
    EVTHR_RES_OK = 0,
    EVTHR_RES_BACKLOG,
    EVTHR_RES_RETRY,
    EVTHR_RES_NOCB,
    EVTHR_RES_FATAL
};

evthr_t      * evthr_new(void * arg);
int            evthr_start(evthr_t * evthr);
evthr_res      evthr_stop(evthr_t * evthr);
evthr_res      evthr_defer(evthr_t * evthr, evthr_cb cb, void * arg);
void           evthr_free(evthr_t * evthr);

evthr_pool_t * evthr_pool_new(int nthreads, void * shared);
int            evthr_pool_start(evthr_pool_t * pool);
evthr_res      evthr_pool_stop(evthr_pool_t * pool);
evthr_res      evthr_pool_defer(evthr_pool_t * pool, evthr_cb cb, void * arg);
void           evthr_pool_free(evthr_pool_t * pool);

#endif /* __EVTHR_H__ */

