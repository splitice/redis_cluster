#include "redis_cluster.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>

#define DEBUG
#ifdef DEBUG
#define _redis_cluster_log(fmt, arg...) printf(fmt, ##arg)
#else
#define _redis_cluster_log(fmt, arg...)
#endif


/* crc16 algorithm*/
static const uint16_t crc16_table[256] = {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t _crc16(const char *buf, int len) {
    int i;
    uint16_t crc = 0;
    for (i = 0; i < len; i++) {
        crc = (crc << 8) ^ crc16_table[((crc >> 8) ^ *buf++) & 0x00FF];
    }
    return crc;
}

void _redis_list_ctx_clear(redis_list_ctx_st *list_ctx)
{
    if (!list_ctx) {
        return;
    }

    if (list_ctx->ctx) {
        redisFree(list_ctx->ctx);
    }
    list_ctx->ctx = NULL;
    list_ctx->next = NULL;
}

redis_list_ctx_st *_redis_list_ctx_init(int id, const char *ip, int port, struct timeval timeout)
{
    redis_list_ctx_st *result = (redis_list_ctx_st *)malloc(sizeof(redis_list_ctx_st));
    if (!result) {
        _redis_cluster_log("Malloc list context fail.\n");
        return NULL;
    }

    result->id = id;
    result->next = NULL;
    result->ctx = redisConnectWithTimeout(ip, port, timeout);
    if (!result->ctx || result->ctx->err) {
        if (result->ctx) {
            redisFree(result->ctx);
        }
        free(result);
        _redis_cluster_log("List context new connection fail.[%s:%d](%d)\n", ip, port, (int)(timeout.tv_sec * 1000 + timeout.tv_usec));
        return NULL;
    }

    return result;
}

redis_cluster_node_st *_redis_cluster_node_init(int id, const char *ip, int port)
{
    redis_cluster_node_st *result = (redis_cluster_node_st *)malloc(sizeof(redis_cluster_node_st));
    if (!result) {
        return NULL;
    }

    result->head = NULL;
    result->tail = NULL;
    result->list_count = 0;
    strncpy(result->ip, ip, sizeof(result->ip));
    result->port = port;
    result->id = id;
    return result;
}

void _redis_cluster_node_clear(redis_cluster_node_st *cluster_node)
{
    redis_list_ctx_st *list_ctx;
    while ((list_ctx = _redis_list_pop_front(cluster_node))) {
        _redis_list_ctx_clear(list_ctx);
        free(list_ctx);
    }
}

void _redis_list_push_back(redis_cluster_node_st *cluster_node, redis_list_ctx_st *ctx)
{
    if (!cluster_node || !ctx) {
        return;
    }

    ctx->next = NULL;
    if (!cluster_node->tail) {
        cluster_node->tail = ctx;
    } else {
        cluster_node->tail->next = ctx;
        cluster_node->tail = ctx;
    }

    if (!cluster_node->head) {
        cluster_node->head = cluster_node->tail;
    }

    ++cluster_node->list_count;
}

redis_list_ctx_st *_redis_list_pop_front(redis_cluster_node_st *cluster_node)
{
    if (!cluster_node || !cluster_node->head || !cluster_node->tail) {
        return NULL;
    }

    redis_list_ctx_st *node = cluster_node->head;

    if (cluster_node->head == cluster_node->tail) {
        cluster_node->head = NULL;
        cluster_node->tail = NULL;
    } else {
        cluster_node->head = node->next;
    }

    --cluster_node->list_count;

    return node;
}

int _redis_cluster_refreash(redis_cluster_st *cluster, const redisReply *reply)
{
    if (!cluster || !reply) {
        return -1;
    }
    size_t i, j;
    int k;
    int rc;
    int health_count;
    int ctx_count;
    redis_list_ctx_st *list_ctx = NULL;
    int cluster_node_count = reply->elements;
    int cluster_idx = cluster_node_count;
    int old_idx;
    const char *ip;
    int port;

    for (i = 0; i < reply->elements; ++i) {
        if ( ! (reply->element[i]->elements >= 3 &&
                reply->element[i]->element[0]->type == REDIS_REPLY_INTEGER &&
                reply->element[i]->element[1]->type == REDIS_REPLY_INTEGER &&
                reply->element[i]->element[2]->type == REDIS_REPLY_ARRAY
                )
             ) {
            _redis_cluster_log("Invalid type.\n");
            return -1;
        }

        ip = reply->element[i]->element[2]->element[0]->str;
        port = reply->element[i]->element[2]->element[1]->integer;

        /* Master node */
        old_idx = _redis_cluster_find_connection(cluster, ip, port);
        if (old_idx < 0) {
            /* Move old master node */
            if (cluster->redis_nodes[i]) {
                if (cluster->node_count < REDIS_CLUSTER_NODE_COUNT) {
                    cluster->redis_nodes[cluster->node_count] = cluster->redis_nodes[i];
                } else {
                    _redis_cluster_node_clear(cluster->redis_nodes[i]);
                }
                cluster->redis_nodes[i] = NULL;
            }

            /* Create new master node */
            cluster->redis_nodes[i] = _redis_cluster_node_init(i, ip, port);
            if (!cluster->redis_nodes[i]) {
                _redis_cluster_log("Init new master node fail.\n");
                return -1;
            }
        } else {
            redis_cluster_node_st *swap_tmp = cluster->redis_nodes[i];
            cluster->redis_nodes[i] = cluster->redis_nodes[old_idx];
            cluster->redis_nodes[old_idx] = swap_tmp;
        }

        /* Check old connection */
        health_count = 0;
        ctx_count = cluster->redis_nodes[i]->list_count;
        for (k = 0; k < ctx_count; ++k) {
            list_ctx = _redis_list_pop_front(cluster->redis_nodes[i]);
            if (!list_ctx) {
                break;
            }

            rc = _redis_command_ping(list_ctx->ctx);
            if (rc < 0) {
                _redis_list_ctx_clear(list_ctx);
                free(list_ctx);
                list_ctx = NULL;
            }

            if (health_count < cluster->master_ctx_cnt) {
                _redis_list_push_back(cluster->redis_nodes[i], list_ctx);
                ++health_count;
            }
        }

        /* Make connection */
        if (cluster->redis_nodes[i]->list_count < cluster->master_ctx_cnt) {
            for (k = 0; k < cluster->master_ctx_cnt - cluster->redis_nodes[i]->list_count; ++k) {
                list_ctx = _redis_list_ctx_init(i, ip, port, cluster->timeout);
                if (!list_ctx) {
                    _redis_cluster_log("Make new connection fail.\n");
                    return -1;
                }

                _redis_list_push_back(cluster->redis_nodes[i], list_ctx);
            }
        }

        /* Slots handler */
        for (k = (int)reply->element[i]->element[0]->integer; k <= (int)reply->element[i]->element[1]->integer; ++k) {
            cluster->slots_handler[k] = cluster->redis_nodes[i];
        }

        _redis_cluster_log("Master:[%d] (%d - %d)[%s:%d]\n", (int)i, (int)reply->element[i]->element[0]->integer, (int)reply->element[i]->element[1]->integer, ip, port);

        /* Slave node */
        for (j = 3; j < reply->element[i]->elements; ++j) {
            if (reply->element[i]->element[j]->type != REDIS_REPLY_ARRAY) {
                continue;
            }

            ip = reply->element[i]->element[j]->element[0]->str;
            port = reply->element[i]->element[j]->element[1]->integer;

            old_idx = _redis_cluster_find_connection(cluster, ip, port);
            if (old_idx < 0) {
                /* Move old slave node */
                if (cluster->redis_nodes[cluster_idx]) {
                    if (cluster->node_count < REDIS_CLUSTER_NODE_COUNT) {
                        cluster->redis_nodes[cluster->node_count] = cluster->redis_nodes[cluster_idx];
                    } else {
                        _redis_cluster_node_clear(cluster->redis_nodes[cluster_idx]);
                    }
                    cluster->redis_nodes[cluster_idx] = NULL;
                }

                /* Create new slave node */
                cluster->redis_nodes[cluster_idx] = _redis_cluster_node_init(cluster_idx, ip, port);
                if (!cluster->redis_nodes[cluster_idx]) {
                    _redis_cluster_log("Init new slave node fail.\n");
                    return -1;
                }
            } else {
                redis_cluster_node_st *swap_tmp = cluster->redis_nodes[cluster_idx];
                cluster->redis_nodes[cluster_idx] = cluster->redis_nodes[old_idx];
                cluster->redis_nodes[old_idx] = swap_tmp;
            }

            /* Only create one slave connection
             * Check old connection */
            health_count = 0;
            ctx_count = cluster->redis_nodes[i]->list_count;
            for (k = 0; k < ctx_count; ++k) {
                list_ctx = _redis_list_pop_front(cluster->redis_nodes[i]);
                if (!list_ctx) {
                    break;
                }

                rc = _redis_command_ping(list_ctx->ctx);
                if (rc < 0) {
                    _redis_list_ctx_clear(list_ctx);
                    free(list_ctx);
                    list_ctx = NULL;
                }

                if (health_count < 1) {
                    _redis_list_push_back(cluster->redis_nodes[cluster_idx], list_ctx);
                    ++health_count;
                }
            }

            /* Make connection */
            if (cluster->redis_nodes[cluster_idx]->list_count < 1) {
                list_ctx = _redis_list_ctx_init(cluster_idx, ip, port, cluster->timeout);
                if (!list_ctx) {
                    _redis_cluster_log("Make slave new connection fail.\n");
                    return -1;
                }

                _redis_list_push_back(cluster->redis_nodes[cluster_idx], list_ctx);
            }

            _redis_cluster_log("Slave:[%d] [%s:%d]\n", cluster_idx, ip, port);

            ++cluster_idx;
        }
    }

    for (i = cluster_idx; i < cluster->node_count; ++i) {
        _redis_cluster_node_clear(cluster->redis_nodes[i]);
        cluster->redis_nodes[i] = NULL;
    }
    cluster->node_count = cluster_idx;
    return 0;
}

redis_list_ctx_st *_redis_cluster_get_context(redis_cluster_st *cluster, const char *key)
{
    assert(cluster);
    assert(key);
    int idx = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;
    return _redis_list_pop_front(cluster->slots_handler[idx]);
}

void _redis_cluster_set_slot(redis_cluster_st *cluster, redis_cluster_node_st *cluster_node, int slot)
{
    assert(cluster);
    assert(slot >= 0);
    cluster->slots_handler[slot] = cluster_node;
}

int _redis_cluster_find_connection(redis_cluster_st *cluster, const char *ip, int port)
{
    assert(cluster);
    assert(ip);
    assert(port >= 0);
    int i;
    for (i = 0; i < cluster->node_count; ++i) {
        if (!cluster->redis_nodes[i]) {
            continue;
        }
        if (port == cluster->redis_nodes[i]->port && 0 == strcmp(cluster->redis_nodes[i]->ip, ip)) {
            return i;
        }
    }

    return -1;
}

int _redis_command_ping(redisContext *ctx)
{
    if (!ctx) {
        return -1;
    }

    int ret = -1;
    redisReply *reply = redisCommand(ctx, "PING");
    if (!reply) {
        return -1;
    }
    if (REDIS_REPLY_STRING == reply->type && 0 == strcmp(reply->str, "PONG")) {
        ret = 0;
    }
    freeReplyObject(reply);
    return ret;
}

redisReply *_redis_command_cluster_slots(redisContext *ctx)
{
    if (!ctx) {
        return NULL;
    }

    redisReply *reply = redisCommand(ctx, "CLUSTER SLOTS");
    if (!reply) {
        return NULL;
    }

    return reply;
}

redis_cluster_st *redis_cluster_init(const char (*ips)[64], int *ports, int count, int timeout, int master_ctx_cnt)
{
    if (!ips || !ports || count < 0 || timeout <= 0) {
        return NULL;
    }
    redisContext *ctx = NULL;
    redisReply *r = NULL;
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = timeout % 1000;

    int i;
    for (i = 0; i < count; ++i) {
        if (!ips[i] || ports[i] <= 0) {
            continue;
        }

        ctx = redisConnectWithTimeout(ips[i], ports[i], tv);
        if (!ctx || ctx->err) {
            if (ctx) {
                redisFree(ctx);
                ctx = NULL;
            }
            _redis_cluster_log("Connect to %s:%d fail!\n", ips[i], ports[i]);
            continue;
        }

        r = _redis_command_cluster_slots(ctx);
        if (!r || REDIS_REPLY_ARRAY != r->type) {
            redisFree(ctx);
            if (r) {
                freeReplyObject(r);
                r = NULL;
            }
            _redis_cluster_log("Get reply fail.\n");
            continue;
        }
        break;
    }

    if (!r) {
        if (ctx) {
            redisFree(ctx);
            ctx = NULL;
        }
        _redis_cluster_log("Init fail.\n");
        return NULL;
    }

    redis_cluster_st *cluster = (redis_cluster_st *)malloc(sizeof(redis_cluster_st));
    if (!cluster) {
        goto ON_INIT_ERROR;
    }
    memset(cluster, 0x00, sizeof(redis_cluster_st));

    cluster->master_ctx_cnt = master_ctx_cnt;
    cluster->timeout = tv;

    int rc = _redis_cluster_refreash(cluster, r);
    if (rc < 0) {
        _redis_cluster_log("Refresh fail.\n");
        goto ON_INIT_ERROR;
    }

    freeReplyObject(r);
    r = NULL;
    redisFree(ctx);
    ctx = NULL;
    return cluster;

ON_INIT_ERROR:
    if (cluster) {
        free(cluster);
        cluster = NULL;
    }
    if (r) {
        freeReplyObject(r);
        r = NULL;
    }
    if (ctx) {
        redisFree(ctx);
        ctx = NULL;
    }
    return NULL;
}

void redis_cluster_free(redis_cluster_st *cluster)
{
    if (!cluster) {
        return;
    }
    int i;

    for (i = 0; i < cluster->node_count; ++i) {
        _redis_cluster_node_clear(cluster->redis_nodes[i]);
        free(cluster->redis_nodes[i]);
        cluster->redis_nodes[i] = NULL;
    }
    cluster->node_count = 0;

    free(cluster);
}

redisReply *redis_cluster_execute(redis_cluster_st *cluster, const char *key, const char *fmt, ...)
{
    int slot = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;

    _redis_cluster_log("Key[%s] Slot[%d]\n", key, slot);
    va_list ap;
    va_start(ap, fmt);
    redisReply *r = redis_cluster_arg_execute(cluster, slot, fmt, ap);
    va_end(ap);

    return r;
}

redisReply *redis_cluster_arg_execute(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap)
{
    if (!cluster || !slot || !fmt) {
        return NULL;
    }

    int rc;
    char *p, *s;
    int is_ask;
    int redirect_slot;

    redisReply *reply;
    int handler_idx;

    handler_idx = cluster->slots_handler[slot]->id;
    redis_list_ctx_st *list_ctx = _redis_list_pop_front(cluster->redis_nodes[handler_idx]);
    if (!list_ctx) {
        list_ctx = _redis_list_ctx_init(handler_idx, cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port, cluster->timeout);
        if (!list_ctx) {
            _redis_cluster_log("Refresh cluster.\n");
            // Refresh cluster while reconnect fail.
            int i;
            for (i = 0; i < cluster->node_count; ++i) {
                if (i == handler_idx) {
                    continue;
                }
                _redis_cluster_log("Trying to other node connection.\n");
                list_ctx = _redis_list_pop_front(cluster->redis_nodes[i]);
                if (!list_ctx) {
                    list_ctx = _redis_list_ctx_init(i, cluster->redis_nodes[i]->ip, cluster->redis_nodes[i]->port, cluster->timeout);
                    if (!list_ctx) {
                        _redis_cluster_log("Refresh init context fail.[%s:%d]\n", cluster->redis_nodes[i]->ip, cluster->redis_nodes[i]->port);
                        continue;
                    }
                }

                reply = _redis_command_cluster_slots(list_ctx->ctx);
                if (!reply || REDIS_REPLY_ARRAY != reply->type) {
                    if (reply) {
                        freeReplyObject(reply);
                    }
                    _redis_list_ctx_clear(list_ctx);
                    free(list_ctx);
                    _redis_cluster_log("Refresh get reply fail.\n");
                    return NULL;
                }

                rc = _redis_cluster_refreash(cluster, reply);
                if (rc < 0) {
                    _redis_list_ctx_clear(list_ctx);
                    free(list_ctx);
                    _redis_cluster_log("Refresh fail.\n");
                    return NULL;
                }

                freeReplyObject(reply);
                _redis_list_push_back(cluster->redis_nodes[i], list_ctx);

                handler_idx = _redis_cluster_find_connection(cluster, cluster->slots_handler[slot]->ip, cluster->slots_handler[slot]->port);
                if (handler_idx < 0) {
                    _redis_list_ctx_clear(list_ctx);
                    free(list_ctx);
                    _redis_cluster_log("Find slot handler connection fail.\n");
                    return NULL;
                }

                list_ctx = _redis_list_pop_front(cluster->redis_nodes[handler_idx]);
                break;
            }
            if (!list_ctx) {
                _redis_cluster_log("Still connect fail.\n");
                return NULL;
            }
        }
    }

    _redis_cluster_log("Slot[%d] handler[%s:%d]\n", slot, cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port);
    reply = (redisReply *)(redisvCommand(list_ctx->ctx, fmt, ap));
    if (!reply) {
        _redis_list_ctx_clear(list_ctx);
        free(list_ctx);
        return NULL;
    }

    /* Cluster redirection */
    is_ask = 0;
    while (REDIS_REPLY_ERROR == reply->type && (0 == strncmp(reply->str,"MOVED",5) || 0 == strncmp(reply->str,"ASK",3))) {
        if (0 == strncmp(reply->str,"ASK",3)) {
            is_ask = 1;
        }

        p = reply->str;
        /* Comments show the position of the pointer as:
         *
         * [S] for pointer 's'
         * [P] for pointer 'p'
         */
        s = strchr(p, ' ');      /* MOVED[S]3999 127.0.0.1:6381 */
        p = strchr(s + 1, ' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
        *p = '\0';
        redirect_slot = atoi(s + 1);
        assert(redirect_slot == slot);
        s = strchr(p + 1, ':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
        *s = '\0';
        handler_idx = _redis_cluster_find_connection(cluster, p + 1, atoi(s + 1));
        if (handler_idx < 0) {
            /* Refresh cluster nodes */
            freeReplyObject(reply);
            reply = _redis_command_cluster_slots(list_ctx->ctx);
            if (!reply || REDIS_REPLY_ARRAY != reply->type) {
                if (reply) {
                    freeReplyObject(reply);
                }
                _redis_list_ctx_clear(list_ctx);
                free(list_ctx);
                return NULL;
            }

            rc = _redis_cluster_refreash(cluster, reply);
            if (rc < 0) {
                _redis_list_ctx_clear(list_ctx);
                free(list_ctx);
                return NULL;
            }

            handler_idx = _redis_cluster_find_connection(cluster, p + 1, atoi(s + 1));
            if (handler_idx < 0) {
                _redis_list_ctx_clear(list_ctx);
                free(list_ctx);
                return NULL;
            }

            _redis_list_push_back(cluster->redis_nodes[list_ctx->id], list_ctx);
        } else {
            if (!is_ask) {
                /* Save redirection */
                _redis_cluster_set_slot(cluster, cluster->redis_nodes[handler_idx], handler_idx);
            }
        }

        list_ctx = _redis_list_pop_front(cluster->redis_nodes[handler_idx]);
        if (!list_ctx) {
            list_ctx = _redis_list_ctx_init(handler_idx, cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port, cluster->timeout);
            if (!list_ctx) {
                return NULL;
            }
        }

        freeReplyObject(reply);
        reply = (redisReply *)(redisvCommand(list_ctx->ctx, fmt, ap));
        if (!reply) {
            _redis_list_ctx_clear(list_ctx);
            free(list_ctx);
            return NULL;
        }
    }

    _redis_list_push_back(cluster->redis_nodes[handler_idx], list_ctx);
    return reply;
}

int redis_cluster_append(redis_cluster_st *cluster, const char *key, const char *fmt, ...)
{
    return -1;
}

redisReply *redis_cluster_get_reply()
{
    return NULL;
}
