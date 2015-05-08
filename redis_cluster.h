#ifndef POCO_REDIS_CLUSTER_H
#define POCO_REDIS_CLUSTER_H

#include <stdint.h>
#include "hiredis/hiredis.h"

uint16_t _crc16(const char *buf, int len);

/* redisContext link list */
typedef struct _redis_list_ctx_st{
    struct _redis_list_ctx_st *next;
    redisContext *ctx;
    int id;
}redis_list_ctx_st;
void _redis_list_ctx_clear(redis_list_ctx_st *list_ctx);
redis_list_ctx_st *_redis_list_ctx_init(int id, const char *ip, int port, struct timeval timeout);

typedef struct {
    redis_list_ctx_st *head;
    redis_list_ctx_st *tail;
    int list_count;
    char ip[64];
    int port;
    int id;
}redis_cluster_node_st;
redis_cluster_node_st *_redis_cluster_node_init(int id, const char *ip, int port);
void _redis_cluster_node_clear(redis_cluster_node_st *cluster_node);
void _redis_list_push_back(redis_cluster_node_st *cluster_node, redis_list_ctx_st *ctx);
redis_list_ctx_st *_redis_list_pop_front(redis_cluster_node_st *cluster_node);

#define REDIS_CLUSTER_NODE_COUNT 256
#define REDIS_CLUSTER_SLOTS 16384
typedef struct {
    int node_count;
    redis_cluster_node_st *redis_nodes[REDIS_CLUSTER_NODE_COUNT];
    redis_cluster_node_st *slots_handler[REDIS_CLUSTER_SLOTS];
    int state;
    int master_ctx_cnt;
    struct timeval timeout;
}redis_cluster_st;
int _redis_cluster_refreash(redis_cluster_st *cluster, const redisReply *reply);
redis_list_ctx_st *_redis_cluster_get_context(redis_cluster_st *cluster, const char *key);
void _redis_cluster_set_slot(redis_cluster_st *cluster, redis_cluster_node_st *cluster_node, int slot);
int _redis_cluster_find_connection(redis_cluster_st *cluster, const char *ip, int port);

/* Inner interface */
int _redis_command_ping(redisContext *ctx);
redisReply *_redis_command_cluster_slots(redisContext *ctx);

/* Client interface */
redis_cluster_st *redis_cluster_init(const char (*ips)[64], int *ports, int count, int timeout, int master_ctx_cnt);
void redis_cluster_free(redis_cluster_st *cluster);
redisReply *redis_cluster_execute(redis_cluster_st *cluster, const char *key, const char *fmt, ...);
redisReply *redis_cluster_arg_execute(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap);
int redis_cluster_append(redis_cluster_st *cluster, const char *key, const char *fmt, ...);
redisReply *redis_cluster_get_reply();

#endif // POCO_REDIS_CLUSTER_H
