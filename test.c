#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "redis_cluster.h"

void printReply(redisReply *r, int depth, int idx);

int main(int argc, char *argv[])
{
    if (argc <= 2) {
        printf("Input redis command please.\n");
        return -1;
    }

    char *key = argv[1];
    char cmd_args[1024] = {0x00};
    int i;
    for (i = 1; i < argc; ++i) {
        strcat(cmd_args, argv[i]);
        strcat(cmd_args, " ");
    }
    cmd_args[strlen(cmd_args) - 1] = '\0';
    printf("%s\n", cmd_args);

    char ips[][64] = {
        "127.0.0.1",
        "127.0.0.1",
        "127.0.0.1",
        "127.0.0.1",
        "127.0.0.1",
        "127.0.0.1"
    };
    int ports[] = {
        6379,
        6380,
        6381,
        6382,
        6383,
        6384
    };

    redis_cluster_st *cluster = redis_cluster_init();
    if (!cluster) {
        printf("Init cluster fail.\n");
        return -1;
    }

    int rc;
    char c;

    rc = redis_cluster_connect(cluster, (const char(*)[64])ips, ports, 6, 1000);
    if (rc < 0) {
        printf("Connect to redis cluster fail.\n");
        return -1;
    }

    redisReply *reply;
    while (1) {
        c = getchar();
        if (c == 'q') break;
        reply = redis_cluster_execute(cluster, key, cmd_args);
        if (!reply) {
            printf("Execute fail.\n");
            continue;
        }

        printReply(reply, 0, 0);
        freeReplyObject(reply);

        /* Pipelining testing */

        rc = redis_cluster_append(cluster, key, cmd_args);
        if (rc < 0) {
            printf("Append command fail.\n");
            continue;
        }

        rc = redis_cluster_append(cluster, key, cmd_args);
        if (rc < 0) {
            printf("Append command fail.\n");
            continue;
        }

        reply = redis_cluster_get_reply(cluster);
        if (!reply) {
            printf("Get reply fail.\n");
            continue;
        }
        printReply(reply, 0, 0);
        freeReplyObject(reply);

        reply = redis_cluster_get_reply(cluster);
        if (!reply) {
            printf("Get reply fail.\n");
            continue;
        }
        printReply(reply, 0, 0);
        freeReplyObject(reply);
    }

    redis_cluster_free(cluster);
    return 0;
}

int idx_record[128] = {0};
void printReply(redisReply *r, int depth, int idx)
{
    int i;
    int tmp_idx;

    idx_record[depth] = idx;
    if (depth > 0) {
        for (i = 0; i < depth - 1 && idx > 0; ++i) {
            printf("   ");
            tmp_idx = idx_record[i];
            tmp_idx /= 10;
            while (tmp_idx) {
                printf(" ");
                tmp_idx /= 10;
            }
        }
        printf("%d) ", idx + 1);
    }

    switch (r->type) {
    case REDIS_REPLY_ARRAY:
        for (i = 0; i < r->elements; ++i) {
            printReply(r->element[i], depth + 1, i);
        }
        break;

    case REDIS_REPLY_ERROR:
        printf("(error) %s\n", r->str);
        break;

    case REDIS_REPLY_STRING:
    case REDIS_REPLY_STATUS:
        printf("\"%s\"\n", r->str);
        break;

    case REDIS_REPLY_NIL:
        printf("Nil\n");
        break;

    case REDIS_REPLY_INTEGER:
        printf("(integer) %lld\n", r->integer);
        break;

    default:
        break;
    }
}
