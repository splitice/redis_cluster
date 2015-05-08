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

    char *cmd = argv[2];
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

    redis_cluster_st *cluster = redis_cluster_init((const char(*)[64])ips, ports, 6, 1000, 1);
    if (!cluster) {
        printf("Init cluster fail.\n");
        return -1;
    }

    redisReply *reply;
    while (1) {
        getchar();
        reply = redis_cluster_execute(cluster, cmd, cmd_args);
        if (!reply) {
            printf("Get reply fail.\n");
            continue;
        }

        printReply(reply, 0, 0);
        freeReplyObject(reply);
    }

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
