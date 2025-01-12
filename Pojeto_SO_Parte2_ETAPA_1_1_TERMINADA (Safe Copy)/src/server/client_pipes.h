#ifndef SERVER_PIPES_H
#define SERVER_PIPES_H

#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

/*typedef struct SubscriptionNode {
    char key[MAX_STRING_SIZE];
    struct SubscriptionNode *next;
} SubscriptionNode;
*/

//Talvez as strings devam ser char*???
typedef struct client_struct{
    char subscribed_keys[MAX_NUMBER_SUB][MAX_STRING_SIZE];
    int count_keys;
    int client_id_num;
    int req_pipe_fd;
    int resp_pipe_fd;
    int notif_pipe_fd;
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
} client_t;

void client_init(client_t* client, int id_num);

//void *client_thread_fn(void* client_args);

int client_handler(client_t *client);

int create_pipe(char const *filepath);

int delete_pipe(char const *filepath);

int handle_subscribe(char *keys, client_t *client);

int handle_disconnect(client_t *client);

int handle_unsubscribe(char *key, client_t *client);

#endif
