#include "client_pipes.h"
#include "operations.h"

void client_init(client_t* client, int id_num){
    
    client->client_id_num = id_num;
    client->count_keys = 0;
    client->notif_pipe_fd = 0;
    client->req_pipe_fd = 0;
    client->resp_pipe_fd = 0;
    memset(client->notif_pipe_path, '\0', sizeof(client->notif_pipe_path));
    memset(client->req_pipe_path, '\0', sizeof(client->req_pipe_path));
    memset(client->resp_pipe_path, '\0', sizeof(client->resp_pipe_path));
    
    for(int i = 0; i < MAX_NUMBER_SUB; i++){
        memset(client->subscribed_keys[i], '\0', sizeof(MAX_STRING_SIZE));
    }
}

int create_pipe(char const *filepath){
    /* remove pipe if it exists */
    if (unlink(filepath) != 0 && errno != ENOENT) {
        perror("[ERR]: unlink(%s) failed");
        return EXIT_FAILURE;
    }

    /* create pipe */
    if (mkfifo(filepath, 0640) != 0) {
        perror("[ERR]: mkfifo failed");
        return EXIT_FAILURE;
    }

    return 0;
}

int delete_pipe(char const *filepath){
    /* remove pipe if it exists */
  if (unlink(filepath) != 0 && errno != ENOENT) {
      perror("[ERR]: unlink(%s) failed");
      return EXIT_FAILURE;
  }

  return 0;
}

int handle_disconnect(client_t *client) {
    char result = 0;

    for (int i = 0; i < client->count_keys; i++) {
        memset(client->subscribed_keys[i], '\0', sizeof(client->subscribed_keys[i]));

        for (int j = 0; j < MAX_STRING_SIZE; j++) {
            if (client->subscribed_keys[i][j] != '\0') {
                result = 1; 
                break;
            }
        }

        if (result) {
            break; 
        }

        client->count_keys--;
    }

    if ((result == 0) && (client->count_keys == 0)) {
        printf("[INFO]: Disconnected client %d successfully.\n", client->client_id_num);
    } else {
        fprintf(stderr, "[ERR]: Failed to disconnect client %d.\n", client->client_id_num);
    }

    char disconnect_message[2] = {OP_CODE_DISCONNECT, result};

    if(write_all(client->resp_pipe_fd, disconnect_message, sizeof(disconnect_message)) == -1){
        fprintf(stderr, "[ERR]: Failed to send disconnect response message back to server.\n");
        return -1;
    }

    return result;
}


int handle_subscribe(char *keys, client_t *client){
    int key_exists;
    char response;

    /*
    1: abre o req for reading 
    2: verify keys 
    3: abre resp for writing
    4: devolve 0-chave nao existe OU devolve 1-chave existe (FIFO RESPOSTA)
    */

    key_exists = verify_keys(keys); 
    response = ((key_exists) ? 1 : 0);

    if (key_exists && client->count_keys < MAX_NUMBER_SUB){
        int is_subscribed = 0;
        for (int i = 0; i < client->count_keys; i++){
            if(strcmp(client->subscribed_keys[i], keys) == 0){
                is_subscribed = 1;
                break;
            }
        }
        if (!is_subscribed){
            strncpy(client->subscribed_keys[client->count_keys], keys, MAX_STRING_SIZE);
            client->count_keys++;
        }
    }

    char subscribe_message[2] = {OP_CODE_SUBSCRIBE, response};
    //subscribe_message[0] = (char) OP_CODE_SUBSCRIBE;
    //subscribe_message[1] = response;

    if(write_all(client->resp_pipe_fd, subscribe_message, sizeof(subscribe_message)) == -1){
        perror("[ERR]: Failed to send subscribe message");
        //close(client->resp_pipe_fd);
        return -1;
    }

    return 0;
}

int handle_unsubscribe(char *key, client_t *client){
    char response;

    /*
    1: verifica subscription
    2: se existia -> apagar e resposta 0
    3:se nao existia -> resposta 1
    4:
    */

    int key_place = -1;
    for (int i = 0; i < client->count_keys; i++){
        if(strcmp(client->subscribed_keys[i], key) == 0){
            key_place= i; //guardar a chave onde foi encontrada
            break;    
       }
    }

    if (key_place == -1){
        response = 1;
    }

    else{
        for (int j = key_place; j < client->count_keys; j++){
            strncpy(client->subscribed_keys[j], client->subscribed_keys[j + 1], sizeof(client->subscribed_keys[j]));
        }
        client->count_keys--;
        response = 0;
    }

    char unsubscribe_message[2] = {OP_CODE_UNSUBSCRIBE, response};
        
    if(write_all(client->resp_pipe_fd, unsubscribe_message, sizeof(unsubscribe_message)) == -1){
        perror("[ERR]: Failed to send subscribe message");
        //close(client->resp_pipe_fd);
        return -1;
    }

  return 0;
}
   


