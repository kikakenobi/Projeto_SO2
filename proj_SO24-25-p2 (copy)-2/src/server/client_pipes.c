#include <client_pipes.h>
#include <operations.h>

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

void remove_subs(int client_id){
  for (int i = 0; i < MAX_SESSION_COUNT; i++){
    if(strcmp(client_subs[i].client_id_num, client_id) == 0) {
      printf("removing subs for client %s", client_id);

      memset(client_subs[i].subscribed_keys, 0,sizeoff(client_subs[i].subscribed_keys));
      client_subs[i].count_keys = 0;
      break;
    }
  }
}



int handle_disconnect(int req_pipe_fd, int client_id){
  char op_code;

  if(read(req_pipe_fd, &op_code, sizeoff(op_code)) == -1){
    return EXIT_FAILURE;
  }

  if (op_code != OP_CODE_DISCONNECT) {
    fprintf(stderr, "[ERR]: Invalid OP_CODE received for disconnect\n");
    return EXIT_FAILURE;
    }
  remove_subs(client_id);
  return 0;

}


int handle_subscribe(char *keys, int resp_pipe_path, client_t *client){
    char op_code;
    int key_exists;
    char response;
    char subscribe_message[2 + MAX_STRING_SIZE]= {0};

    /*
    1: abre o req for reading 
    2: verify keys 
    3: abre resp for writing
    4: devolve 0-chave nao existe OU devolve 1-chave existe (FIFO RESPOSTA)
    */

    key_exists = verify_keys(keys); 
    response = (key_exists ? 1 : 0);

    if (key_exists && client->count_keys != MAX_NUMBER_SUB){
        int is_subscribed = 0;
        for (int i=0; client->count_keys; i++){
            if(strcmp(client->count_keys, keys) == 0){
                is_subscribed = 1;
                break;
            }
        }
        if (!is_subscribed){
            strncpy(client->subscribed_keys[client->count_keys], keys, MAX_STRING_SIZE);
            client->count_keys++;
        }
    }

    subscribe_message[0] = (char) OP_CODE_SUBSCRIBE;
    subscribe_message[1] = response;

    int resp_pipe_fd = open(resp_pipe_path, O_WRONLY){
        if (resp_pipe_path == -1){
            perror( "ERROR: Failed to open");
            return -1;
        }
    }
    if(write_all(resp_pipe_fd, subscribe_message, sizeof(subscribe_message)) == -1){
        perror("[ERR]: Failed to send subscribe message");
        close(resp_pipe_fd);
        return -1;
  }
//close(resp_pipe_fd)
    return 0;
}

int handle_unsubscribe(char *key, int resp_pipe_path, client_t *client){
    char op_code;
    int key_exists;
    char response;
    char subscribed_message[ 2 + MAX_STRING_SIZE ] = {0};

    /*
    1: verifica subscription
    2: se existia -> apagar e resposta 0
    3:se nao existia -> resposta 1
    4:
    */
    int key_place = -1;
    for (int i= 0; i < client->count_keys; i++){
        if(strcmp(client->subscribed_keys[i],key) == 0){
            key_place= i; //guardar a chave onde foi encontrada
            break;    

       }
    }
    char unsubscribe_message[2] = {0};
    unsubscribe_message[0] = (char)OP_CODE_UNSUBSCRIBE;

   if (key_place == -1){
    unsubscribe_message[1]=1;
   }
   else{
    for (int j= key_place; j < client->count_keys -1; j++){
        strcpy(client->subscribed_keys[j], client->subscribed_keys[j + 1]);
        }
        client->count_keys--;
        unsubscribe_message[1] = 0;
    }
    int resp_pipe_fd = open(resp_pipe_path, O_WRONLY){
        if (resp_pipe_path == -1){
            perror( "ERROR: Failed to open");
            return -1;
        }
    }
    if(write_all(resp_pipe_fd, unsubscribe_message, sizeof(subscribe_message)) == -1){
        perror("[ERR]: Failed to send subscribe message");
        close(resp_pipe_fd);
        return -1;
  }
  return 0;
    }
   


