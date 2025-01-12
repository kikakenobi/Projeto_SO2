//Não esquecer de testar todas as condições que podem dar origem a erros

#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

//pipe paths
const char *req_pipe_path_global;
const char *resp_pipe_path_global;
const char *notif_pipe_path_global;

int req_pipe_fd_global = 0;
int resp_pipe_fd_global = 0;
int notif_pipe_fd_global = 0;
int server_pipe_fd_global = 0;

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

/* VER SE ISTO É NECESSÁRIO  - PROBABLY NOT*/

/*void close_pipe(int pipe_fd) {
  if (pipe_fd != -1){
    close(pipe_fd);
    return 0;
  }
  return EXIT_FAILURE;
}*/

/* VER SE ISTO É NECESSÁRIO - PROBABLY NOT*/

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
  // create pipes and connect

  if(create_pipe(req_pipe_path)){
    return 1;
  }

  if(create_pipe(resp_pipe_path)){
    delete_pipe(req_pipe_path);
    return 1;
  }

  if(create_pipe(notif_pipe_path)){
    delete_pipe(req_pipe_path);
    delete_pipe(resp_pipe_path);
    return 1;
  }

  req_pipe_path_global = req_pipe_path;
  resp_pipe_path_global = resp_pipe_path;
  notif_pipe_path_global = notif_pipe_path;

  int server_pipe_fd = open(server_pipe_path, O_WRONLY);
  if (server_pipe_fd == -1) {
    perror("[ERR]: server_pipe open failed");
    //Fechar os pipes
    return EXIT_FAILURE;
  }

  server_pipe_fd_global = server_pipe_fd;

  char connect_message[1 + 3 * MAX_PIPE_PATH_LENGTH] = {0};

  //memset(connect_message, '\0', sizeof(connect_message) * sizeof(char));

  connect_message[0] = (char) OP_CODE_CONNECT;
  //Ponderar usar STRNCPY
  snprintf(connect_message + 1, MAX_PIPE_PATH_LENGTH, "%s", req_pipe_path);
  snprintf(connect_message + 1 + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH, "%s", resp_pipe_path);
  snprintf(connect_message + 1 + 2 * MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH, "%s", notif_pipe_path);

  if(write_all(server_pipe_fd, connect_message, sizeof(connect_message)) == -1){
    perror("[ERR]: Failed to send connect message");
    return EXIT_FAILURE;
  }

  *notif_pipe = open(notif_pipe_path, O_RDONLY);
  if (*notif_pipe == -1) {
    perror("[ERR]: notif_pipe open failed");
    //Fechar o que precisa de ser fechado
    return EXIT_FAILURE;
  }

  notif_pipe_fd_global = *notif_pipe;

  req_pipe_fd_global = open(req_pipe_path, O_WRONLY);
  if (req_pipe_fd_global == -1) {
    perror("[ERR]: req_pipe open failed");
    return EXIT_FAILURE;
  }

  resp_pipe_fd_global = open(resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd_global == -1) {
    perror("[ERR]: resp_pipe open failed");
    return EXIT_FAILURE;
  }

  char server_response[2]; // zero or 1 
  if(read(resp_pipe_fd_global, server_response, sizeof(server_response)) <= 0){
    perror("[ERR]: Failed to read connect response message");
    return EXIT_FAILURE;
  }

  if(server_response[1] != 0){
    printf("Server returned %d for operation: connect\n", server_response[1]);
    return EXIT_FAILURE;
  }

  printf("Server returned %d for operation: connect\n", server_response[1]);

  return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  char disconnect_message[1];
  
  disconnect_message[0]= (char) OP_CODE_DISCONNECT;

  if(write_all(req_pipe_fd_global, disconnect_message, sizeof(disconnect_message)) == -1){
    perror("[ERR]: Failed to send disconnect message");
    return EXIT_FAILURE;
  }

  char server_response[2];
  if(read(resp_pipe_fd_global, server_response, sizeof(server_response)) <= 0){
    perror("[ERR]: Failed to read disconnect response message");
    return EXIT_FAILURE;
  }

  if(server_response[1] != 0){
    printf("Server returned %d for operation: disconnect\n", server_response[1]);
    return EXIT_FAILURE;
  }

  printf("Server returned %d for operation: disconnect\n", server_response[1]);

  close(req_pipe_fd_global);
  close(resp_pipe_fd_global);
  close(notif_pipe_fd_global);
  close(server_pipe_fd_global);

  delete_pipe(req_pipe_path_global);
  delete_pipe(resp_pipe_path_global);
  delete_pipe(notif_pipe_path_global);

  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  char subscribe_message[2 + MAX_STRING_SIZE] = {0}; //1 op code + final char for key

  subscribe_message[0] = (char) OP_CODE_SUBSCRIBE;

  snprintf(subscribe_message + 1, MAX_STRING_SIZE, "%s", key);

  if(write_all(req_pipe_fd_global, subscribe_message, sizeof(subscribe_message)) == -1){
    perror("[ERR]: Failed to send subscribe message");
    return EXIT_FAILURE;
  }

  char server_response[2];
  if(read(resp_pipe_fd_global, server_response, sizeof(server_response)) <= 0){
    perror("[ERR]: Failed to read subscribe response message");
    return EXIT_FAILURE;
  }

  if(server_response[1] != 1){
    printf("Server returned %d for operation: subscribe\n", server_response[1]);
    return EXIT_FAILURE;
  }

  printf("Server returned %d for operation: subscribe\n", server_response[1]);

  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  char unsubscribe_message[2 + MAX_STRING_SIZE] = {0};

  unsubscribe_message[0] = (char) OP_CODE_UNSUBSCRIBE;

  snprintf(unsubscribe_message + 1, MAX_STRING_SIZE, "%s", key);

  if(write_all(req_pipe_fd_global, unsubscribe_message, sizeof(unsubscribe_message)) == -1){
    perror("[ERR]: Failed to send unsubscribe message");
    return EXIT_FAILURE;
  }

  char server_response[2];
  if(read(resp_pipe_fd_global, server_response, sizeof(server_response)) <= 0){
    perror("[ERR]: Failed to read unsubscribe response message");
    return EXIT_FAILURE;
  }

  if(server_response[1] != 0){
    printf("Server returned %d for operation: unsubscribe\n", server_response[1]);
    return EXIT_FAILURE;
  }

  printf("Server returned %d for operation: unsubscribe\n", server_response[1]);

  return 0;
}



