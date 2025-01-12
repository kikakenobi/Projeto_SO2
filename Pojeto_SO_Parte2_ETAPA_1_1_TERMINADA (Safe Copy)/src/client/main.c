// As threads gestoras devem ser criadas logo quando o servidor é inicializado

// A tarefa anfitriã é apenas a tarefa main no server (deve ficar à espera num ciclo de leitura no dispatch_threads após as threads dos .job serem criadas)

// O FIFO de registo deve ser aberto com permissões de read-write

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

void *notif_handler(void* thread_args){
  int notif_thread_fd = *((int *) thread_args); 
  char notif_buffer[2 + 2 * MAX_STRING_SIZE] = {0};
  int interrupt = 0;

  while(1){
    int read_result = read_all(notif_thread_fd, notif_buffer, sizeof(notif_buffer), &interrupt);

    if (read_result == -1) {
      if(interrupt){
        fprintf(stderr, "[INFO]: Read interrupted\n");
        interrupt = 0;
        continue;
      }
      fprintf(stderr, "[ERR]: Failed to read from Notification FIFO\n");
      break;
    } else if (read_result == 0) {
        fprintf(stderr, "[INFO]: Notification FIFO closed.\n");
        break;
    } else {
        char *key = notif_buffer;
        char *value = notif_buffer + 41;

        if (strcmp(value, "DELETED") == 0) {
            printf("(%s,DELETED)\n", key);
        } else {
            printf("(%s,%s)\n", key, value);
        }
    }
  }
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/req";
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/resp";
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  int *notif_pipe_fd = (int*) malloc(sizeof(int));

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  if(kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path, notif_pipe_fd)){
    perror("[ERR]: Failed to connect to server\n");
    return EXIT_FAILURE;
  }

  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, notif_handler, (void *)notif_pipe_fd) != 0) {
    fprintf(stderr, "Failed to create notification handler thread: %s\n", strerror(errno));
    pthread_detach(notif_thread);
    kvs_disconnect();
    return 1;
  }

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect() != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }
      // TODO: end notifications thread
      if(pthread_cancel(notif_thread) != 0){
        fprintf(stderr, "Failed to cancel notifications thread.\n");
      }
      if(pthread_join(notif_thread, NULL) != 0){
        fprintf(stderr, "Failed to join notifications thread.\n");
      }
      printf("Disconnected from server\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0])) {
        fprintf(stderr, "Command unsubscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
}
