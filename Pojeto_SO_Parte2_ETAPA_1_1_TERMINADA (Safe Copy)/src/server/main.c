#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "client_pipes.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

client_t *connected_clients[MAX_SESSION_COUNT];
int available_clients[MAX_SESSION_COUNT];

/********************************************************************************************************* */

/*void *client_thread_fn(void* client_args){
  client_t *client = (client_t *) client_args;
  client_handler(client);
  free(client);
  pthread_exit(NULL);
}*/

int client_handler(client_t *client){
  char connect_result = 0;
  //TEMOS DE MUDAR O RESULT PARA 1 EM ALGUNS CASOS???
  
  client->notif_pipe_fd = open(client->notif_pipe_path, O_WRONLY);
  if (client->notif_pipe_fd == -1){
    perror("[ERR]: notif_pipe open failed");
    return EXIT_FAILURE;
  }

  client->req_pipe_fd = open(client->req_pipe_path, O_RDONLY);
  if (client->req_pipe_fd == -1){
    perror("[ERR]: req_pipe open failed");
    return EXIT_FAILURE;
  }

  client->resp_pipe_fd = open(client->resp_pipe_path, O_WRONLY);
  if (client->resp_pipe_fd == -1){
    perror("[ERR]: server_pipe open failed");
    return EXIT_FAILURE;
  }

  char response_message[2] = {OP_CODE_CONNECT, connect_result};

  if (write_all(client->resp_pipe_fd, response_message, sizeof(response_message)) == -1){
    perror("[ERR]: Failed to send connect message");
    return EXIT_FAILURE;
  }

  while(1){
    char op_code;
    int interrupt = 0;
    
    int read_result = read_all(client->req_pipe_fd, &op_code, sizeof(op_code), &interrupt);
    if (read_result == -1) {
      if(interrupt){
        fprintf(stderr, "[INFO]: Read interrupted\n");
        interrupt = 0;
        continue;
      }
      fprintf(stderr, "[ERR]: Failed to read from Request FIFO\n");
      break;
    } else if (read_result == 0) {
        fprintf(stderr, "[INFO]: Request FIFO closed.\n");
        break;
    }

    switch (op_code){

      case OP_CODE_DISCONNECT:{
        handle_disconnect(client);

        //TODO (1.2): LIBERTAR A MEMÓRIA ASSSOCIADA A ESTE CLIENTE
        //TODO (1.2): FAZER CLOSE DE TUDO O QUE ESTÁ RELACIONADO COM ESTE CLIENTE
        //TODO (1.2): DIZER QUE A POSIÇÃO DESTE CLIENT NO VETOR available_clients (MATER A 0)
        //free(client);
        break;
      }

      case OP_CODE_SUBSCRIBE:{
        char key_placeholder[MAX_STRING_SIZE + 1] = {0};

        //Ver como é que a interrupção funcionaria neste caso
        read_result = read_all(client->req_pipe_fd, key_placeholder, sizeof(key_placeholder), &interrupt);
        if (read_result == -1) {
          if(interrupt){
            fprintf(stderr, "[INFO]: Read interrupted\n");
            interrupt = 0;
            continue;
          }
          fprintf(stderr, "[ERR]: Failed to read from Request FIFO\n");
          break;
        } else if (read_result == 0) {
            fprintf(stderr, "[INFO]: Request FIFO closed.\n");
            break;
        }

        /*if(read(client->req_pipe_fd, key, sizeof(key)) <= 0){
          perror("[ERR]: Failed to read key for subscription");
          break;
        }*/

        char key[MAX_STRING_SIZE] = {0};
        strncpy(key, key_placeholder, MAX_STRING_SIZE);

        if (handle_subscribe(key, client) != 0){
          fprintf(stderr, "[ERR]: Subscription handling failed for key '%s'\n", key);
        }

        break;
      }  
      
      case OP_CODE_UNSUBSCRIBE:{
        printf("f %d", op_code);
        //Mandamos 41 bytes na chave, só queremos 40, é só esse tamanho que é aceite pelo programa.
        char key_placeholder[MAX_STRING_SIZE + 1] = {0};

        read_result = read_all(client->req_pipe_fd, key_placeholder, sizeof(key_placeholder), &interrupt);
        if (read_result == -1) {
          if(interrupt){
            fprintf(stderr, "[INFO]: Read interrupted\n");
            interrupt = 0;
            continue;
          }
          fprintf(stderr, "[ERR]: Failed to read from Request FIFO\n");
          break;
        } else if (read_result == 0) {
            fprintf(stderr, "[INFO]: Request FIFO closed.\n");
            break;
        }

        char key[MAX_STRING_SIZE] = {0};
        strncpy(key, key_placeholder, MAX_STRING_SIZE);

        /*if(read(client->req_pipe_fd, key, sizeof(key) <= 0)){
          perror("[ERR]: Failed to read key for unsubscription");
          break;
        }*/

        if (handle_unsubscribe(key, client) != 0){
          fprintf(stderr, "[ERR]: Unsubscription handling failed for key '%s'\n", key);
        }
        
        break;
      }
       
      default:{
        fprintf(stderr, "[ERR]: Unknown OP_CODE '%d'\n", op_code);
        break;
      }
    }
  }

  return 0;
}

/********************************************************************************************************************************** */

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values, connected_clients)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd, connected_clients)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void dispatch_threads(DIR *dir, const char *server_pipe_path) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  int interrupt = 0;
  int first_available_position = -1;

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  /*************************************************** SECÇÃO DO FIFO DE REGISTO ********************************************************/
  
  // A tarefa anfitriã é apenas a tarefa main no server (deve ficar à espera num ciclo de leitura no dispatch_threads após as threads dos .job serem criadas)

  // O FIFO de registo deve ser aberto com permissões de read-write

  int server_pipe_fd = open(server_pipe_path, O_RDWR);
  if ((server_pipe_fd) == -1){
    perror("[ERR]: server_pipe open failed");
    //Fechar os pipes
    exit(EXIT_FAILURE);
  }

  // ler do FIFO de registo
  while(1){
    char server_buffer[1 + 3 * MAX_PIPE_PATH_LENGTH];

    int read_result = read_all(server_pipe_fd, server_buffer, sizeof(server_buffer), &interrupt);

    if (read_result == -1) {
      if(interrupt){
        fprintf(stderr, "[INFO]: Read interrupted\n");
        interrupt = 0;
        continue;
      }
      fprintf(stderr, "[ERR]: Failed to read from Notification FIFO\n");
      break;
    } else if (read_result == 0) {
        fprintf(stderr, "[INFO]: Server FIFO closed.\n");
        break;
    } else {
        const char op_code = server_buffer[0];

        if(op_code != OP_CODE_CONNECT){
          fprintf(stderr, "[INFO]: OP_CODE is incorrect for connect operation. Should equal OP_CODE_CONNECT (1)\n");
          continue;
        }

        for(int i = 0; i < MAX_SESSION_COUNT; i++){
          if(available_clients[i] == 0){
            first_available_position = i;
            break;
          }
        }

        client_t *client = malloc(sizeof(client_t));

        connected_clients[first_available_position] = client;
        available_clients[first_available_position] = 1;

        client_init(client, first_available_position);
        
        strncpy(client->req_pipe_path, server_buffer + 1, MAX_PIPE_PATH_LENGTH);
        strncpy(client->resp_pipe_path, server_buffer + 1 + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
        strncpy(client->notif_pipe_path, server_buffer + 1 + 2 * MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);

        client_handler(client);
        free(client);
        available_clients[first_available_position] = 0;
    }
  }

  /*************************************************** SECÇÃO DO FIFO DE REGISTO ********************************************************/

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}




int main(int argc, char **argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <reg_fifo_name>\n");
    return 1;
  }

  const char *reg_fifo_name = argv[4];
  if(create_pipe(reg_fifo_name)){
    return EXIT_FAILURE;
  }

  /*pthread_t client_thread[MAX_SESSION_COUNT];
  for(int i = 0; i < MAX_SESSION_COUNT; i++){

    client_t *client = malloc(sizeof(client_t));

    client_init(client, i);

    if (pthread_create(&client_thread[i], NULL, client_handler, (void *)client) != 0) {
      fprintf(stderr, "Failed to create client handler thread: %s\n", strerror(errno));
      // Terminar as outras threads que já tinham sido criadas de forma bem sucedida antes de terminar 
      // o programa, de modo a evitar memory leaks
      pthread_detach(client_thread[i]);
     return 1;
    }

  }*/

  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }
  
  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir, reg_fifo_name);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
