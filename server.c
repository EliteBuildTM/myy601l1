/* server.c
   Sample code of
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina
   (c) S. Anastasiadis, G. Kappes 2016
*/


#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define SIZE_OF_QUEUE            200
#define THREAD_SIZE               20


// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation;

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];
  char value[VALUE_SIZE];
} Request;

//Each node of the Req_Queue consists of the descriptor and the conection start_time
typedef struct node {
  int fd;
  struct timeval start_time;
} Node;

// Definition of the database.
KISSDB *db = NULL;

//Definition of global variables
Node Req_Queue[SIZE_OF_QUEUE];
int while_breaker=0;
//Define array of Threads
pthread_t Threads [THREAD_SIZE];

//head and tail Initialized at the first position of the queue
int head=0;
int tail=0;

//Initialize conditions for the signals
pthread_cond_t not_full;
pthread_cond_t not_empty;
//queue_mutex => to use with Req_Queue,head,tail and signals conditions
pthread_mutex_t queue_mutex;
//time_mutex => to use with time counters and complete_requests
pthread_mutex_t time_mutex;
pthread_mutex_t while_mutex = PTHREAD_MUTEX_INITIALIZER;
//Mutex and conditions for the readers and writers
pthread_mutex_t read_write_mutex;
pthread_cond_t reader_inQueue;
pthread_cond_t writer_inQueue;
int readers=0,writers=0;

struct timeval total_waiting_time;
struct timeval total_service_time;
int complete_requests=0;

/*
@check_bounds- Checks if var's position is out o bounds
*/
void check_bounds(int *var){
    if(*var>(SIZE_OF_QUEUE-1)){
      pthread_mutex_lock(&queue_mutex);
      *var=0;
      pthread_mutex_unlock(&queue_mutex);
    }
}

/*
@is_Full - Returns one if Req_Queue is full
*/
int is_Full(){
  if((tail-head)==(SIZE_OF_QUEUE-1)||(head-tail)==1){
    return 1;
    //Is full
  }

  return 0;
}

/*
@is_Empty - Returns one if Req_Queue is empty
*/
int is_Empty(){
  if(head==tail){
    return 1;
    //The queue is empty if head and tail point at the same position
  }
  return 0;

}

/*
 * @calculate_time- Calculates the request's time.
 *
 * @param update_value: Pointer to total_waiting_time or to total_service_time
 */
void calculate_time(struct timeval *update_value, struct timeval start, struct timeval end){
  struct timeval result;
  timersub(&end, &start, &result);
  pthread_mutex_lock(&time_mutex);
  timeradd(update_value, &result, update_value);
  pthread_mutex_unlock(&time_mutex);
}

/*
@put_queue- Producer uses this to put new requests in the queue
*/
void put_request(int fd){
  pthread_mutex_lock(&queue_mutex);

  Req_Queue[tail].fd=fd;
  gettimeofday(&Req_Queue[tail].start_time,NULL);
  tail++;
  check_bounds(&tail);

  pthread_mutex_unlock(&queue_mutex);
}

/*
@get_request -Consumer Threads use this to get requests from queue
--This function returns one Node of the queue
*/
Node get_request(){
  Node request;
  struct timeval end;

  pthread_mutex_lock(&queue_mutex);

  gettimeofday(&end, NULL);
  calculate_time(&total_waiting_time, Req_Queue[head].start_time, end); //bottleneck
  request=Req_Queue[head];
  head++;
  check_bounds(&head);

  pthread_mutex_unlock(&queue_mutex);

  return request;
}

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;

  // Check arguments.
  if (!buffer)
    return NULL;

  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }

  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }

  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);

    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);

    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:
          pthread_mutex_lock(&read_write_mutex);
            while(writers>0){
              pthread_cond_wait(&writer_inQueue,&read_write_mutex);
            }
            readers++;
            pthread_mutex_unlock(&read_write_mutex);
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);
            readers--;
            if(readers==0){
              pthread_mutex_lock(&read_write_mutex);
              pthread_cond_signal(&reader_inQueue);
              pthread_mutex_unlock(&read_write_mutex);
            }
            break;
          case PUT:
            // Write the given key/value pair to the database.
            pthread_mutex_lock(&read_write_mutex);
            while(readers>0){
              pthread_cond_wait(&reader_inQueue,&read_write_mutex);
            }
            writers++;
            pthread_mutex_unlock(&read_write_mutex);
            if (KISSDB_put(db, request->key, request->value))
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
            writers--;
            if(writers==0){
              pthread_mutex_lock(&read_write_mutex);
              pthread_cond_signal(&writer_inQueue);
              pthread_mutex_unlock(&read_write_mutex);
            }
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}

/*@TSTP_handler->Ctrl+Z handler
-Joins all threads
-Calculates and prints avarage waiting and service time
-Exits the server process
*/
void TSTP_handler(){
  int i;
  long avg_waiting_time_secs,avg_service_time_secs;
  long avg_waiting_time_usecs,avg_service_time_usecs;

  pthread_mutex_lock(&while_mutex);
  while_breaker=1;
  pthread_mutex_unlock(&while_mutex);

  pthread_mutex_lock(&queue_mutex);
  pthread_cond_broadcast(&not_empty);
  pthread_mutex_unlock(&queue_mutex);

  for (i=0; i<THREAD_SIZE;i++) {
    pthread_join(Threads[i], NULL);
  }

  avg_waiting_time_secs=total_waiting_time.tv_sec/complete_requests;
  avg_waiting_time_usecs=total_waiting_time.tv_usec/complete_requests;
  avg_service_time_secs=total_service_time.tv_sec/complete_requests;
  avg_service_time_usecs=total_service_time.tv_usec/complete_requests;

  printf("NUMBER OF COMPLETED REQUESTS : %d\n",complete_requests );
  printf("AVERAGE WAITING TIME : %ld,%ld seconds\n",avg_waiting_time_secs,avg_waiting_time_usecs);
  printf("AVERAGE SERVICE TIME : %ld,%ld seconds\n",avg_service_time_secs,avg_service_time_usecs );

  printf("Terminating.....\n");
  sleep(2);
  exit(1);
}

/*
@ thread_start =>is the startup point for the Threads
--Excecutes get_request and process_request
--Pass NULL as argument
*/
void *thread_start (void * argument){
  int req_fd;
  struct timeval service_start,service_end;
  Node request;

  while(1){
    pthread_mutex_lock(&queue_mutex);

    while(is_Empty()){
	  if(while_breaker) {
    	  pthread_mutex_unlock(&queue_mutex);
		  return NULL;
	  }
      pthread_cond_wait(&not_empty,&queue_mutex);
      if(while_breaker) {
    	  pthread_mutex_unlock(&queue_mutex);
    	  return NULL;
      }
    }

    gettimeofday(&service_start,NULL);

    request=get_request();
    pthread_cond_signal(&not_full);
    req_fd=request.fd;
    process_request(req_fd);

    gettimeofday(&service_end, NULL);
    pthread_mutex_unlock(&queue_mutex);

    calculate_time(&total_service_time, service_start, service_end);

    pthread_mutex_lock(&time_mutex);
    complete_requests++;
    pthread_mutex_unlock(&time_mutex);
    close(req_fd);
  }

  return NULL;
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  int i;
  pid_t server_pid;
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  server_pid=getpid();
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information
  signal(SIGTSTP,TSTP_handler);   //signal handler for Ctrl+Z
  printf("Server's pid %d\n",server_pid);//to use kill function
  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);

  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);

  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");

  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }

  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }


  pthread_mutexattr_t attr;

  if (pthread_cond_init(&not_empty,NULL) != 0 ||
		  pthread_cond_init(&not_full,NULL) != 0 ||
      pthread_cond_init(&writer_inQueue,NULL) != 0 ||
      pthread_cond_init(&reader_inQueue,NULL) != 0 ||
		  pthread_mutexattr_init(&attr) != 0 ||
		  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0 ||
		  pthread_mutex_init(&time_mutex, &attr) != 0 ||
      pthread_mutex_init(&read_write_mutex, &attr) != 0 ||
		  pthread_mutex_init(&queue_mutex, &attr) != 0) {

	  fprintf(stderr,"(Error) main: Cannot initialize synchronization locks");
	  return 1;
  }

  pthread_mutexattr_destroy(&attr);

  for(i=0;i<THREAD_SIZE;i++){
    pthread_create(&Threads[i],NULL,thread_start,NULL);
  }

  while (1) {
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }

    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));

    pthread_mutex_lock(&queue_mutex);

    while(is_Full()){
      pthread_cond_wait(&not_full,&queue_mutex);
    }
    put_request(new_fd);
    pthread_cond_signal(&not_empty);

    pthread_mutex_unlock(&queue_mutex);
  }

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0;
}
