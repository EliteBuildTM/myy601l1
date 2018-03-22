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
#define SIZE_OF_QUEUE             11
#define THREAD_SIZE               10


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
}Node;

// Definition of the database.
KISSDB *db = NULL;

//Definition of global variables
Node Req_Queue[SIZE_OF_QUEUE];

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

struct timeval total_waiting_time;
struct timeval total_service_time;
int complete_requests;

/*
@check_tail_bounds- Checks if tail's position is out o bounds
*/
void check_tail_bounds(){
    if(tail>(SIZE_OF_QUEUE-1)){
      pthread_mutex_lock(&queue_mutex);
      tail=0;
      pthread_mutex_unlock(&queue_mutex);
    }
}

/*
@check_head_bounds- Checks if tail's position is out o bounds
*/
void check_head_bounds(){
  if(head>(SIZE_OF_QUEUE-1)){
    pthread_mutex_lock(&queue_mutex);
    head=0;
    pthread_mutex_unlock(&queue_mutex);
  }
}

/*
@calculate_waiting_time- Calculates the request's waiting time
*/
void calculate_waiting_time(long secs,long usecs){
  struct timeval waiting_time;
  pthread_mutex_lock(&time_mutex);
  gettimeofday(&waiting_time,NULL);
  //Locking and calculating waiting time for the request
  total_waiting_time.tv_sec=(waiting_time.tv_sec)-(secs);
  total_waiting_time.tv_usec=(waiting_time.tv_usec)-(usecs);
  pthread_mutex_unlock(&time_mutex);
}

/*
@calculate_service_time- Calculates the request's service time
*/
void calculate_service_time(struct timeval end,struct timeval start){
  //Locking and calculating waiting time for the request
  pthread_mutex_lock(&time_mutex);
  total_service_time.tv_sec=(end.tv_sec)-(start.tv_sec);
  total_service_time.tv_usec=(end.tv_usec)-(start.tv_usec);
  pthread_mutex_unlock(&time_mutex);
}

/*
@put_queue- Producer uses this to put new requests in the queue
*/
void put_request(int fd){
  pthread_mutex_lock(&queue_mutex);
  Req_Queue[tail].fd=fd;
  //Get the start time
  gettimeofday(&Req_Queue[tail].start_time,NULL);
  tail++;
  check_tail_bounds();
  pthread_mutex_unlock(&queue_mutex);
}

/*
@get_request -Consumer Threads use this to get requests from queue
--This function returns one Node of the queue
*/
Node get_request(){
  Node request;
  pthread_mutex_lock(&queue_mutex);
  //Calling calculate_waiting_time
  calculate_waiting_time(Req_Queue[head].start_time.tv_sec,Req_Queue[head].start_time.tv_usec);
  request=Req_Queue[head];
  head++;
  check_head_bounds();
  pthread_mutex_unlock(&queue_mutex);
  return request;
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
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);
            break;
          case PUT:
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value))
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
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
      //should check for return value of wait
      pthread_cond_wait(&not_empty,&queue_mutex);
    }
    request=get_request();
    //gets the request's descriptor to call process_request
    req_fd=request.fd;
    //Send this signal to the Producer thread in case it waits
    pthread_cond_signal(&not_full);
    pthread_mutex_unlock(&queue_mutex);
    //call process_request to serve the request
    gettimeofday(&service_start,NULL);
    process_request(req_fd);
    close(req_fd);
    gettimeofday(&service_end,NULL);
    //call calculate_service_time
    calculate_service_time(service_end,service_start);
  }
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  int i;
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

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
		  pthread_mutexattr_init(&attr) != 0 ||
		  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0 ||
		  pthread_mutex_init(&time_mutex, &attr) != 0 ||
		  pthread_mutex_init(&time_mutex, &attr) != 0) {

	  fprintf(stderr,"(Error) main: Cannot initialize synchronization locks");
	  return 1;
  }

  pthread_mutexattr_destroy(&attr);

  for(i=0;i<THREAD_SIZE;i++){
    pthread_create(&Threads[i],NULL,thread_start,NULL);
  }

  // main loop: wait for new connection/requests
  while (1) {
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }

    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    pthread_mutex_lock(&queue_mutex);
    while(is_Full()){
      //if the queue is Full with requests then it waits for the signal from the consumer
      //should check for the returning value of wait
      pthread_cond_wait(&not_full,&queue_mutex);
    }
    //adds a request in the queue
    put_request(new_fd);
    //send signal to the consumers that the queue is not empty
    //should check for the returning value of signal
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
