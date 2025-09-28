#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define MAX_CLIENTS 400     // max number of client requests served at a time
#define MAX_BYTES 4096       // max allowed byte_size of request/response from/to the server
#define MAX_SIZE 200*(1<<20)     // total size of the cache
#define MAX_ELEMENT_SIZE 10*(1<<20)     // max size of one node in cache 



typedef struct cache_element cache_element;

// implementing cache elements as a LinkedList
struct cache_element{
    char* data;      // strores response of the data
    int len;          // length of data i.e.. sizeof(data)...
    char* url;        // url stores the http request
	time_t lru_time_track;   // stores the latest time when the element is accesed
    cache_element* next;    // pointer to next element
};

cache_element* head;        // starting pointer to the cache
int cache_size;             // denotes current size of the cache

cache_element* find(char* url);  // finding the element based on URL
int add_cache_element(char* data,int size,char* url);  // add new element in the cache if it is not present already
void remove_cache_element(); // remove the cache based on time (LRU)



int port_number = 8080;             // Default Port
int proxy_socketId;	                // socket descriptor of our proxy server
pthread_t tid[MAX_CLIENTS];         // array to store the thread ids of clients
sem_t seamaphore;	                // if client requests exceeds the max_clients this seamaphore puts the waiting threads to sleep and wakes them when traffic on queue decreases by giving signal
pthread_mutex_t lock;               // lock is used for locking the cache so that no read and write operation preformed at a same time by more than one thread


int sendErrorMessage(int socket, int status_code);

int checkHTTPversion(char *msg){
	int version = -1;
	if(strncmp(msg, "HTTP/1.1", 8) == 0) version = 1;
	else if(strncmp(msg, "HTTP/1.0", 8) == 0) version = 1;	// Handling this similar to version 1.1
	else version = -1;

	return version;
}


void* thread_fn(void* socketNew);
int handle_request(int clientSocket, ParsedRequest *request, char *tempReq);
int connectRemoteServer(char* host_addr, int port_num);


int main(int argc, char * argv[]){

	int client_socketId;   // store the client socketId
	int client_len;  // store the length of the request
	struct sockaddr_in server_addr, client_addr; 

    sem_init(&seamaphore, 0, MAX_CLIENTS); 
    pthread_mutex_init(&lock, NULL); 
    
	//checking whether two arguments are received or not (./proxy 8080)	
	if(argc == 2)  port_number = atoi(argv[1]);  // 8080
	else{
		printf("Too few arguments\n");
		exit(1);  // if we have not given port number then exit the programe
	}
	printf("Setting Proxy Server Port : %d\n",port_number);

	
	proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);   //creating the proxy socket

	if(proxy_socketId < 0){
		perror("Failed to create socket.\n");
		exit(1);
	}

	int reuse = 1;
	if(setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0) 
        perror("setsockopt(SO_REUSEADDR) failed\n");

	bzero((char*)&server_addr, sizeof(server_addr));  // handling garbage value
	
	server_addr.sin_family = AF_INET;  // IPv4
	server_addr.sin_port = htons(port_number); // Assigning port to the Proxy
	server_addr.sin_addr.s_addr = INADDR_ANY; // Any available adress assigned

	if(bind(proxy_socketId, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0){  // Binding the socket
		perror("Port is not free\n");
		exit(1);
	}
	printf("Binding on port: %d\n",port_number);

	int listen_status = listen(proxy_socketId, MAX_CLIENTS); // Proxy socket listening to the requests
	if(listen_status < 0){
		perror("Error while Listening !\n");
		exit(1);
	}


	int i = 0; // Iterator for thread_id (tid) and Accepted Client_Socket for each thread
	int Connected_socketId[MAX_CLIENTS];  // stores descriptors of connected clients

	while(1){
		bzero((char*)&client_addr, sizeof(client_addr));  
		client_len = sizeof(client_addr); 

        // Accepting the connections
		client_socketId = accept(proxy_socketId, (struct sockaddr*)&client_addr,(socklen_t*)&client_len);
		if(client_socketId < 0){
			fprintf(stderr, "Error in Accepting connection !\n");
			exit(1);
		}
		else Connected_socketId[i] = client_socketId; // Storing accepted client

		// Getting IP address and port number of client
		struct sockaddr_in* client_pt = (struct sockaddr_in*)&client_addr; // copy value
		struct in_addr ip_addr = client_pt->sin_addr; // extracting address
		char str[INET_ADDRSTRLEN];	// INET_ADDRSTRLEN: Default ip address size (16)

		inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN);
		printf("Client is connected with port number: %d and ip address: %s \n",ntohs(client_addr.sin_port), str);
		printf("Socket values of index %d in main function is %d\n",i, client_socketId);

		pthread_create(&tid[i], NULL, thread_fn, (void*)&Connected_socketId[i]); // Creating thread for each client accepted
		i++; 
	}

	close(proxy_socketId);	 // Close socket
 	return 0;
}

void* thread_fn(void* socketNew){
	sem_wait(&seamaphore); 
	int p;
	sem_getvalue(&seamaphore, &p);
	printf("semaphore value:%d\n", p);

    int* t = (int*)(socketNew);
	int socket = *t;           // socket is descriptor of the connected Client
	int bytes_send_client, len;	  // Bytes Transferred

	char *buffer = (char*)calloc(MAX_BYTES,sizeof(char));	// buffer of 4kb for a client
	bzero(buffer, MAX_BYTES);	// Making buffer zero

	bytes_send_client = recv(socket, buffer, MAX_BYTES, 0); // Receiving the Request of client by proxy server
	
	while(bytes_send_client > 0){
		len = strlen(buffer);
		if(strstr(buffer, "\r\n\r\n") == NULL)  // checks if the buffer contains the sequence "\r\n\r\n"
			bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
		else break;  // full HTTP request header obtained → exit the loop
	}

	printf("---------------------------------------------------------------------------------\n");
	printf("%s\n",buffer);
	printf("----------------------------------------%d-------------------------------------\n",strlen(buffer));
	
	char *tempReq = (char*)malloc(strlen(buffer)*sizeof(char)+1);
    //tempReq and buffer both store the http request sent by client
	for(int i = 0; i < strlen(buffer); i++) tempReq[i] = buffer[i];  // making a copy
	
	struct cache_element* temp = find(tempReq);  //checking for the request in cache 

	if(temp != NULL){  //request found in cache, so sending the response to client from proxy's cache
		int size = temp->len/sizeof(char);
		int pos = 0;
		char response[MAX_BYTES];
		
		while(pos < size){
			bzero(response, MAX_BYTES);
			for(int i = 0; i < MAX_BYTES; i++){
				response[i] = temp->data[pos];
				pos++;
			}
			send(socket, response, MAX_BYTES, 0);
		}

		printf("Data retrived from the Cache\n\n");
		printf("%s\n\n", response);
		// close(socketNew);
		// sem_post(&seamaphore);
		// return NULL;
	}	
	else if(bytes_send_client > 0){
		len = strlen(buffer); 
		
		ParsedRequest* request = ParsedRequest_create();  //Parsing the request
        //ParsedRequest_parse returns 0 on success and -1 on failure. On success it stores parsed request in the request
		if(ParsedRequest_parse(request, buffer, len) < 0) printf("Parsing failed\n");
		else{	
			bzero(buffer, MAX_BYTES);
			if(!strcmp(request->method, "GET")){
				if(request->host && request->path && (checkHTTPversion(request->version) == 1)){
					bytes_send_client = handle_request(socket, request, tempReq);  // Handle GET request
					if(bytes_send_client == -1) sendErrorMessage(socket, 500);
				}
				else sendErrorMessage(socket, 500);			// 500 Internal Error
			}
            else printf("This code doesn't support any method other than GET\n");
		}
       
		ParsedRequest_destroy(request);  //freeing up the request pointer
	}

	else if(bytes_send_client < 0) perror("Error in receiving from client.\n");
	else if(bytes_send_client == 0) printf("Client disconnected!\n");

	shutdown(socket, SHUT_RDWR);
	close(socket);
	free(buffer);
	sem_post(&seamaphore);	
	
	sem_getvalue(&seamaphore,&p);
	printf("Semaphore post value:%d\n",p);
	free(tempReq);
	return NULL;
}

int handle_request(int clientSocket, ParsedRequest *request, char *tempReq){
	char *buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	strcpy(buf, "GET ");
	strcat(buf, request->path);
	strcat(buf, " ");
	strcat(buf, request->version);
	strcat(buf, "\r\n");

	size_t len = strlen(buf);

	if(ParsedHeader_set(request, "Connection", "close") < 0) printf("set header key not work\n");
	// tells the server not to keep the TCP connection alive after the response, but to close it after one request-response cycle.

	if(ParsedHeader_get(request, "Host") == NULL)
		if(ParsedHeader_set(request, "Host", request->host) < 0)
			printf("Set \"Host\" header key not working\n");
	//  if the client’s HTTP request does NOT already include a Host header, adds a Host header manually, using the value stored in request->host

	if(ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) printf("unparse failed\n");
	//  converts the modified request structure (i.e., parsed headers) back into a plain HTTP header string

	int server_port = 80;  // Default Remote Server Port
	if(request->port != NULL) server_port = atoi(request->port);  // atoi --> string to integer

	int remoteSocketID = connectRemoteServer(request->host, server_port);
	if(remoteSocketID < 0) return -1;

	int bytes_send = send(remoteSocketID, buf, strlen(buf), 0);
	bzero(buf, MAX_BYTES);  // memset(buf, 0, MAX_BYTES);
	bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0); 

	char *temp_buffer = (char*)malloc(sizeof(char)*MAX_BYTES);   //temp buffer
	int temp_buffer_size = MAX_BYTES;
	int temp_buffer_index = 0;

	while(bytes_send > 0){
		bytes_send = send(clientSocket, buf, bytes_send, 0);
		
		for(int i = 0; i < bytes_send/sizeof(char); i++){
			temp_buffer[temp_buffer_index] = buf[i];
			temp_buffer_index++;
		}
		temp_buffer_size += MAX_BYTES;  // increasing the size of receiving the next chunk 
		temp_buffer = (char*)realloc(temp_buffer, temp_buffer_size);

		if(bytes_send < 0){
			perror("Error in sending data to client socket.\n");
			break;
		}
		bzero(buf, MAX_BYTES);
		bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);  // Receive the next chunk from server
	} 

	temp_buffer[temp_buffer_index] = '\0';  // null character at last index

	free(buf);
	add_cache_element(temp_buffer, strlen(temp_buffer), tempReq);
	printf("Done\n");
	free(temp_buffer);
	
 	close(remoteSocketID);
	return 0;
}

// host_addr → hostname (e.g., "example.com") or IP (e.g., "142.250.77.206").
// port_num → port number to connect to (e.g., 80 for HTTP).
int connectRemoteServer(char* host_addr, int port_num){  // Creating Socket for remote server 
	int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);
	if(remoteSocket < 0){
		printf("Error in Creating Socket.\n");
		return -1;
	}
	
	struct hostent *host = gethostbyname(host_addr);  // Get host by the name or ip address provided
	if(host == NULL){
		fprintf(stderr, "No such host exists.\n");	
		return -1;
	}

	struct sockaddr_in server_addr;  // inserts ip address and port number of host in struct 'server_addr'
	bzero((char*)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port_num);  // htons() converts an integer into network-byte-order (big-endian).

	bcopy((char *)host->h_addr,(char *)&server_addr.sin_addr.s_addr,host->h_length);
	// Copies the IP address obtained from gethostbyname into server_addr.sin_addr.

	if(connect(remoteSocket, (struct sockaddr*)&server_addr, (socklen_t)sizeof(server_addr)) < 0){ // Connect to Remote server (Attempt a TCP connection handshake with that server.)
		fprintf(stderr, "Error in connecting !\n"); 
		return -1;
	}

	return remoteSocket;
}


// Checks for url in the cache if found returns pointer to the respective cache element or else returns NULL
cache_element* find(char* url){
    cache_element* site = NULL;

    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 

    if(head != NULL){
        site = head;
        while(site!=NULL){  // can we optimize it (can we use QUEUE) 
            if(!strcmp(site->url, url)){  
				printf("LRU Time Track Before : %ld", site->lru_time_track);
                printf("\nurl found\n");
				
				site->lru_time_track = time(NULL);  // Updating the time_track
				printf("LRU Time Track After : %ld", site->lru_time_track);
				break;
            }
            site = site->next;
        }       
    }
	else printf("\nurl not found\n");

    temp_lock_val = pthread_mutex_unlock(&lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 

    return site;
}


// If cache is not empty searches for the node which has the least lru_time_track and deletes it
void remove_cache_element(){
    cache_element * p ;  	// will point to the node just before temp, so we can unlink it
	cache_element * q ;		// used for traversing
	cache_element * temp;	// // will point to the element to delete

    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 

	if(head != NULL){ // Cache != empty
		for(q = head, p = head, temp = head ; q -> next != NULL; q = q -> next){ // Iterate through entire cache and search for oldest time track
			if(((q -> next) -> lru_time_track) < (temp -> lru_time_track)){
				temp = q -> next;  
				p = q;
			}
		}
		if(temp == head) head = head -> next; // *Handle the base case* --> If it's the first element
		else p->next = temp->next;	   // Unlink it from middle or end

		cache_size = cache_size - (temp -> len) - sizeof(cache_element) - strlen(temp -> url) - 1;  

		free(temp->data);      		
		free(temp->url);  
		free(temp);
	} 

    temp_lock_val = pthread_mutex_unlock(&lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
}


int add_cache_element(char* data, int size, char* url){   // Add element to the cache
    int temp_lock_val = pthread_mutex_lock(&lock);  // must lock to prevent race conditions in multi-threaded proxy.
	printf("Add Cache Lock Acquired %d\n", temp_lock_val);

    int element_size = size + 1 + strlen(url) + sizeof(cache_element); // Size of new ele which will be added to cache
    if(element_size > MAX_ELEMENT_SIZE){  // don’t allow elements too large to be cached at all.
        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
        return 0;
    }
    else{    // We keep removing elements from cache until we get enough space to add the element
		while((cache_size + element_size) > MAX_SIZE) remove_cache_element();  

        cache_element* element = (cache_element*) malloc(sizeof(cache_element)); 
        element->data = (char*)malloc(size + 1); // Allocating memory for the response to be stored in the cache element
		strcpy(element->data, data); 
        element->url = (char*)malloc(1 + (strlen(url)*sizeof(char))); // Allocating memory for the request to be stored in the cache element (as a key)
		strcpy(element->url, url);
		
		element->lru_time_track = time(NULL);   // Updating the time_track
        element->next = head;  // Adds to the head of list → recently used goes to front
        element->len = size; 
        head = element;  // setting the head
        cache_size += element_size;

        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
        return 1;
    }
	
    return 0;
}

int sendErrorMessage(int socket, int status_code){
	char str[1024];
	char currentTime[50];
	time_t now = time(0);

	struct tm data = *gmtime(&now);
	strftime(currentTime,sizeof(currentTime),"%a, %d %b %Y %H:%M:%S %Z", &data);

	switch(status_code){
		case 400: snprintf(str, sizeof(str), "HTTP/1.1 400 Bad Request\r\nContent-Length: 95\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>400 Bad Request</TITLE></HEAD>\n<BODY><H1>400 Bad Rqeuest</H1>\n</BODY></HTML>", currentTime);
				  printf("400 Bad Request\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 403: snprintf(str, sizeof(str), "HTTP/1.1 403 Forbidden\r\nContent-Length: 112\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>403 Forbidden</TITLE></HEAD>\n<BODY><H1>403 Forbidden</H1><br>Permission Denied\n</BODY></HTML>", currentTime);
				  printf("403 Forbidden\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 404: snprintf(str, sizeof(str), "HTTP/1.1 404 Not Found\r\nContent-Length: 91\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Found</TITLE></HEAD>\n<BODY><H1>404 Not Found</H1>\n</BODY></HTML>", currentTime);
				  printf("404 Not Found\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 500: snprintf(str, sizeof(str), "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 115\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>500 Internal Server Error</TITLE></HEAD>\n<BODY><H1>500 Internal Server Error</H1>\n</BODY></HTML>", currentTime);
				  //printf("500 Internal Server Error\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 501: snprintf(str, sizeof(str), "HTTP/1.1 501 Not Implemented\r\nContent-Length: 103\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Implemented</TITLE></HEAD>\n<BODY><H1>501 Not Implemented</H1>\n</BODY></HTML>", currentTime);
				  printf("501 Not Implemented\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 505: snprintf(str, sizeof(str), "HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 125\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>505 HTTP Version Not Supported</TITLE></HEAD>\n<BODY><H1>505 HTTP Version Not Supported</H1>\n</BODY></HTML>", currentTime);
				  printf("505 HTTP Version Not Supported\n");
				  send(socket, str, strlen(str), 0);
				  break;

		default: return -1;
	}

	return 1;
}