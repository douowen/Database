#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "./comm.h"
#include "./db.h"
#ifdef __APPLE__
#include "pthread_OSX.h"
#endif

/*
 * Use the variables in this struct to synchronize your main thread with client
 * threads. Note that all client threads must have terminated before you clean
 * up the database.
 */
typedef struct server_control {
    pthread_mutex_t server_mutex;
    pthread_cond_t server_cond;
    int num_client_threads;
} server_control_t;

/*
 * Controls when the clients in the client thread list should be stopped and
 * let go.
 */
typedef struct client_control {
    pthread_mutex_t go_mutex;
    pthread_cond_t go;
    int stopped;
} client_control_t;

/*
 * The encapsulation of a client thread, i.e., the thread that handles
 * commands from clients.
 */
typedef struct client {
    pthread_t thread;
    FILE *cxstr;  // File stream for input and output
    // For client list
    struct client *prev;
    struct client *next;
} client_t;

/*
 * The encapsulation of a thread that handles signals sent to the server.
 * When SIGINT is sent to the server all client threads should be destroyed.
 */
typedef struct sig_handler {
    sigset_t set;
    pthread_t thread;
} sig_handler_t;

int accept_c = 1;
client_t *thread_list_head;
pthread_mutex_t thread_list_mutex = PTHREAD_MUTEX_INITIALIZER;
client_control_t client_control = {PTHREAD_MUTEX_INITIALIZER,
                                   PTHREAD_COND_INITIALIZER, 1};
server_control_t server = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
                           0};

void *run_client(void *arg);
void *monitor_signal(void *arg);
void thread_cleanup(void *arg);

// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // TODO: Block the calling thread until the main thread calls
    int error;
    if ((error = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock client mutex.");
    }
    pthread_cleanup_push(&pthread_mutex_unlock, &client_control.go_mutex);
    // client_control_release(). See the client_control_t struct.
    while (!client_control.stopped) {
        if ((error = pthread_cond_wait(&client_control.go,
                                       &client_control.go_mutex)) != 0) {
            handle_error_en(error,
                            "Error: pthread_cond_wait on client_control_go.");
        }
    }
    pthread_cleanup_pop(1);
}

// Called by main thread to stop client threads
void client_control_stop() {
    // TODO: Ensure that the next time client threads call client_control_wait()
    // at the top of the event loop in run_client, they will block.
    int error;
    if ((error = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(error,
                        "Error: failed to lock client_control.go_mutex.");
    }

    client_control.stopped = 0;

    if ((error = pthread_mutex_unlock(&client_control.go_mutex)) != 0) {
        handle_error_en(error,
                        "Error: Failed to unlock client_control.go_mutex.");
    }
}

// Called by main thread to resume client threads
void client_control_release() {
    if (pthread_mutex_lock(&client_control.go_mutex)) {
        perror("mutex could not be locked: \n");
        exit(1);
    }
    client_control.stopped = 1;
    if (pthread_cond_broadcast(&client_control.go)) {
        perror("pthread_cond_broadcast failure: \n");
        exit(1);
    }
    if (pthread_mutex_unlock(&client_control.go_mutex)) {
        perror("mutex could not be unlocked: \n");
        exit(1);
    }
}

// Called by listener (in comm.c) to create a new client thread
void client_constructor(FILE *cxstr) {
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    // Step 2: Create the new client thread running the run_client routine.
    client_t *client = (client_t *)malloc(sizeof(client_t));
    client->cxstr = cxstr;
    int error;
    if ((error = pthread_create(&client->thread, 0, run_client, client)) != 0) {
        handle_error_en(error, "Error: Failed to create thread.");
    }
    if ((error = pthread_detach(client->thread)) != 0) {
        handle_error_en(error, "Error: Failed to detach thread.");
    }
}

void client_destructor(client_t *client) {
    // TODO: Free all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
}

// Code executed by a client thread
void *run_client(void *arg) {
    // TODO:
    // Step 1: Make sure that the server is still accepting clients.
    // Step 2: Add client to the client list and push thread_cleanup to remove
    //       it if the thread is canceled.
    // Step 3: Loop comm_serve (in comm.c) to receive commands and output
    //       responses. Note that the client may terminate the connection at
    //       any moment, in which case reading/writing to the connection stream
    //       on the server side will send this process a SIGPIPE. You must
    //       ensure that the server doesn't crash when this happens!
    // Step 4: When the client is done sending commands, exit the thread
    //       cleanly.
    //
    // Keep the signal handler thread in mind when writing this function!
    client_t *client = (client_t *)arg;
    int error;

    if (accept_c == 0) {
        client_destructor(client);
        return (void *)-1;
    }

    if ((error = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: Failed to lock the thread.");
    }

    sigset_t signal_set;
    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGPIPE);

    if ((error = pthread_sigmask(SIG_BLOCK, &signal_set, 0)) != 0) {
        handle_error_en(error, "Error: Failed to mask the signal.");
    }

    client->prev = NULL;
    if (thread_list_head) {
        client->next = thread_list_head;
        thread_list_head->prev = client;
    } else {
        client->next = NULL;
    }

    thread_list_head = client;
    pthread_cleanup_push(&thread_cleanup, client);

    if ((error = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: failed to unlock the thread mutex.");
    }

    if ((error = pthread_mutex_lock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock server mutex.");
    }
    server.num_client_threads++;

    if ((error = pthread_mutex_unlock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to unlock server mutex.");
    }

    char response[BUFLEN];
    char command[BUFLEN];
    memset(&response, 0, BUFLEN);
    memset(&command, 0, BUFLEN);
    while (comm_serve(client->cxstr, response, command) == 0) {
        client_control_wait();
        interpret_command(command, response, BUFLEN);
    }

    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, 0);
    pthread_cleanup_pop(1);
    return NULL;
}

void delete_all() {
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    int error;
    if ((error = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock the thread list mutex.");
    }

    client_t *current = thread_list_head;
    while (current) {
        client_t *temp = current->next;
        pthread_cancel(current->thread);
        current = temp;
    }

    if ((error = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(error,
                        "Error: failed to unlock the thread list mutex.");
    }
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    int error;
    if ((error = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock the thread_list_mutex.");
    }

    client_t *client = (client_t *)arg;
    client_t *next = client->next;
    client_t *prev = client->prev;
    if (client == thread_list_head) {
        thread_list_head = NULL;
    }

    if (next) {
        next->prev = prev;
    }

    if (prev) {
        prev->next = next;
    }

    if ((error = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: failed to unlock thread_list_mutex.");
    }

    client_destructor(client);

    if ((error = pthread_mutex_lock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock server mutex");
    }

    server.num_client_threads--;
    if (!server.num_client_threads) {
        if ((error = pthread_cond_signal(&server.server_cond)) != 0) {
            handle_error_en(error, "Error: failed to set server condition.");
        }
    }

    if ((error = pthread_mutex_unlock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to unlock server_mutex");
    }
}

// Code executed by the signal handler thread. For the purpose of this
// assignment, there are two reasonable ways to implement this.
// The one you choose will depend on logic in sig_handler_constructor.
// 'man 7 signal' and 'man sigwait' are both helpful for making this
// decision. One way or another, all of the server's client threads
// should terminate on SIGINT. The server (this includes the listener
// thread) should not, however, terminate on SIGINT!
void *monitor_signal(void *arg) {
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    int sig = 0;
    while (1) {
        sigwait((sigset_t *)arg, &sig);
        if (sig == SIGINT) {
            printf("SIGINT received, cancelling all clients\n");
            delete_all();
        }
    }
    return NULL;
}

sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    sig_handler_t *thread = (sig_handler_t *)malloc(sizeof(sig_handler_t));
    sigemptyset(&thread->set);
    sigaddset(&thread->set, SIGINT);
    int error;
    if ((error = pthread_sigmask(SIG_BLOCK, &thread->set, 0)) != 0) {
        handle_error_en(error, "Error: failed to execute sigmask.");
    }

    if ((error = pthread_create(&thread->thread, 0, monitor_signal,
                                &thread->set)) != 0) {
        handle_error_en(error, "Error: failed to create the thread for SIGINT");
    }

    if ((error = pthread_detach(thread->thread)) != 0) {
        handle_error_en(error, "Error: failed to detach the thread.");
    }

    return thread;
}

void sig_handler_destructor(sig_handler_t *sighandler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    int error;
    if ((error = pthread_cancel(sighandler->thread)) != 0) {
        handle_error_en(error,
                        "Error: failed to pthread_cancel for sig_handler.");
    }
    free(sighandler);
}

// The arguments to the server should be the port number.
int main(int argc, char *argv[]) {
    // TODO:
    // Step 1: Set up the signal handler.
    // Step 2: Start a listener thread for clients (see start_listener in
    //       comm.c).
    // Step 3: Loop for command line input and handle accordingly until EOF.
    // Step 4: Destroy the signal handler, delete all clients, cleanup the
    //       database, cancel the listener thread, and exit.
    //
    // You should ensure that the thread list is empty before cleaning up the
    // database and canceling the listener thread. Think carefully about what
    // happens in a call to delete_all() and ensure that there is no way for a
    // thread to add itself to the thread list after the server's final
    // delete_all().
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port number>\n", argv[0]);
        exit(1);
    }

    sig_handler_t *signal_handler = sig_handler_constructor();
    pthread_t listener_thread =
        start_listener(atoi(argv[1]), &client_constructor);

    char command[BUFLEN];
    memset(command, 0, BUFLEN);
    char *result;
    while ((result = fgets(command, BUFLEN, stdin)) != NULL) {
        if (command[0] == 's') {
            fprintf(stdout, "stopping all clients\n");
            client_control_stop();
        } else if (command[0] == 'g') {
            fprintf(stdout, "releasing all clients\n");
            client_control_release();
        } else if (command[0] == 'p') {
            char *file = strtok(&command[1], " \t\n");
            db_print(file);
        }
        memset(command, 0, BUFLEN);
    }

    int error;

    if (result == NULL) {
        if ((error = pthread_mutex_lock(&thread_list_mutex)) != 0) {
            handle_error_en(error, "Error: Failed to lock the thread.");
        }
        accept_c = 0;
        if ((error = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
            handle_error_en(error, "Error: Failed to lock the thread.");
        }
        fprintf(stdout, "exiting database\n");
    }

    delete_all();

    if ((error = pthread_mutex_lock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to lock server mutex.");
    }

    while (server.num_client_threads) {
        if ((error = pthread_cond_wait(&server.server_cond,
                                       &server.server_mutex)) != 0) {
            handle_error_en(error, "Error: failed to pthread_cond_wait.");
        }
    }

    if ((error = pthread_mutex_unlock(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to unlock server_mutex.");
    }

    db_cleanup();
    if ((error = pthread_cancel(listener_thread)) != 0) {
        handle_error_en(error, "Error: failed to cancel listener thread.");
    }

    if ((error = pthread_mutex_destroy(&client_control.go_mutex)) != 0) {
        handle_error_en(error, "Error: failed to destroy client go_mutex.");
    }

    if ((error = pthread_mutex_destroy(&server.server_mutex)) != 0) {
        handle_error_en(error, "Error: failed to destroy server go_mutex.");
    }

    if ((error = pthread_mutex_destroy(&thread_list_mutex)) != 0) {
        handle_error_en(error, "Error: failed to destroy thread list mutex.");
    }

    if ((error = pthread_cond_destroy(&client_control.go)) != 0) {
        handle_error_en(error, "Error: failed to destroy client condition.");
    }

    if ((error = pthread_cond_destroy(&server.server_cond)) != 0) {
        handle_error_en(error, "Error: failed to destroy server condition.");
    }

    sig_handler_destructor(signal_handler);
    pthread_exit(0);
}
