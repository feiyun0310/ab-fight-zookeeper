#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zookeeper.h"

#define LOGIN_PORT 8890
#define MAX_PACKET 4097

static int net_fd = -1;
static int data_fd = -1;
static pthread_mutex_t net_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t data_lock = PTHREAD_MUTEX_INITIALIZER;

static int parse_listen_port(int argc, char *argv[]) {
    if (argc < 2) {
        return LOGIN_PORT;
    }

    char *end = NULL;
    long value = strtol(argv[1], &end, 10);
    if (!end || *end != '\0' || value < 0) {
        return -1;
    }

    /* 0/1/... are instance indexes; a value above 1000 is an explicit port. */
    if (value <= 1000) {
        value = LOGIN_PORT + value * 2;
    }
    return value > 0 && value <= 65535 ? (int)value : -1;
}

static int connect_data_server(void) {
    return zk_connect_service("dataserver");
}

static int connect_initial_data_server(int attempts) {
    for (int attempt = 0; attempt < attempts; ++attempt) {
        int fd = connect_data_server();
        if (fd >= 0) {
            pthread_mutex_lock(&data_lock);
            data_fd = fd;
            pthread_mutex_unlock(&data_lock);
            return 0;
        }
        sleep(1);
    }
    return -1;
}

static void *data_receiver(void *unused) {
    char packet[MAX_PACKET];
    (void)unused;

    for (;;) {
        int fd;

        pthread_mutex_lock(&data_lock);
        fd = data_fd;
        pthread_mutex_unlock(&data_lock);

        if (fd < 0) {
            int new_fd = connect_data_server();
            if (new_fd < 0) {
                sleep(1);
                continue;
            }

            pthread_mutex_lock(&data_lock);
            if (data_fd < 0) {
                data_fd = new_fd;
                fd = new_fd;
                new_fd = -1;
            } else {
                fd = data_fd;
            }
            pthread_mutex_unlock(&data_lock);

            if (new_fd >= 0) {
                close(new_fd);
            }
        }

        if (rpc_recv(fd, packet, sizeof(packet)) < 0) {
            pthread_mutex_lock(&data_lock);
            if (data_fd == fd) {
                shutdown(fd, SHUT_RDWR);
                close(fd);
                data_fd = -1;
            }
            pthread_mutex_unlock(&data_lock);
            continue;
        }

        pthread_mutex_lock(&net_lock);
        if (net_fd >= 0) {
            rpc_send(net_fd, packet);
        }
        pthread_mutex_unlock(&net_lock);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    int listen_port = parse_listen_port(argc, argv);
    if (listen_port < 0) {
        fprintf(stderr, "Usage: %s [instance-index|listen-port]\n", argv[0]);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    if (zk_service_start("loginserver",
                         "127.0.0.1",
                         listen_port,
                         "dataserver") != 0 ||
        zk_wait_required_downstreams(30) != 0) {
        fprintf(stderr, "[LoginServer] DataServer is unavailable\n");
        return 1;
    }

    int listener = tcp_listen("127.0.0.1", listen_port);
    if (listener < 0) {
        return 1;
    }

    if (connect_initial_data_server(10) != 0) {
        fprintf(stderr, "[LoginServer] cannot connect to DataServer\n");
        close(listener);
        return 1;
    }

    pthread_t receiver;
    if (pthread_create(&receiver, NULL, data_receiver, NULL) != 0) {
        fprintf(stderr, "[LoginServer] cannot create DataServer receiver\n");
        close(listener);
        return 1;
    }
    pthread_detach(receiver);

    zk_service_mark_ready();
    printf("[LoginServer] Listening on port %d\n", listen_port);

    for (;;) {
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            continue;
        }

        pthread_mutex_lock(&net_lock);
        net_fd = fd;
        pthread_mutex_unlock(&net_lock);

        char packet[MAX_PACKET];
        while (rpc_recv(fd, packet, sizeof(packet)) > 0) {
            char *command = strncmp(packet, "CLIENT|", 7) == 0
                          ? strchr(packet + 7, '|')
                          : NULL;
            if (!command || strncasecmp(command + 1, "LOGIN", 5) != 0) {
                continue;
            }

            pthread_mutex_lock(&data_lock);
            if (data_fd >= 0) {
                rpc_send(data_fd, packet);
            }
            pthread_mutex_unlock(&data_lock);
        }

        pthread_mutex_lock(&net_lock);
        if (net_fd == fd) {
            shutdown(fd, SHUT_RDWR);
            close(fd);
            net_fd = -1;
        } else {
            close(fd);
        }
        pthread_mutex_unlock(&net_lock);
    }
}
