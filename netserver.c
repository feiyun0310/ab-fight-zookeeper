#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zookeeper.h"

#define NET_PORT 8888
#define MAX_PACKET 4097

typedef struct {
    const char *name;
    int fd;
    unsigned long generation;
    pthread_mutex_t lock;
    pthread_mutex_t connect_lock;
} Upstream;

typedef struct Client {
    int id;
    int fd;
    pthread_mutex_t write_lock;
    struct Client *next;
} Client;

static Upstream login_up = {
    .name = "loginserver",
    .fd = -1,
    .generation = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .connect_lock = PTHREAD_MUTEX_INITIALIZER
};

static Upstream game_up = {
    .name = "gameserver",
    .fd = -1,
    .generation = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .connect_lock = PTHREAD_MUTEX_INITIALIZER
};

static Client *clients;
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;
static int next_client_id = 100000;

static void add_client(Client *client) {
    pthread_mutex_lock(&clients_lock);
    client->next = clients;
    clients = client;
    pthread_mutex_unlock(&clients_lock);
}

static void remove_and_close_client(Client *client) {
    pthread_mutex_lock(&clients_lock);

    Client **cursor = &clients;
    while (*cursor && *cursor != client) {
        cursor = &(*cursor)->next;
    }
    if (*cursor == client) {
        *cursor = client->next;
    }

    pthread_mutex_lock(&client->write_lock);
    if (client->fd >= 0) {
        shutdown(client->fd, SHUT_RDWR);
        close(client->fd);
        client->fd = -1;
    }
    pthread_mutex_unlock(&client->write_lock);

    pthread_mutex_unlock(&clients_lock);
}

static int send_to_client(int client_id, const char *message) {
    int result = -1;

    pthread_mutex_lock(&clients_lock);
    for (Client *client = clients; client; client = client->next) {
        if (client->id != client_id) {
            continue;
        }

        pthread_mutex_lock(&client->write_lock);
        if (client->fd >= 0) {
            result = rpc_send(client->fd, message);
        }
        pthread_mutex_unlock(&client->write_lock);
        break;
    }
    pthread_mutex_unlock(&clients_lock);

    return result;
}

static int connect_upstream(Upstream *upstream) {
    return zk_connect_service(upstream->name);
}

static int ensure_upstream_connected(Upstream *upstream) {
    int current_fd;

    pthread_mutex_lock(&upstream->lock);
    current_fd = upstream->fd;
    pthread_mutex_unlock(&upstream->lock);
    if (current_fd >= 0) {
        return 0;
    }

    pthread_mutex_lock(&upstream->connect_lock);

    pthread_mutex_lock(&upstream->lock);
    current_fd = upstream->fd;
    pthread_mutex_unlock(&upstream->lock);

    if (current_fd < 0) {
        int new_fd = connect_upstream(upstream);
        if (new_fd >= 0) {
            pthread_mutex_lock(&upstream->lock);
            if (upstream->fd < 0) {
                upstream->fd = new_fd;
                upstream->generation++;
                new_fd = -1;
            }
            pthread_mutex_unlock(&upstream->lock);

            if (new_fd >= 0) {
                close(new_fd);
            }
        }
    }

    pthread_mutex_lock(&upstream->lock);
    current_fd = upstream->fd;
    pthread_mutex_unlock(&upstream->lock);

    pthread_mutex_unlock(&upstream->connect_lock);
    return current_fd >= 0 ? 0 : -1;
}

static void invalidate_upstream(Upstream *upstream,
                                int expected_fd,
                                unsigned long expected_generation) {
    pthread_mutex_lock(&upstream->lock);
    if (upstream->fd == expected_fd &&
        upstream->generation == expected_generation) {
        shutdown(expected_fd, SHUT_RDWR);
        close(expected_fd);
        upstream->fd = -1;
        upstream->generation++;
    }
    pthread_mutex_unlock(&upstream->lock);
}

static int snapshot_upstream(Upstream *upstream,
                             int *fd,
                             unsigned long *generation) {
    pthread_mutex_lock(&upstream->lock);
    *fd = upstream->fd;
    *generation = upstream->generation;
    pthread_mutex_unlock(&upstream->lock);
    return *fd >= 0 ? 0 : -1;
}

static void *upstream_reader(void *arg) {
    Upstream *upstream = arg;
    char packet[MAX_PACKET];

    for (;;) {
        int fd;
        unsigned long generation;

        if (ensure_upstream_connected(upstream) != 0) {
            sleep(1);
            continue;
        }
        if (snapshot_upstream(upstream, &fd, &generation) != 0) {
            continue;
        }

        if (rpc_recv(fd, packet, sizeof(packet)) < 0) {
            invalidate_upstream(upstream, fd, generation);
            continue;
        }

        if (strncmp(packet, "CLIENT|", 7) != 0) {
            continue;
        }

        char *id_text = packet + 7;
        char *separator = strchr(id_text, '|');
        if (!separator) {
            continue;
        }

        *separator = '\0';
        char *end = NULL;
        long client_id = strtol(id_text, &end, 10);
        if (!end || *end != '\0' || client_id <= 0 || client_id > INT32_MAX) {
            continue;
        }

        send_to_client((int)client_id, separator + 1);
    }

    return NULL;
}

static int send_to_logic_once(Upstream *upstream, const char *message) {
    int fd;
    unsigned long generation;
    int result;

    if (ensure_upstream_connected(upstream) != 0) {
        return -1;
    }

    pthread_mutex_lock(&upstream->lock);
    fd = upstream->fd;
    generation = upstream->generation;
    result = fd >= 0 ? rpc_send(fd, message) : -1;
    pthread_mutex_unlock(&upstream->lock);

    if (result != 0 && fd >= 0) {
        invalidate_upstream(upstream, fd, generation);
    }
    return result;
}

static int send_to_logic(Upstream *upstream, const char *message) {
    if (send_to_logic_once(upstream, message) == 0) {
        return 0;
    }

    /* Re-resolve ZooKeeper and retry once after invalidating a dead socket. */
    return send_to_logic_once(upstream, message);
}

static void *client_thread(void *arg) {
    Client *client = arg;
    char buffer[MAX_PACKET];

    for (;;) {
        int size = rpc_recv(client->fd, buffer, sizeof(buffer));
        if (size <= 0) {
            break;
        }

        Upstream *upstream = !strncasecmp(buffer, "LOGIN", 5)
                           ? &login_up
                           : &game_up;
        char packet[MAX_PACKET + 32];
        int written = snprintf(packet,
                               sizeof(packet),
                               "CLIENT|%d|%s",
                               client->id,
                               buffer);
        if (written < 0 || (size_t)written >= sizeof(packet) ||
            send_to_logic(upstream, packet) != 0) {
            pthread_mutex_lock(&client->write_lock);
            if (client->fd >= 0) {
                rpc_send(client->fd, "ERROR|目标服务不可用");
            }
            pthread_mutex_unlock(&client->write_lock);
        }
    }

    char leave[64];
    snprintf(leave, sizeof(leave), "CLIENT_DISCONNECT|%d", client->id);
    send_to_logic(&game_up, leave);

    remove_and_close_client(client);
    pthread_mutex_destroy(&client->write_lock);
    free(client);
    return NULL;
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);

    if (zk_service_start("netserver",
                         "127.0.0.1",
                         NET_PORT,
                         "loginserver,gameserver") != 0 ||
        zk_wait_required_downstreams(30) != 0) {
        fprintf(stderr, "[NetServer] required services are unavailable\n");
        return 1;
    }

    int listener = tcp_listen("127.0.0.1", NET_PORT);
    if (listener < 0) {
        return 1;
    }

    zk_service_mark_ready();

    pthread_t login_reader;
    pthread_t game_reader;
    if (pthread_create(&login_reader, NULL, upstream_reader, &login_up) != 0 ||
        pthread_create(&game_reader, NULL, upstream_reader, &game_up) != 0) {
        fprintf(stderr, "[NetServer] failed to create upstream threads\n");
        close(listener);
        return 1;
    }
    pthread_detach(login_reader);
    pthread_detach(game_reader);

    printf("[NetServer] Listening on port %d\n", NET_PORT);

    for (;;) {
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            continue;
        }

        Client *client = calloc(1, sizeof(*client));
        if (!client) {
            close(fd);
            continue;
        }

        client->id = __sync_add_and_fetch(&next_client_id, 1);
        client->fd = fd;
        pthread_mutex_init(&client->write_lock, NULL);
        add_client(client);

        pthread_t thread;
        if (pthread_create(&thread, NULL, client_thread, client) != 0) {
            remove_and_close_client(client);
            pthread_mutex_destroy(&client->write_lock);
            free(client);
            continue;
        }
        pthread_detach(thread);
    }
}
