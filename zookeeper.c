#define _POSIX_C_SOURCE 200809L
#include "zookeeper.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>

#define MAX_SERVICE_INSTANCES 128

typedef struct {
    char host[64];
    int port;
} ServiceEndpoint;

static zhandle_t *g_zh;
static pthread_mutex_t g_state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_zk_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_connected;
static int g_session_expired;
static int g_ready;
static int g_stopping;
static int g_register_started;
static char g_service[64];
static char g_host[64];
static int g_port;
static char g_downstreams[256];
static char g_instance_path[512];
static unsigned long g_resolve_cursor;

static int state_is_stopping(void) {
    int stopping;
    pthread_mutex_lock(&g_state_lock);
    stopping = g_stopping;
    pthread_mutex_unlock(&g_state_lock);
    return stopping;
}

static int state_is_connected(void) {
    int connected;
    pthread_mutex_lock(&g_state_lock);
    connected = g_connected;
    pthread_mutex_unlock(&g_state_lock);
    return connected;
}

static int send_all(int fd, const void *data, size_t size) {
    const char *cursor = data;

    while (size > 0) {
        ssize_t written = send(fd, cursor, size, MSG_NOSIGNAL);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return -1;
        }
        cursor += written;
        size -= (size_t)written;
    }
    return 0;
}

static int recv_all(int fd, void *data, size_t size) {
    char *cursor = data;

    while (size > 0) {
        ssize_t received = recv(fd, cursor, size, 0);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received <= 0) {
            return -1;
        }
        cursor += received;
        size -= (size_t)received;
    }
    return 0;
}

int rpc_send(int fd, const char *body) {
    size_t size = strlen(body);
    if (size == 0 || size > RPC_MAX_BODY) {
        return -1;
    }

    uint32_t network_size = htonl((uint32_t)size);
    return send_all(fd, &network_size, sizeof(network_size)) != 0 ||
           send_all(fd, body, size) != 0
         ? -1
         : 0;
}

int rpc_recv(int fd, char *body, size_t capacity) {
    uint32_t network_size;
    if (recv_all(fd, &network_size, sizeof(network_size)) != 0) {
        return -1;
    }

    size_t size = ntohl(network_size);
    if (size == 0 || size >= capacity || size > RPC_MAX_BODY) {
        return -1;
    }
    if (recv_all(fd, body, size) != 0) {
        return -1;
    }

    body[size] = '\0';
    return (int)size;
}

int tcp_listen(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    struct sockaddr_in address = {0};

    if (fd < 0) {
        perror("socket");
        return -1;
    }

    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);

    if (inet_pton(AF_INET, host, &address.sin_addr) != 1) {
        fprintf(stderr, "invalid listen address: %s\n", host);
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 128) != 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in address = {0};

    if (fd < 0) {
        return -1;
    }

    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &address.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void watcher(zhandle_t *zh,
                    int type,
                    int state,
                    const char *path,
                    void *context) {
    (void)zh;
    (void)path;
    (void)context;

    if (type != ZOO_SESSION_EVENT) {
        return;
    }

    pthread_mutex_lock(&g_state_lock);
    g_connected = state == ZOO_CONNECTED_STATE;
    if (state == ZOO_EXPIRED_SESSION_STATE) {
        g_session_expired = 1;
        g_instance_path[0] = '\0';
    }
    pthread_mutex_unlock(&g_state_lock);
}

/* g_zk_lock must be held by the caller. */
static void ensure_path_locked(const char *path) {
    char created[512];
    if (!g_zh) {
        return;
    }

    int result = zoo_create(g_zh,
                            path,
                            "",
                            0,
                            &ZOO_OPEN_ACL_UNSAFE,
                            0,
                            created,
                            sizeof(created));
    if (result != ZOK && result != ZNODEEXISTS) {
        fprintf(stderr, "[ZooKeeper] cannot create %s: %s\n",
                path,
                zerror(result));
    }
}

static void ensure_tree(void) {
    char path[256];
    char created[512];

    pthread_mutex_lock(&g_zk_lock);
    if (!g_zh) {
        pthread_mutex_unlock(&g_zk_lock);
        return;
    }

    ensure_path_locked("/ab-game");
    ensure_path_locked("/ab-game/config");
    ensure_path_locked("/ab-game/registry");

    snprintf(path, sizeof(path), "/ab-game/config/%s", g_service);
    ensure_path_locked(path);

    snprintf(path,
             sizeof(path),
             "/ab-game/config/%s/downstreams",
             g_service);
    int result = zoo_create(g_zh,
                            path,
                            g_downstreams,
                            (int)strlen(g_downstreams),
                            &ZOO_OPEN_ACL_UNSAFE,
                            0,
                            created,
                            sizeof(created));
    if (result != ZOK && result != ZNODEEXISTS) {
        fprintf(stderr, "[ZooKeeper] cannot create %s: %s\n",
                path,
                zerror(result));
    }

    snprintf(path, sizeof(path), "/ab-game/registry/%s", g_service);
    ensure_path_locked(path);
    pthread_mutex_unlock(&g_zk_lock);
}

static void refresh_config(void) {
    char path[256];
    char value[256] = {0};
    int length = (int)sizeof(value) - 1;
    int result = ZINVALIDSTATE;

    snprintf(path,
             sizeof(path),
             "/ab-game/config/%s/downstreams",
             g_service);

    pthread_mutex_lock(&g_zk_lock);
    if (g_zh) {
        result = zoo_get(g_zh, path, 1, value, &length, NULL);
    }
    pthread_mutex_unlock(&g_zk_lock);

    if (result == ZOK) {
        value[length] = '\0';
        pthread_mutex_lock(&g_state_lock);
        snprintf(g_downstreams, sizeof(g_downstreams), "%s", value);
        pthread_mutex_unlock(&g_state_lock);
    }
}

static int has_instance(const char *service) {
    char path[256];
    struct String_vector children = {0};
    int result = ZINVALIDSTATE;

    snprintf(path, sizeof(path), "/ab-game/registry/%s", service);

    pthread_mutex_lock(&g_zk_lock);
    if (g_zh) {
        result = zoo_get_children(g_zh, path, 1, &children);
    }
    pthread_mutex_unlock(&g_zk_lock);

    int available = result == ZOK && children.count > 0;
    if (result == ZOK) {
        deallocate_String_vector(&children);
    }
    return available;
}

static void *zk_connection_thread(void *unused) {
    (void)unused;

    while (!state_is_stopping()) {
        int expired;

        pthread_mutex_lock(&g_state_lock);
        expired = g_session_expired;
        pthread_mutex_unlock(&g_state_lock);

        if (expired) {
            pthread_mutex_lock(&g_zk_lock);
            if (g_zh) {
                zookeeper_close(g_zh);
                g_zh = NULL;
            }
            pthread_mutex_unlock(&g_zk_lock);

            pthread_mutex_lock(&g_state_lock);
            g_connected = 0;
            g_session_expired = 0;
            g_instance_path[0] = '\0';
            pthread_mutex_unlock(&g_state_lock);
        }

        pthread_mutex_lock(&g_zk_lock);
        if (!g_zh) {
            g_zh = zookeeper_init("127.0.0.1:2181",
                                  watcher,
                                  10000,
                                  NULL,
                                  NULL,
                                  0);
        }
        pthread_mutex_unlock(&g_zk_lock);

        if (state_is_connected()) {
            ensure_tree();
        }
        sleep(1);
    }
    return NULL;
}

static void *zk_downstream_manager_thread(void *unused) {
    (void)unused;

    while (!state_is_stopping()) {
        if (state_is_connected()) {
            refresh_config();
        }
        sleep(1);
    }
    return NULL;
}

/* g_zk_lock must be held by the caller. */
static int find_existing_registration_locked(const char *expected_value,
                                             char *found_path,
                                             size_t found_capacity) {
    char parent[256];
    struct String_vector children = {0};

    snprintf(parent, sizeof(parent), "/ab-game/registry/%s", g_service);
    if (!g_zh || zoo_get_children(g_zh, parent, 0, &children) != ZOK) {
        return 0;
    }

    int found = 0;
    for (int index = 0; index < children.count && !found; ++index) {
        char child[512];
        char value[128] = {0};
        int length = (int)sizeof(value) - 1;

        snprintf(child,
                 sizeof(child),
                 "%s/%s",
                 parent,
                 children.data[index]);
        if (zoo_get(g_zh, child, 0, value, &length, NULL) != ZOK) {
            continue;
        }
        value[length] = '\0';
        if (strcmp(value, expected_value) == 0) {
            snprintf(found_path, found_capacity, "%s", child);
            found = 1;
        }
    }

    deallocate_String_vector(&children);
    return found;
}

static void register_instance_once(void) {
    char saved_path[512];
    char value[128];
    int path_exists = 0;

    pthread_mutex_lock(&g_state_lock);
    snprintf(saved_path, sizeof(saved_path), "%s", g_instance_path);
    pthread_mutex_unlock(&g_state_lock);

    snprintf(value, sizeof(value), "%s:%d", g_host, g_port);

    pthread_mutex_lock(&g_zk_lock);
    if (!g_zh) {
        pthread_mutex_unlock(&g_zk_lock);
        return;
    }

    if (saved_path[0] != '\0' &&
        zoo_exists(g_zh, saved_path, 0, NULL) == ZOK) {
        path_exists = 1;
    }

    if (!path_exists) {
        char found_path[512] = {0};
        if (find_existing_registration_locked(value,
                                              found_path,
                                              sizeof(found_path))) {
            pthread_mutex_lock(&g_state_lock);
            snprintf(g_instance_path,
                     sizeof(g_instance_path),
                     "%s",
                     found_path);
            pthread_mutex_unlock(&g_state_lock);
            path_exists = 1;
        }
    }

    if (!path_exists) {
        char prefix[256];
        char created[512] = {0};
        snprintf(prefix,
                 sizeof(prefix),
                 "/ab-game/registry/%s/instance-",
                 g_service);

        int result = zoo_create(g_zh,
                                prefix,
                                value,
                                (int)strlen(value),
                                &ZOO_OPEN_ACL_UNSAFE,
                                ZOO_EPHEMERAL | ZOO_SEQUENCE,
                                created,
                                sizeof(created));
        if (result == ZOK) {
            pthread_mutex_lock(&g_state_lock);
            snprintf(g_instance_path,
                     sizeof(g_instance_path),
                     "%s",
                     created);
            pthread_mutex_unlock(&g_state_lock);
            printf("[ZooKeeper] Registered %s at %s\n", g_service, value);
        } else if (result != ZCONNECTIONLOSS && result != ZOPERATIONTIMEOUT) {
            fprintf(stderr,
                    "[ZooKeeper] cannot register %s: %s\n",
                    g_service,
                    zerror(result));
        }
    }

    pthread_mutex_unlock(&g_zk_lock);
}

static void *zk_register_thread(void *unused) {
    (void)unused;

    while (!state_is_stopping()) {
        int ready;
        int connected;

        pthread_mutex_lock(&g_state_lock);
        ready = g_ready;
        connected = g_connected;
        pthread_mutex_unlock(&g_state_lock);

        if (ready && connected) {
            register_instance_once();
        }
        sleep(1);
    }
    return NULL;
}

int zk_service_start(const char *service_name,
                     const char *bind_host,
                     int listen_port,
                     const char *default_downstreams) {
    snprintf(g_service, sizeof(g_service), "%s", service_name);
    snprintf(g_host, sizeof(g_host), "%s", bind_host);
    snprintf(g_downstreams,
             sizeof(g_downstreams),
             "%s",
             default_downstreams ? default_downstreams : "");
    g_port = listen_port;

    pthread_t connection_thread;
    pthread_t downstream_thread;
    if (pthread_create(&connection_thread,
                       NULL,
                       zk_connection_thread,
                       NULL) != 0) {
        return -1;
    }
    pthread_detach(connection_thread);

    if (pthread_create(&downstream_thread,
                       NULL,
                       zk_downstream_manager_thread,
                       NULL) != 0) {
        return -1;
    }
    pthread_detach(downstream_thread);
    return 0;
}

int zk_wait_required_downstreams(int seconds) {
    for (int elapsed = 0; elapsed < seconds; ++elapsed) {
        char copy[256];
        char *save = NULL;
        int ready = state_is_connected();

        pthread_mutex_lock(&g_state_lock);
        snprintf(copy, sizeof(copy), "%s", g_downstreams);
        pthread_mutex_unlock(&g_state_lock);

        for (char *name = strtok_r(copy, ",", &save);
             ready && name;
             name = strtok_r(NULL, ",", &save)) {
            ready = has_instance(name);
        }

        if (ready) {
            return 0;
        }
        sleep(1);
    }
    return -1;
}

static int collect_service_endpoints(const char *service,
                                     ServiceEndpoint *endpoints,
                                     int capacity) {
    char parent[256];
    struct String_vector children = {0};
    int count = 0;

    snprintf(parent, sizeof(parent), "/ab-game/registry/%s", service);

    pthread_mutex_lock(&g_zk_lock);
    if (!g_zh ||
        zoo_get_children(g_zh, parent, 1, &children) != ZOK ||
        children.count == 0) {
        pthread_mutex_unlock(&g_zk_lock);
        if (children.data) {
            deallocate_String_vector(&children);
        }
        return 0;
    }

    for (int index = 0;
         index < children.count && count < capacity;
         ++index) {
        char child[512];
        char value[128] = {0};
        int length = (int)sizeof(value) - 1;

        snprintf(child,
                 sizeof(child),
                 "%s/%s",
                 parent,
                 children.data[index]);
        if (zoo_get(g_zh, child, 0, value, &length, NULL) != ZOK) {
            continue;
        }
        value[length] = '\0';

        char *colon = strrchr(value, ':');
        if (!colon) {
            continue;
        }
        *colon = '\0';

        char *end = NULL;
        long port = strtol(colon + 1, &end, 10);
        if (!end || *end != '\0' || port <= 0 || port > 65535 ||
            value[0] == '\0') {
            continue;
        }

        snprintf(endpoints[count].host,
                 sizeof(endpoints[count].host),
                 "%s",
                 value);
        endpoints[count].port = (int)port;
        count++;
    }

    deallocate_String_vector(&children);
    pthread_mutex_unlock(&g_zk_lock);
    return count;
}

int zk_resolve_service(const char *service,
                       char *host,
                       size_t host_capacity,
                       int *port) {
    ServiceEndpoint endpoints[MAX_SERVICE_INSTANCES];
    int count = collect_service_endpoints(service,
                                          endpoints,
                                          MAX_SERVICE_INSTANCES);
    if (count <= 0) {
        return -1;
    }

    unsigned long cursor = __sync_fetch_and_add(&g_resolve_cursor, 1);
    int selected = (int)(cursor % (unsigned long)count);
    snprintf(host, host_capacity, "%s", endpoints[selected].host);
    *port = endpoints[selected].port;
    return 0;
}

int zk_connect_service(const char *service) {
    ServiceEndpoint endpoints[MAX_SERVICE_INSTANCES];
    int count = collect_service_endpoints(service,
                                          endpoints,
                                          MAX_SERVICE_INSTANCES);
    if (count <= 0) {
        return -1;
    }

    unsigned long cursor = __sync_fetch_and_add(&g_resolve_cursor, 1);
    int start = (int)(cursor % (unsigned long)count);

    for (int offset = 0; offset < count; ++offset) {
        int index = (start + offset) % count;
        int fd = tcp_connect(endpoints[index].host, endpoints[index].port);
        if (fd >= 0) {
            return fd;
        }
    }
    return -1;
}

void zk_service_mark_ready(void) {
    int start_thread = 0;

    pthread_mutex_lock(&g_state_lock);
    g_ready = 1;
    if (!g_register_started) {
        g_register_started = 1;
        start_thread = 1;
    }
    pthread_mutex_unlock(&g_state_lock);

    if (start_thread) {
        pthread_t thread;
        if (pthread_create(&thread, NULL, zk_register_thread, NULL) == 0) {
            pthread_detach(thread);
        }
    }
}

void zk_service_stop(void) {
    pthread_mutex_lock(&g_state_lock);
    g_stopping = 1;
    g_connected = 0;
    pthread_mutex_unlock(&g_state_lock);

    pthread_mutex_lock(&g_zk_lock);
    if (g_zh) {
        zookeeper_close(g_zh);
        g_zh = NULL;
    }
    pthread_mutex_unlock(&g_zk_lock);
}
