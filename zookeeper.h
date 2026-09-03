#pragma once

#include <stddef.h>

#define RPC_MAX_BODY 4096

/* Framed internal protocol: [uint32 network-order body length][UTF-8 body]. */
int rpc_send(int fd, const char *body);
int rpc_recv(int fd, char *body, size_t capacity);

/* ZooKeeper control plane. No player or room data belongs here. */
int zk_service_start(const char *service_name,
                     const char *bind_host,
                     int listen_port,
                     const char *default_downstreams);
int zk_wait_required_downstreams(int timeout_seconds);
int zk_resolve_service(const char *service_name,
                       char *host,
                       size_t host_capacity,
                       int *port);

/* Resolve every registered instance and connect to the first reachable one. */
int zk_connect_service(const char *service_name);

void zk_service_mark_ready(void);
void zk_service_stop(void);

int tcp_listen(const char *host, int port);
int tcp_connect(const char *host, int port);
