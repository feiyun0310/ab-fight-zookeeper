// dataserver.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <stdarg.h>
#include <fcntl.h>
#include <ctype.h>
#include <hiredis/hiredis.h>
#include "zookeeper.h"

#define MAX_EVENTS 1024
#define MAX_BUFFER 4096
#define DATA_PORT 8889
#define WORKER_THREAD_COUNT 4
#define TIMEOUT_SEC 10
#define CLEANUP_INTERVAL 10

#define REDIS_HOST "127.0.0.1"
#define REDIS_PORT 6379

#define MSG_LOGIN 1
#define MSG_CREATE_ROOM 2
#define MSG_JOIN_ROOM 3
#define MSG_SELECT_ROOM 4
#define MSG_SEND_NUMBER 5
#define MSG_LIST_ROOMS 6
#define MSG_LEAVE_ROOM 7
#define MSG_STATS 8
#define MSG_RANK 9
#define MSG_TITLES 10
#define MSG_REDEEM 11

typedef enum {
    ROOM_WAITING,
    ROOM_READY,
    ROOM_PLAYING,
    ROOM_FINISHED
} RoomState;

typedef struct RoomCache {
    int room_id;
    int player_fds[2];
    char player_names[2][32];
    int numbers[2];
    int has_sent[2];
    time_t sent_times[2];
    time_t ready_time;
    RoomState state;
    int is_active;
    int has_recorded;
    struct RoomCache *next;
} RoomCache;

typedef struct ClientCache {
    int fd;                 // NetServer client id, never a DataServer socket fd
    int route_fd;           // current LogicServer connection used for replies
    char username[32];
    int current_room_id;
    int selecting_room;
    int is_logged_in;
    struct ClientCache *next;
} ClientCache;

typedef struct Task {
    int client_fd;
    int msg_type;
    char data[MAX_BUFFER];
    struct Task *next;
} Task;

typedef struct SendItem {
    int client_fd;
    int route_fd;
    char data[MAX_BUFFER];
    struct SendItem *next;
} SendItem;

static int epoll_fd;
static int server_fd;
static int server_running = 1;
static redisContext *redis_conn = NULL;
static pthread_mutex_t redis_mutex = PTHREAD_MUTEX_INITIALIZER;

static RoomCache *room_cache = NULL;
static ClientCache *client_cache = NULL;
static int next_room_id = 1;
static pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;

static Task *task_queue_head = NULL;
static Task *task_queue_tail = NULL;
static pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t task_cond = PTHREAD_COND_INITIALIZER;

static SendItem *send_queue_head = NULL;
static SendItem *send_queue_tail = NULL;
static pthread_mutex_t send_mutex = PTHREAD_MUTEX_INITIALIZER;

// 函数声明
void enqueue_send(int client_fd, const char *fmt, ...);
void flush_send_queue(void);
void record_match_to_redis(RoomCache *room, const char *winner, int change_a, int change_b);
void resolve_game(RoomCache *room);

// ============ Redis操作 ============

int init_redis(void) {
    struct timeval timeout = {3, 0};
    redis_conn = redisConnectWithTimeout(REDIS_HOST, REDIS_PORT, timeout);
    if (redis_conn == NULL || redis_conn->err) {
        fprintf(stderr, "[DataServer] Redis连接失败: %s\n",
                redis_conn ? redis_conn->errstr : "NULL");
        return -1;
    }
    printf("[DataServer] Redis连接成功 %s:%d\n", REDIS_HOST, REDIS_PORT);
    return 0;
}

redisReply* redis_cmd(const char *format, ...) {
    pthread_mutex_lock(&redis_mutex);
    if (!redis_conn) {
        pthread_mutex_unlock(&redis_mutex);
        return NULL;
    }
    va_list args;
    va_start(args, format);
    redisReply *reply = (redisReply *)redisvCommand(redis_conn, format, args);
    va_end(args);
    pthread_mutex_unlock(&redis_mutex);
    return reply;
}

void init_titles(void) {
    redisReply *reply = redis_cmd("EXISTS title:config");
    if (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        freeReplyObject(reply);
        return;
    }
    if (reply) freeReplyObject(reply);
    
    const char *titles[10][2] = {
        {"初入江湖", "0"},
        {"略有所成", "100"},
        {"小有名气", "300"},
        {"实力不凡", "600"},
        {"百战精英", "1000"},
        {"千胜宗师", "2000"},
        {"纵横天下", "3500"},
        {"一代枭雄", "5000"},
        {"绝世高手", "8000"},
        {"武林至尊", "12000"}
    };
    
    for (int i = 0; i < 10; i++) {
        redis_cmd("HSET title:config %d %s|%s", i + 1, titles[i][0], titles[i][1]);
    }
    printf("[DataServer] 称号配置初始化完成\n");
}

// ============ 缓存管理 ============

void add_room_to_cache(RoomCache *room) {
    pthread_mutex_lock(&cache_mutex);
    room->next = room_cache;
    room_cache = room;
    pthread_mutex_unlock(&cache_mutex);
}

void remove_room_from_cache(int room_id) {
    pthread_mutex_lock(&cache_mutex);
    RoomCache *prev = NULL;
    RoomCache *curr = room_cache;
    while (curr) {
        if (curr->room_id == room_id) {
            if (prev) {
                prev->next = curr->next;
            } else {
                room_cache = curr->next;
            }
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
}

RoomCache* find_room_in_cache(int room_id) {
    pthread_mutex_lock(&cache_mutex);
    RoomCache *curr = room_cache;
    while (curr) {
        if (curr->room_id == room_id && curr->is_active) {
            pthread_mutex_unlock(&cache_mutex);
            return curr;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    return NULL;
}

ClientCache* find_client_in_cache(int fd) {
    pthread_mutex_lock(&cache_mutex);
    ClientCache *curr = client_cache;
    while (curr) {
        if (curr->fd == fd) {
            pthread_mutex_unlock(&cache_mutex);
            return curr;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    return NULL;
}

void add_client_to_cache(ClientCache *client) {
    pthread_mutex_lock(&cache_mutex);
    client->next = client_cache;
    client_cache = client;
    pthread_mutex_unlock(&cache_mutex);
}

void update_client_room(int fd, int room_id) {
    ClientCache *client = find_client_in_cache(fd);
    if (client) {
        client->current_room_id = room_id;
    }
}

// ============ 网络发送 ============

void enqueue_send(int client_fd, const char *fmt, ...) {
    char buffer[MAX_BUFFER];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    
    printf("[DataServer] enqueue_send to fd %d: %s\n", client_fd, buffer);
    
    SendItem *item = (SendItem*)malloc(sizeof(SendItem));
    if (!item) return;
    item->client_fd = client_fd;
    ClientCache *client = find_client_in_cache(client_fd);
    item->route_fd = client ? client->route_fd : -1;
    if (item->route_fd < 0) { free(item); return; }
    strncpy(item->data, buffer, MAX_BUFFER - 1);
    item->data[MAX_BUFFER - 1] = '\0';
    item->next = NULL;
    
    pthread_mutex_lock(&send_mutex);
    if (send_queue_tail) {
        send_queue_tail->next = item;
        send_queue_tail = item;
    } else {
        send_queue_head = send_queue_tail = item;
    }
    pthread_mutex_unlock(&send_mutex);
}

void flush_send_queue(void) {
    pthread_mutex_lock(&send_mutex);
    while (send_queue_head) {
        SendItem *item = send_queue_head;
        send_queue_head = send_queue_head->next;
        if (!send_queue_head) send_queue_tail = NULL;
        pthread_mutex_unlock(&send_mutex);
        
        printf("[DataServer] flush_send_queue to fd %d: %s\n", item->client_fd, item->data);
        char wire[MAX_BUFFER + 32];
        snprintf(wire, sizeof(wire), "CLIENT|%d|%s", item->client_fd, item->data);
        int ret = rpc_send(item->route_fd, wire);
        if (ret < 0) {
            perror("[DataServer] send failed");
        }
        free(item);
        pthread_mutex_lock(&send_mutex);
    }
    pthread_mutex_unlock(&send_mutex);
}

// ============ 游戏核心逻辑 ============

void record_match_to_redis(RoomCache *room, const char *winner, int change_a, int change_b) {
    char key_a[64], key_b[64];
    snprintf(key_a, sizeof(key_a), "player:%s", room->player_names[0]);
    snprintf(key_b, sizeof(key_b), "player:%s", room->player_names[1]);
    
    redisReply *reply_a = redis_cmd("HGET %s score", key_a);
    redisReply *reply_b = redis_cmd("HGET %s score", key_b);
    int score_a = reply_a ? atoi(reply_a->str) : 1000;
    int score_b = reply_b ? atoi(reply_b->str) : 1000;
    if (reply_a) freeReplyObject(reply_a);
    if (reply_b) freeReplyObject(reply_b);
    
    int new_score_a = score_a + change_a;
    int new_score_b = score_b + change_b;
    if (new_score_a < 0) new_score_a = 0;
    if (new_score_b < 0) new_score_b = 0;
    
    redis_cmd("HSET %s score %d", key_a, new_score_a);
    redis_cmd("HSET %s score %d", key_b, new_score_b);
    redis_cmd("ZADD leaderboard %d %s", new_score_a, room->player_names[0]);
    redis_cmd("ZADD leaderboard %d %s", new_score_b, room->player_names[1]);
    
    if (strcmp(winner, "A") == 0) {
        redis_cmd("HINCRBY %s wins 1", key_a);
        redis_cmd("HINCRBY %s losses 1", key_b);
    } else if (strcmp(winner, "B") == 0) {
        redis_cmd("HINCRBY %s losses 1", key_a);
        redis_cmd("HINCRBY %s wins 1", key_b);
    } else {
        redis_cmd("HINCRBY %s draws 1", key_a);
        redis_cmd("HINCRBY %s draws 1", key_b);
    }
    redis_cmd("HINCRBY %s total_games 1", key_a);
    redis_cmd("HINCRBY %s total_games 1", key_b);
}

void resolve_game(RoomCache *room) {
    if (!room || room->has_recorded) return;
    
    int num_a = room->numbers[0];
    int num_b = room->numbers[1];
    char winner[16];
    int change_a, change_b;
    
    if ((num_a % 2) == (num_b % 2)) {
        strcpy(winner, "A");
        change_a = 10;
        change_b = -10;
        enqueue_send(room->player_fds[0], "RESULT|WIN|🎉 你赢了! 你(A):%d vs 对手(B):%d (+10分)", num_a, num_b);
        enqueue_send(room->player_fds[1], "RESULT|LOSE|😞 你输了! 你(B):%d vs 对手(A):%d (-10分)", num_b, num_a);
    } else {
        strcpy(winner, "B");
        change_a = -10;
        change_b = 10;
        enqueue_send(room->player_fds[0], "RESULT|LOSE|😞 你输了! 你(A):%d vs 对手(B):%d (-10分)", num_a, num_b);
        enqueue_send(room->player_fds[1], "RESULT|WIN|🎉 你赢了! 你(B):%d vs 对手(A):%d (+10分)", num_b, num_a);
    }
    
    room->has_recorded = 1;
    room->state = ROOM_FINISHED;
    room->is_active = 0;
    
    record_match_to_redis(room, winner, change_a, change_b);
    
    enqueue_send(room->player_fds[0], "ROOM_DESTROY|💥 房间已解散");
    enqueue_send(room->player_fds[1], "ROOM_DESTROY|💥 房间已解散");
    enqueue_send(room->player_fds[0], ">>> 输入 CREATE_ROOM 开始新游戏");
    enqueue_send(room->player_fds[1], ">>> 输入 CREATE_ROOM 开始新游戏");
    
    update_client_room(room->player_fds[0], 0);
    update_client_room(room->player_fds[1], 0);
    remove_room_from_cache(room->room_id);
}

// ============ 业务逻辑 ============

void handle_login(int client_fd, char *data) {
    char username[32];
    strncpy(username, data, 31);
    username[31] = '\0';
    
    printf("[DataServer] handle_login: username=%s, fd=%d\n", username, client_fd);
    
    char key[64];
    snprintf(key, sizeof(key), "player:%s", username);
    redisReply *reply = redis_cmd("EXISTS %s", key);
    
    int exists = 0;
    if (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        exists = 1;
    }
    if (reply) freeReplyObject(reply);
    
    if (!exists) {
        redis_cmd("HMSET %s username %s score 1000 title_level 1 titles_owned '1' current_title 1 "
                  "total_games 0 wins 0 losses 0 draws 0",
                  key, username);
        redis_cmd("ZADD leaderboard 1000 %s", username);
    }
    
    char title[32] = "初入江湖";
    reply = redis_cmd("HMGET %s current_title", key);
    if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements >= 1) {
        if (reply->element[0]) {
            int current = atoi(reply->element[0]->str);
            redisReply *t_reply = redis_cmd("HGET title:config %d", current);
            if (t_reply && t_reply->type == REDIS_REPLY_STRING) {
                char *p = strchr(t_reply->str, '|');
                if (p) {
                    int len = (p - t_reply->str) > 31 ? 31 : (p - t_reply->str);
                    strncpy(title, t_reply->str, len);
                    title[len] = '\0';
                }
            }
            if (t_reply) freeReplyObject(t_reply);
        }
    }
    if (reply) freeReplyObject(reply);
    
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client) {
        client = (ClientCache*)calloc(1, sizeof(ClientCache));
        if (client) {
            client->fd = client_fd;
            add_client_to_cache(client);
        }
    }
    if (client) {
        strcpy(client->username, username);
        client->is_logged_in = 1;
    }
    
    enqueue_send(client_fd, "LOGIN_RESULT|【%s】%s", title, username);
    enqueue_send(client_fd, ">>> 输入 CREATE_ROOM 创建房间 或 JOIN_ROOM 加入房间 或 LIST_ROOMS 查看房间");
}

void handle_create_room(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    if (client->current_room_id > 0) {
        RoomCache *room = find_room_in_cache(client->current_room_id);
        if (room && room->is_active) {
            enqueue_send(client_fd, "ERROR|你已经在房间 %d 中", room->room_id);
            return;
        }
    }
    
    RoomCache *room = (RoomCache*)calloc(1, sizeof(RoomCache));
    if (!room) {
        enqueue_send(client_fd, "ERROR|创建房间失败");
        return;
    }
    
    room->room_id = next_room_id++;
    room->player_fds[0] = client_fd;
    room->player_fds[1] = -1;
    strcpy(room->player_names[0], client->username);
    room->numbers[0] = -1;
    room->numbers[1] = -1;
    room->has_sent[0] = 0;
    room->has_sent[1] = 0;
    room->state = ROOM_WAITING;
    room->is_active = 1;
    room->has_recorded = 0;
    room->ready_time = time(NULL);
    
    add_room_to_cache(room);
    update_client_room(client_fd, room->room_id);
    
    enqueue_send(client_fd, "ROOM_CREATED|√ 房间 %d 已创建, 你已自动加入 (你是玩家 A)", room->room_id);
    enqueue_send(client_fd, ">>> 等待对手加入...");
    enqueue_send(client_fd, ">>> 输入 LIST_ROOMS 查看所有房间");
    enqueue_send(client_fd, ">>> 输入 LEAVE_ROOM 退出房间");
}

void handle_list_rooms(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    pthread_mutex_lock(&cache_mutex);
    int count = 0;
    char list[512] = "";
    RoomCache *curr = room_cache;
    while (curr) {
        if (curr->is_active && curr->state == ROOM_WAITING) {
            if (count > 0) strcat(list, ", ");
            char buf[64];
            snprintf(buf, sizeof(buf), "%d(%s)", curr->room_id, curr->player_names[0]);
            strcat(list, buf);
            count++;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    
    if (count == 0) {
        enqueue_send(client_fd, ">>> 当前没有可用房间");
    } else {
        enqueue_send(client_fd, ">>> 📋 可用房间: %s", list);
    }
}

void handle_join_room(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    if (client->current_room_id > 0) {
        RoomCache *room = find_room_in_cache(client->current_room_id);
        if (room && room->is_active) {
            enqueue_send(client_fd, "ERROR|你已经在房间 %d 中", room->room_id);
            return;
        }
    }
    
    enqueue_send(client_fd, ">>> 请输入要加入的房间号（直接输入数字）");
    client->selecting_room = 1;
    enqueue_send(client_fd, ">>> 输入 LIST_ROOMS 查看可用房间列表");
}

void handle_select_room(int client_fd, char *data) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (client) client->selecting_room = 0;
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    int room_id = atoi(data);
    if (room_id <= 0) {
        enqueue_send(client_fd, "ERROR|请输入有效的房间号");
        return;
    }
    
    RoomCache *room = find_room_in_cache(room_id);
    if (!room) {
        enqueue_send(client_fd, "ERROR|房间 %d 不存在", room_id);
        return;
    }
    
    if (room->state != ROOM_WAITING) {
        enqueue_send(client_fd, "ERROR|房间 %d 已满或已结束", room_id);
        return;
    }
    
    if (room->player_fds[0] == client_fd) {
        enqueue_send(client_fd, "ERROR|不能加入自己创建的房间");
        return;
    }
    
    room->player_fds[1] = client_fd;
    strcpy(room->player_names[1], client->username);
    room->state = ROOM_READY;
    room->ready_time = time(NULL);
    
    update_client_room(room->player_fds[0], room_id);
    update_client_room(client_fd, room_id);
    
    enqueue_send(room->player_fds[0], "NOTIFY|玩家 %s 已加入房间 (你是玩家 A)", client->username);
    enqueue_send(room->player_fds[0], ">>> 请输入数字 (0-999)");
    enqueue_send(client_fd, "JOIN_SUCCESS|已加入房间 %d (你是玩家 B)", room_id);
    enqueue_send(client_fd, ">>> 请输入数字 (0-999)");
}

void handle_send_number(int client_fd, char *data) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    int number = atoi(data);
    if (number < 0 || number > 999) {
        enqueue_send(client_fd, "ERROR|请输入 0-999 之间的数字");
        return;
    }
    
    if (client->current_room_id <= 0) {
        enqueue_send(client_fd, "ERROR|你不在任何房间中");
        return;
    }
    
    RoomCache *room = find_room_in_cache(client->current_room_id);
    if (!room || !room->is_active) {
        enqueue_send(client_fd, "ERROR|房间不存在");
        client->current_room_id = 0;
        return;
    }
    
    if (room->state == ROOM_FINISHED) {
        enqueue_send(client_fd, "ERROR|游戏已结束");
        return;
    }
    
    int idx = (room->player_fds[0] == client_fd) ? 0 : 1;
    char role = (idx == 0) ? 'A' : 'B';
    
    if (room->has_sent[idx]) {
        enqueue_send(client_fd, "ERROR|你已经出过数字了");
        return;
    }
    
    room->numbers[idx] = number;
    room->has_sent[idx] = 1;
    room->sent_times[idx] = time(NULL);
    
    if (room->state == ROOM_READY) {
        room->state = ROOM_PLAYING;
    }
    
    int other_idx = 1 - idx;
    if (room->player_fds[other_idx] > 0) {
        enqueue_send(room->player_fds[other_idx], "NOTIFY|玩家 %c 已出数字，请出数字 (0-999)", role);
    }
    enqueue_send(client_fd, "SUBMIT_OK|玩家 %c 的数字 %d 已提交，等待对手", role, number);
    
    if (room->has_sent[0] && room->has_sent[1]) {
        resolve_game(room);
    }
}

void handle_leave_room(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    if (client->current_room_id <= 0) {
        enqueue_send(client_fd, "ERROR|你不在任何房间中");
        return;
    }
    
    RoomCache *room = find_room_in_cache(client->current_room_id);
    if (!room) {
        client->current_room_id = 0;
        enqueue_send(client_fd, "ERROR|房间不存在");
        return;
    }
    
    if (room->has_sent[0] || room->has_sent[1]) {
        enqueue_send(client_fd, "ERROR|游戏已开始，不能退出");
        return;
    }
    
    int room_id = room->room_id;
    int other_fd = -1;
    if (room->player_fds[0] == client_fd) {
        other_fd = room->player_fds[1];
    } else {
        other_fd = room->player_fds[0];
    }
    
    remove_room_from_cache(room_id);
    update_client_room(client_fd, 0);
    
    enqueue_send(client_fd, "LEAVE_SUCCESS|✅ 已退出房间 %d，房间已销毁", room_id);
    enqueue_send(client_fd, ">>> 输入 CREATE_ROOM 创建新房间 或 JOIN_ROOM 加入其他房间");
    
    if (other_fd > 0) {
        update_client_room(other_fd, 0);
        enqueue_send(other_fd, "NOTIFY|💥 对手已退出房间 %d，房间已销毁", room_id);
        enqueue_send(other_fd, ">>> 输入 CREATE_ROOM 创建新房间 或 JOIN_ROOM 加入其他房间");
    }
}

void handle_stats(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    char key[64];
    snprintf(key, sizeof(key), "player:%s", client->username);
    redisReply *reply = redis_cmd("HGETALL %s", key);
    
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "STATS|玩家不存在或数据异常");
        return;
    }
    
    int score = 1000, total_games = 0, wins = 0, losses = 0, draws = 0;
    int title_level = 1, current_title = 1;
    
    for (int i = 0; i < (int)reply->elements; i += 2) {
        if (i + 1 >= (int)reply->elements) break;
        const char *field = reply->element[i]->str;
        const char *value = reply->element[i+1]->str;
        if (strcmp(field, "score") == 0) score = atoi(value);
        else if (strcmp(field, "total_games") == 0) total_games = atoi(value);
        else if (strcmp(field, "wins") == 0) wins = atoi(value);
        else if (strcmp(field, "losses") == 0) losses = atoi(value);
        else if (strcmp(field, "draws") == 0) draws = atoi(value);
        else if (strcmp(field, "title_level") == 0) title_level = atoi(value);
        else if (strcmp(field, "current_title") == 0) current_title = atoi(value);
    }
    freeReplyObject(reply);
    
    char title_name[32] = "初入江湖";
    reply = redis_cmd("HGET title:config %d", current_title);
    if (reply && reply->type == REDIS_REPLY_STRING) {
        char *p = strchr(reply->str, '|');
        if (p) {
            int len = (p - reply->str) > 31 ? 31 : (p - reply->str);
            strncpy(title_name, reply->str, len);
            title_name[len] = '\0';
        }
    }
    if (reply) freeReplyObject(reply);
    
    int next_level = title_level + 1;
    int next_cost = 0;
    char next_name[32] = "已满级";
    if (next_level <= 10) {
        reply = redis_cmd("HGET title:config %d", next_level);
        if (reply && reply->type == REDIS_REPLY_STRING) {
            char *p = strchr(reply->str, '|');
            if (p) {
                int len = (p - reply->str) > 31 ? 31 : (p - reply->str);
                strncpy(next_name, reply->str, len);
                next_name[len] = '\0';
                next_cost = atoi(p + 1);
            }
        }
        if (reply) freeReplyObject(reply);
    }
    
    int rank = 0;
    reply = redis_cmd("ZREVRANK leaderboard %s", client->username);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        rank = reply->integer + 1;
    }
    if (reply) freeReplyObject(reply);
    
    // 修复：确保所有数据都是正确的
    int draws_calc = total_games - wins - losses;
    if (draws_calc < 0) draws_calc = 0;
    
    enqueue_send(client_fd, "STATS|%s|%d|%s|%d|%d|%d|%d|%d|%d|%s|%d",
                 title_name, title_level, client->username, score, total_games,
                 wins, losses, draws_calc, rank, next_name, next_cost);
}

void handle_rank(int client_fd) {
    redisReply *reply = redis_cmd("ZREVRANGE leaderboard 0 9 WITHSCORES");
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "RANK|暂无排行数据");
        return;
    }
    
    char buffer[2048] = "";
    int count = 0;
    for (int i = 0; i < (int)reply->elements && count < 10; i += 2) {
        if (i + 1 >= (int)reply->elements) break;
        const char *name = reply->element[i]->str;
        const char *score_str = reply->element[i+1]->str;
        
        // 跳过空数据
        if (!name || !score_str || strlen(name) == 0 || strlen(score_str) == 0) {
            continue;
        }
        
        int score = atoi(score_str);
        
        // 获取称号
        char key[64];
        snprintf(key, sizeof(key), "player:%s", name);
        redisReply *t_reply = redis_cmd("HGET %s current_title", key);
        char title[32] = "初入江湖";
        if (t_reply && t_reply->type == REDIS_REPLY_STRING) {
            int t = atoi(t_reply->str);
            freeReplyObject(t_reply);
            t_reply = redis_cmd("HGET title:config %d", t);
            if (t_reply && t_reply->type == REDIS_REPLY_STRING) {
                char *p = strchr(t_reply->str, '|');
                if (p) {
                    int len = (p - t_reply->str) > 31 ? 31 : (p - t_reply->str);
                    strncpy(title, t_reply->str, len);
                    title[len] = '\0';
                }
            }
        }
        if (t_reply) freeReplyObject(t_reply);
        
        if (count > 0) strcat(buffer, "|");
        char entry[256];
        snprintf(entry, sizeof(entry), "%s|%s|%d", title, name, score);
        strcat(buffer, entry);
        count++;
    }
    freeReplyObject(reply);
    
    if (strlen(buffer) == 0) {
        enqueue_send(client_fd, "RANK|暂无排行数据");
    } else {
        enqueue_send(client_fd, "RANK|%s", buffer);
    }
}

void handle_titles(int client_fd) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    char key[64];
    snprintf(key, sizeof(key), "player:%s", client->username);
    redisReply *reply = redis_cmd("HMGET %s title_level titles_owned current_title", key);
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 3) {
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "TITLES|❌ 玩家不存在");
        return;
    }
    
    int level = reply->element[0] ? atoi(reply->element[0]->str) : 1;
    const char *owned_str = reply->element[1] ? reply->element[1]->str : "1";
    int current = reply->element[2] ? atoi(reply->element[2]->str) : 1;
    freeReplyObject(reply);
    
    char buffer[2048] = "";
    for (int i = 1; i <= 10; i++) {
        reply = redis_cmd("HGET title:config %d", i);
        if (!reply || reply->type != REDIS_REPLY_STRING) {
            if (reply) freeReplyObject(reply);
            continue;
        }
        
        char *p = strchr(reply->str, '|');
        char name[32] = "";
        int cost = 0;
        if (p) {
            int len = (p - reply->str) > 31 ? 31 : (p - reply->str);
            strncpy(name, reply->str, len);
            name[len] = '\0';
            cost = atoi(p + 1);
        }
        freeReplyObject(reply);
        
        char owned_check[16];
        snprintf(owned_check, sizeof(owned_check), "%d", i);
        char status[16];
        if (current == i) {
            strcpy(status, "current");
        } else if (strstr(owned_str, owned_check)) {
            strcpy(status, "owned");
        } else if (i == level + 1) {
            strcpy(status, "next");
        } else {
            strcpy(status, "locked");
        }
        
        if (strlen(buffer) > 0) strcat(buffer, "|");
        char entry[128];
        snprintf(entry, sizeof(entry), "%d|%s|%d|%s", i, name, cost, status);
        strcat(buffer, entry);
    }
    
    enqueue_send(client_fd, "TITLES|%s", buffer);
}

void handle_redeem(int client_fd, char *data) {
    ClientCache *client = find_client_in_cache(client_fd);
    if (!client || !client->is_logged_in) {
        enqueue_send(client_fd, "ERROR|请先登录");
        return;
    }
    
    int target_level = atoi(data);
    if (target_level < 1 || target_level > 10) {
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 无效的称号等级 (1-10)");
        return;
    }
    
    char key[64];
    snprintf(key, sizeof(key), "player:%s", client->username);
    redisReply *reply = redis_cmd("HMGET %s title_level titles_owned score", key);
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 3) {
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 玩家不存在");
        return;
    }
    
    int current_level = reply->element[0] ? atoi(reply->element[0]->str) : 1;
    const char *owned_str = reply->element[1] ? reply->element[1]->str : "1";
    int score = reply->element[2] ? atoi(reply->element[2]->str) : 0;
    freeReplyObject(reply);
    
    char check[16];
    snprintf(check, sizeof(check), "%d", target_level);
    if (strstr(owned_str, check)) {
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 已拥有该称号");
        return;
    }
    
    if (target_level > current_level + 1) {
        int prev_level = target_level - 1;
        reply = redis_cmd("HGET title:config %d", prev_level);
        char prev_name[32] = "";
        if (reply && reply->type == REDIS_REPLY_STRING) {
            char *p = strchr(reply->str, '|');
            if (p) {
                int len = (p - reply->str) > 31 ? 31 : (p - reply->str);
                strncpy(prev_name, reply->str, len);
                prev_name[len] = '\0';
            }
        }
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 请先解锁 %s", prev_name);
        return;
    }
    
    reply = redis_cmd("HGET title:config %d", target_level);
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 称号不存在");
        return;
    }
    
    char *p = strchr(reply->str, '|');
    char title_name[32] = "";
    int cost = 0;
    if (p) {
        int len = (p - reply->str) > 31 ? 31 : (p - reply->str);
        strncpy(title_name, reply->str, len);
        title_name[len] = '\0';
        cost = atoi(p + 1);
    }
    freeReplyObject(reply);
    
    if (score < cost) {
        enqueue_send(client_fd, "REDEEM_RESULT|❌ 积分不足 (需要%d, 你有%d)", cost, score);
        return;
    }
    
    char new_owned[128];
    snprintf(new_owned, sizeof(new_owned), "%s,%d", owned_str, target_level);
    redis_cmd("MULTI");
    redis_cmd("HSET %s score %d", key, score - cost);
    redis_cmd("HSET %s titles_owned %s", key, new_owned);
    if (target_level > current_level) {
        redis_cmd("HSET %s title_level %d", key, target_level);
        redis_cmd("HSET %s current_title %d", key, target_level);
    }
    redis_cmd("ZADD leaderboard %d %s", score - cost, client->username);
    redisReply *exec_reply = redis_cmd("EXEC");
    if (exec_reply) freeReplyObject(exec_reply);
    
    enqueue_send(client_fd, "REDEEM_RESULT|🎉 恭喜!解锁【%s】（消耗%d积分），已自动装备", title_name, cost);
}

// ============ 超时检查 ============

void check_timeout_rooms(void) {
    time_t now = time(NULL);
    pthread_mutex_lock(&cache_mutex);
    RoomCache *curr = room_cache;
    while (curr) {
        if (curr->is_active) {
            if (curr->state == ROOM_READY) {
                if (curr->has_sent[0] || curr->has_sent[1]) {
                    curr->state = ROOM_PLAYING;
                } else if (now - curr->ready_time > TIMEOUT_SEC) {
                    enqueue_send(curr->player_fds[0], "NOTIFY|⏰ 双方都未出数字，房间解散");
                    if (curr->player_fds[1] > 0) {
                        enqueue_send(curr->player_fds[1], "NOTIFY|⏰ 双方都未出数字，房间解散");
                    }
                    enqueue_send(curr->player_fds[0], ">>> 输入 CREATE_ROOM 开始新游戏");
                    if (curr->player_fds[1] > 0) {
                        enqueue_send(curr->player_fds[1], ">>> 输入 CREATE_ROOM 开始新游戏");
                    }
                    update_client_room(curr->player_fds[0], 0);
                    if (curr->player_fds[1] > 0) {
                        update_client_room(curr->player_fds[1], 0);
                    }
                    curr->is_active = 0;
                }
            } else if (curr->state == ROOM_PLAYING) {
                if (curr->has_sent[0] && !curr->has_sent[1]) {
                    if (now - curr->sent_times[0] > TIMEOUT_SEC) {
                        if (!curr->has_recorded) {
                            curr->has_recorded = 1;
                            enqueue_send(curr->player_fds[0], "RESULT|WIN|🎉 对手超时，你赢了！(+5分)");
                            if (curr->player_fds[1] > 0) {
                                enqueue_send(curr->player_fds[1], "RESULT|LOSE|😞 你超时了！(-5分)");
                            }
                            record_match_to_redis(curr, "A", 5, -5);
                            curr->state = ROOM_FINISHED;
                            curr->is_active = 0;
                            enqueue_send(curr->player_fds[0], "ROOM_DESTROY|💥 房间已解散");
                            if (curr->player_fds[1] > 0) {
                                enqueue_send(curr->player_fds[1], "ROOM_DESTROY|💥 房间已解散");
                            }
                            update_client_room(curr->player_fds[0], 0);
                            if (curr->player_fds[1] > 0) {
                                update_client_room(curr->player_fds[1], 0);
                            }
                        }
                    }
                } else if (!curr->has_sent[0] && curr->has_sent[1]) {
                    if (now - curr->sent_times[1] > TIMEOUT_SEC) {
                        if (!curr->has_recorded) {
                            curr->has_recorded = 1;
                            enqueue_send(curr->player_fds[0], "RESULT|LOSE|😞 你超时了！(-5分)");
                            if (curr->player_fds[1] > 0) {
                                enqueue_send(curr->player_fds[1], "RESULT|WIN|🎉 对手超时，你赢了！(+5分)");
                            }
                            record_match_to_redis(curr, "B", -5, 5);
                            curr->state = ROOM_FINISHED;
                            curr->is_active = 0;
                            enqueue_send(curr->player_fds[0], "ROOM_DESTROY|💥 房间已解散");
                            if (curr->player_fds[1] > 0) {
                                enqueue_send(curr->player_fds[1], "ROOM_DESTROY|💥 房间已解散");
                            }
                            update_client_room(curr->player_fds[0], 0);
                            if (curr->player_fds[1] > 0) {
                                update_client_room(curr->player_fds[1], 0);
                            }
                        }
                    }
                }
            }
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&cache_mutex);
}

void cleanup_inactive_rooms(void) {
    pthread_mutex_lock(&cache_mutex);
    RoomCache *prev = NULL;
    RoomCache *curr = room_cache;
    while (curr) {
        if (!curr->is_active) {
            RoomCache *to_delete = curr;
            if (prev) {
                prev->next = curr->next;
            } else {
                room_cache = curr->next;
            }
            curr = curr->next;
            free(to_delete);
        } else {
            prev = curr;
            curr = curr->next;
        }
    }
    pthread_mutex_unlock(&cache_mutex);
}

// ============ 任务处理 ============

void process_task(Task *task) {
    switch (task->msg_type) {
        case MSG_LOGIN:
            handle_login(task->client_fd, task->data);
            break;
        case MSG_CREATE_ROOM:
            handle_create_room(task->client_fd);
            break;
        case MSG_JOIN_ROOM:
            handle_join_room(task->client_fd);
            break;
        case MSG_SELECT_ROOM:
            handle_select_room(task->client_fd, task->data);
            break;
        case MSG_SEND_NUMBER:
            handle_send_number(task->client_fd, task->data);
            break;
        case MSG_LIST_ROOMS:
            handle_list_rooms(task->client_fd);
            break;
        case MSG_LEAVE_ROOM:
            handle_leave_room(task->client_fd);
            break;
        case MSG_STATS:
            handle_stats(task->client_fd);
            break;
        case MSG_RANK:
            handle_rank(task->client_fd);
            break;
        case MSG_TITLES:
            handle_titles(task->client_fd);
            break;
        case MSG_REDEEM:
            handle_redeem(task->client_fd, task->data);
            break;
        default:
            enqueue_send(task->client_fd, "ERROR|Unknown command");
            break;
    }
}

void *worker_thread(void *arg) {
    int tid = *(int*)arg;
    free(arg);
    printf("[DataServer Worker %d] started\n", tid);
    
    while (server_running) {
        Task *task = NULL;
        pthread_mutex_lock(&task_mutex);
        while (task_queue_head == NULL && server_running) {
            pthread_cond_wait(&task_cond, &task_mutex);
        }
        if (!server_running && task_queue_head == NULL) {
            pthread_mutex_unlock(&task_mutex);
            break;
        }
        task = task_queue_head;
        task_queue_head = task_queue_head->next;
        if (!task_queue_head) task_queue_tail = NULL;
        pthread_mutex_unlock(&task_mutex);
        
        if (task) {
            process_task(task);
            free(task);
        }
    }
    printf("[DataServer Worker %d] stopped\n", tid);
    return NULL;
}

// ============ 网络 ============

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void *network_thread(void *arg) {
    struct epoll_event ev, events[MAX_EVENTS];
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len = sizeof(client_addr);
    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { 
        perror("socket"); 
        return NULL; 
    }
    
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DATA_PORT);
    server_addr.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind"); 
        close(server_fd); 
        return NULL;
    }
    
    if (listen(server_fd, 128) < 0) {
        perror("listen"); 
        close(server_fd); 
        return NULL;
    }
    
    set_nonblocking(server_fd);
    
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { 
        perror("epoll_create"); 
        close(server_fd); 
        return NULL; 
    }
    
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
    
    printf("[DataServer] Listening on port %d\n", DATA_PORT);
    zk_service_mark_ready();
    
    time_t last_cleanup = 0;
    
    while (server_running) {
        flush_send_queue();
        check_timeout_rooms();
        
        time_t now = time(NULL);
        if (now - last_cleanup > CLEANUP_INTERVAL) {
            cleanup_inactive_rooms();
            last_cleanup = now;
        }
        
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            
            if (fd == server_fd) {
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
                if (client_fd < 0) continue;
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = client_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                printf("[DataServer] LogicServer %d connected\n", client_fd);
            } else {
                char buffer[MAX_BUFFER];
                int bytes = rpc_recv(fd, buffer, sizeof(buffer));
                if (bytes <= 0) {
                    close(fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
                    printf("[DataServer] LogicServer disconnected\n");
                    continue;
                }
                buffer[bytes] = '\0';
                printf("[DataServer] Received: %s\n", buffer);
                
                Task *task = (Task*)calloc(1, sizeof(Task));
                if (!task) {
                    enqueue_send(fd, "ERROR|内存分配失败");
                    continue;
                }
                /* LogicServer transmits CLIENT|<net-client-id>|<original command>. */
                int client_id = fd;
                if (strncmp(buffer, "CLIENT|", 7) == 0) {
                    char *p = buffer + 7;
                    client_id = atoi(p);
                    p = strchr(p, '|');
                    if (!p) { free(task); continue; }
                    memmove(buffer, p + 1, strlen(p + 1) + 1);
                }
                task->client_fd = client_id;
                ClientCache *route_client = find_client_in_cache(client_id);
                if (!route_client) {
                    route_client = (ClientCache*)calloc(1, sizeof(ClientCache));
                    if (route_client) { route_client->fd = client_id; route_client->route_fd = fd; add_client_to_cache(route_client); }
                } else {
                    route_client->route_fd = fd;
                }
                
                char upper_buffer[MAX_BUFFER];
                strncpy(upper_buffer, buffer, MAX_BUFFER - 1);
                upper_buffer[MAX_BUFFER - 1] = '\0';
                for (int i = 0; upper_buffer[i]; i++) {
                    upper_buffer[i] = toupper(upper_buffer[i]);
                }
                
                if (strncmp(upper_buffer, "LOGIN ", 6) == 0) {
                    task->msg_type = MSG_LOGIN;
                    char *param = buffer + 6;
                    while (*param == ' ') param++;
                    strncpy(task->data, param, MAX_BUFFER - 1);
                    task->data[MAX_BUFFER - 1] = '\0';
                } else if (strncmp(upper_buffer, "CREATE_ROOM", 11) == 0) {
                    task->msg_type = MSG_CREATE_ROOM;
                } else if (strncmp(upper_buffer, "JOIN_ROOM", 9) == 0) {
                    task->msg_type = MSG_JOIN_ROOM;
                } else if (strncmp(upper_buffer, "SELECT_ROOM ", 12) == 0) {
                    task->msg_type = MSG_SELECT_ROOM;
                    char *param = buffer + 12;
                    while (*param == ' ') param++;
                    strncpy(task->data, param, MAX_BUFFER - 1);
                    task->data[MAX_BUFFER - 1] = '\0';
                } else if (strncmp(upper_buffer, "SEND_NUMBER ", 12) == 0) {
                    task->msg_type = MSG_SEND_NUMBER;
                    char *param = buffer + 12;
                    while (*param == ' ') param++;
                    strncpy(task->data, param, MAX_BUFFER - 1);
                    task->data[MAX_BUFFER - 1] = '\0';
                } else if (strncmp(upper_buffer, "NUMBER ", 7) == 0) {
                    char *param = buffer + 7;
                    while (*param == ' ') param++;
                    task->msg_type = route_client && route_client->selecting_room ? MSG_SELECT_ROOM : MSG_SEND_NUMBER;
                    strncpy(task->data, param, MAX_BUFFER - 1);
                    task->data[MAX_BUFFER - 1] = '\0';
                } else if (strncmp(upper_buffer, "LIST_ROOMS", 10) == 0) {
                    task->msg_type = MSG_LIST_ROOMS;
                } else if (strncmp(upper_buffer, "LEAVE_ROOM", 10) == 0) {
                    task->msg_type = MSG_LEAVE_ROOM;
                } else if (strncmp(upper_buffer, "STATS", 5) == 0) {
                    task->msg_type = MSG_STATS;
                } else if (strncmp(upper_buffer, "RANK", 4) == 0) {
                    task->msg_type = MSG_RANK;
                } else if (strncmp(upper_buffer, "TITLES", 6) == 0) {
                    task->msg_type = MSG_TITLES;
                } else if (strncmp(upper_buffer, "REDEEM ", 7) == 0) {
                    task->msg_type = MSG_REDEEM;
                    char *param = buffer + 7;
                    while (*param == ' ') param++;
                    strncpy(task->data, param, MAX_BUFFER - 1);
                    task->data[MAX_BUFFER - 1] = '\0';
                } else {
                    free(task);
                    enqueue_send(fd, "ERROR|Unknown command");
                    continue;
                }
                
                pthread_mutex_lock(&task_mutex);
                if (task_queue_tail) {
                    task_queue_tail->next = task;
                    task_queue_tail = task;
                } else {
                    task_queue_head = task_queue_tail = task;
                }
                pthread_cond_signal(&task_cond);
                pthread_mutex_unlock(&task_mutex);
            }
        }
    }
    
    close(server_fd);
    close(epoll_fd);
    return NULL;
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    
    if (init_redis() < 0) {
        return -1;
    }
    
    init_titles();
    if (zk_service_start("dataserver", "127.0.0.1", DATA_PORT, "") != 0 ||
        zk_wait_required_downstreams(10) != 0) {
        fprintf(stderr, "[DataServer] ZooKeeper 初始化失败\n");
        return -1;
    }
    
    pthread_t net_thread;
    pthread_t worker_threads[WORKER_THREAD_COUNT];
    
    printf("=== Data Server ===\n");
    printf("Port: %d\n", DATA_PORT);
    
    pthread_create(&net_thread, NULL, network_thread, NULL);
    
    for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
        int *tid = (int*)malloc(sizeof(int));
        if (tid) {
            *tid = i + 1;
            pthread_create(&worker_threads[i], NULL, worker_thread, tid);
        }
    }
    
    pthread_join(net_thread, NULL);
    
    server_running = 0;
    for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
        pthread_join(worker_threads[i], NULL);
    }
    
    return 0;
}
