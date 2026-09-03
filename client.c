#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdint.h>
#include <termios.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8888
#define BUFFER_SIZE 4096

static int sock_fd = -1;
static int running = 1;

static int send_all_client(int fd, const void *data, size_t len) {
    const char *p = data;
    while (len > 0) { ssize_t n = send(fd, p, len, 0); if (n <= 0) return -1; p += n; len -= (size_t)n; }
    return 0;
}
static int recv_all_client(int fd, void *data, size_t len) {
    char *p = data;
    while (len > 0) { ssize_t n = recv(fd, p, len, 0); if (n <= 0) return -1; p += n; len -= (size_t)n; }
    return 0;
}
static int send_frame(int fd, const char *message) {
    uint32_t len = (uint32_t)strlen(message), net = htonl(len);
    return send_all_client(fd, &net, sizeof(net)) || send_all_client(fd, message, len) ? -1 : 0;
}
static int recv_frame(int fd, char *buffer, size_t capacity) {
    uint32_t net, len;
    if (recv_all_client(fd, &net, sizeof(net))) return -1;
    len = ntohl(net);
    if (len == 0 || len >= capacity) return -1;
    if (recv_all_client(fd, buffer, len)) return -1;
    buffer[len] = '\0'; return (int)len;
}


void show_help(void) {
    printf("\n");
    printf("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    printf("│ 游戏命令：                                                                   │\n");
    printf("│ login <用户名>   - 登录游戏 (首次登录自动创建账户)                          │\n");
    printf("│ create_room      - 创建房间（自动成为玩家 A，等待对手）                     │\n");
    printf("│ list_rooms       - 查看所有等待中的房间                                     │\n");
    printf("│ join_room        - 输入房间号加入（成为玩家 B）                             │\n");
    printf("│ leave_room       - 退出当前房间（房间销毁）                                 │\n");
    printf("│ stats            - 查看个人统计数据                                         │\n");
    printf("│ rank             - 查看排行榜 Top 10                                        │\n");
    printf("│ titles           - 查看称号列表                                             │\n");
    printf("│ redeem <等级>    - 兑换称号，例: redeem 5                                   │\n");
    printf("│ help             - 显示此帮助信息                                           │\n");
    printf("│ quit             - 退出游戏                                                 │\n");
    printf("│                                                                             │\n");
    printf("│ 在房间中直接输入 0-999 的数字即可出牌                                      │\n");
    printf("└──────────────────────────────────────────────────────────────────────────────┘\n");
}

void show_rules(void) {
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════════════════════╗\n");
    printf("║  🎯 猜数字对战游戏规则                                                       ║\n");
    printf("╠═══════════════════════════════════════════════════════════════════════════════╣\n");
    printf("║  1. 玩家 A 创建房间，玩家 B 加入房间                                        ║\n");
    printf("║  2. 双方各出 0-999 之间的数字                                               ║\n");
    printf("║  3. 判定规则：                                                              ║\n");
    printf("║     ● A 和 B 同为奇数 或 同为偶数 → 玩家 A 获胜 (+10分)                    ║\n");
    printf("║     ● A 和 B 一奇一偶 → 玩家 B 获胜 (+10分)                                ║\n");
    printf("║  4. 每方有 10 秒出数字时间，超时判负 (-5分)                                ║\n");
    printf("╠═══════════════════════════════════════════════════════════════════════════════╣\n");
    printf("║  称号系统（由低到高）：                                                     ║\n");
    printf("║  初入江湖 → 略有所成 → 小有名气 → 实力不凡 → 百战精英                      ║\n");
    printf("║  → 千胜宗师 → 纵横天下 → 一代枭雄 → 绝世高手 → 武林至尊                   ║\n");
    printf("║  必须逐级解锁，称号显示为：【称号】用户名                                   ║\n");
    printf("╚═══════════════════════════════════════════════════════════════════════════════╝\n");
}

void show_stats(const char *data) {
    char title[32], username[32], next_name[32];
    int level, score, games, wins, losses, draws, rank, next_level, next_cost;
    
    sscanf(data, "%[^|]|%d|%[^|]|%d|%d|%d|%d|%d|%d|%[^|]|%d",
           title, &level, username, &score, &games, &wins, &losses, &draws, &rank, next_name, &next_cost);
    
    double winrate = (games > 0) ? (wins * 100.0 / games) : 0.0;
    
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║              📊 个人统计数据                               ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║ 玩家: 【%s】%s\n", title, username);
    printf("║ 当前称号: %s (等级%d/10)\n", title, level);
    
    if (level < 10) {
        int diff = next_cost - score;
        printf("║ 下一级称号: %s (需要%d积分)\n", next_name, next_cost);
        if (diff > 0) {
            printf("║ 距离下一级还差: %d 积分\n", diff);
        } else {
            printf("║ ✅ 已满足解锁条件！\n");
        }
    } else {
        printf("║ 🎉 已满级！你已是武林至尊！\n");
    }
    
    printf("║ 积分: %d\n", score);
    printf("║ 总场次: %d | 胜: %d | 负: %d | 平: %d\n", games, wins, losses, draws);
    printf("║ 胜率: %.1f%%\n", winrate);
    printf("║ 排名: #%d\n", rank);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
}

void show_rank(const char *data) {
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║ 🏆 积分排行榜 Top 10                                       ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    
    if (data == NULL || strlen(data) == 0) {
        printf("║ 📭 暂无排行数据                                          ║\n");
        printf("╚══════════════════════════════════════════════════════════════╝\n");
        return;
    }
    
    // 直接解析原始字符串，不用 strtok
    char *p = strdup(data);
    char *ptr = p;
    int rank = 1;
    int has_data = 0;
    
    while (*ptr != '\0') {
        // 找第一个 | 
        char *pipe1 = strchr(ptr, '|');
        if (pipe1 == NULL) break;
        *pipe1 = '\0';
        char *title = ptr;
        
        // 找第二个 |
        char *pipe2 = strchr(pipe1 + 1, '|');
        if (pipe2 == NULL) break;
        *pipe2 = '\0';
        char *username = pipe1 + 1;
        
        // 找第三个 | 或结尾
        char *pipe3 = strchr(pipe2 + 1, '|');
        char *score_start = pipe2 + 1;
        if (pipe3 != NULL) {
            *pipe3 = '\0';
            ptr = pipe3 + 1;
        } else {
            ptr = score_start + strlen(score_start);
        }
        
        int score = atoi(score_start);
        printf("║ #%-2d  【%s】%-12s  %5d 分\n", rank, title, username, score);
        rank++;
        has_data = 1;
        
        if (pipe3 == NULL) break;
    }
    free(p);
    
    if (!has_data) {
        printf("║ 📭 暂无排行数据                                          ║\n");
    }
    printf("╚══════════════════════════════════════════════════════════════╝\n");
}

void show_titles(const char *data) {
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║ 👑 称号系统                                                ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    
    if (data == NULL || strlen(data) == 0) {
        printf("║ 📭 暂无称号数据                                          ║\n");
        printf("╚══════════════════════════════════════════════════════════════╝\n");
        return;
    }
    
    char *p = strdup(data);
    char *ptr = p;
    int has_data = 0;
    
    while (*ptr != '\0') {
        // 找第一个 |
        char *pipe1 = strchr(ptr, '|');
        if (pipe1 == NULL) break;
        *pipe1 = '\0';
        int level = atoi(ptr);
        
        // 找第二个 |
        char *pipe2 = strchr(pipe1 + 1, '|');
        if (pipe2 == NULL) break;
        *pipe2 = '\0';
        char *name = pipe1 + 1;
        
        // 找第三个 |
        char *pipe3 = strchr(pipe2 + 1, '|');
        if (pipe3 == NULL) break;
        *pipe3 = '\0';
        int cost = atoi(pipe2 + 1);
        
        // 找第四个 | 或结尾
        char *pipe4 = strchr(pipe3 + 1, '|');
        char *status_start = pipe3 + 1;
        if (pipe4 != NULL) {
            *pipe4 = '\0';
            ptr = pipe4 + 1;
        } else {
            ptr = status_start + strlen(status_start);
        }
        char *status = status_start;
        
        has_data = 1;
        if (strcmp(status, "current") == 0) {
            printf("║ 👑 等级%d: %-12s ★当前装备★\n", level, name);
        } else if (strcmp(status, "owned") == 0) {
            printf("║ ✅ 等级%d: %-12s (已拥有)\n", level, name);
        } else if (strcmp(status, "next") == 0) {
            printf("║ 🔓 等级%d: %-12s (可解锁，需要%d积分) ⬅️ 下一个\n", level, name, cost);
        } else {
            printf("║ 🔒 等级%d: %-12s (需要%d积分)\n", level, name, cost);
        }
        
        if (pipe4 == NULL) break;
    }
    free(p);
    
    if (!has_data) {
        printf("║ 📭 暂无称号数据                                          ║\n");
    }
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║ 兑换命令: redeem <等级> 例: redeem 5                       ║\n");
    printf("╚══════════════════════════════════════════════════════════════╝\n");
}

void show_redeem_result(const char *data) {
    printf("\n");
    if (strstr(data, "🎉") || strstr(data, "恭喜")) {
        printf("╔══════════════════════════════════════════════════════════════╗\n");
        printf("║  🎉 称号兑换成功                                          ║\n");
        printf("╠══════════════════════════════════════════════════════════════╣\n");
        printf("║  %s\n", data);
        printf("╚══════════════════════════════════════════════════════════════╝\n");
    } else {
        printf("╔══════════════════════════════════════════════════════════════╗\n");
        printf("║  ❌ 称号兑换失败                                          ║\n");
        printf("╠══════════════════════════════════════════════════════════════╣\n");
        printf("║  %s\n", data);
        printf("╚══════════════════════════════════════════════════════════════╝\n");
    }
}

void send_to_server(const char *msg) {
    if (sock_fd < 0) return;
    printf("[Client] Sending: %s\n", msg);
    send_frame(sock_fd, msg);
}

void *recv_thread_func(void *arg) {
    char buffer[BUFFER_SIZE];
    
    while (running) {
        int bytes = recv_frame(sock_fd, buffer, sizeof(buffer));
        if (bytes <= 0) {
            printf("\n[系统] 与服务器断开连接\n");
            running = 0;
            break;
        }
        buffer[bytes] = '\0';
        printf("[Client] Received: %s\n", buffer);
        
        if (strncmp(buffer, "STATS|", 6) == 0) {
            show_stats(buffer + 6);
        } else if (strncmp(buffer, "RANK|", 5) == 0) {
            show_rank(buffer + 5);
        } else if (strncmp(buffer, "TITLES|", 7) == 0) {
            show_titles(buffer + 7);
        } else if (strncmp(buffer, "REDEEM_RESULT|", 14) == 0) {
            show_redeem_result(buffer + 14);
        } else if (strncmp(buffer, "LOGIN_RESULT|", 13) == 0) {
            printf("\n✅ %s\n", buffer + 13);
        } else if (strncmp(buffer, "ROOM_CREATED|", 13) == 0) {
            printf("\n✅ %s\n", buffer + 13);
        } else if (strncmp(buffer, "JOIN_SUCCESS|", 13) == 0) {
            printf("\n✅ %s\n", buffer + 13);
        } else if (strncmp(buffer, "LEAVE_SUCCESS|", 14) == 0) {
            printf("\n✅ %s\n", buffer + 14);
        } else if (strncmp(buffer, "SUBMIT_OK|", 10) == 0) {
            printf("\n✅ %s\n", buffer + 10);
        } else if (strncmp(buffer, "RESULT|", 7) == 0) {
            printf("\n%s\n", buffer + 7);
        } else if (strncmp(buffer, "NOTIFY|", 7) == 0) {
            printf("\n📢 %s\n", buffer + 7);
        } else if (strncmp(buffer, "ROOM_DESTROY|", 13) == 0) {
            printf("\n%s\n", buffer + 13);
        } else if (strncmp(buffer, "ERROR|", 6) == 0) {
            printf("\n❌ %s\n", buffer + 6);
        } else if (strncmp(buffer, ">>>", 3) == 0) {
            printf("\n%s\n", buffer);
        } else {
            printf("\n%s\n", buffer);
        }
        
        printf("> ");
        fflush(stdout);
    }
    return NULL;
}

int connect_server() {
    struct sockaddr_in server_addr;
    
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return -1;
    }
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);
    
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sock_fd);
        sock_fd = -1;
        return -1;
    }
    
    printf("已连接到服务器 %s:%d\n", SERVER_IP, SERVER_PORT);
    return 0;
}

int main(int argc, char *argv[]) {
    // 修复删除键 (Backspace)
    struct termios term;
    tcgetattr(STDIN_FILENO, &term);
    term.c_lflag |= ICANON | ECHO;
    term.c_cc[VERASE] = 0x7f;
    tcsetattr(STDIN_FILENO, TCSANOW, &term);
    
    signal(SIGPIPE, SIG_IGN);
    
    if (connect_server() < 0) {
        return -1;
    }
    
    pthread_t recv_thread;
    pthread_create(&recv_thread, NULL, recv_thread_func, NULL);
    
    printf("\n");
    printf("═══════════════════════════════════════════════════════════════════════════════\n");
    printf(" 🎯 猜数字对战游戏 客户端 v2.0 (积分/称号/排行榜)\n");
    printf("═══════════════════════════════════════════════════════════════════════════════\n");
    
    show_help();
    show_rules();
    
    printf("\n> ");
    fflush(stdout);
    
    char *input = NULL;
    size_t len = 0;
    ssize_t read;
    
    while (running) {
        read = getline(&input, &len, stdin);
        if (read < 0) {
            break;
        }
        
        if (input[read - 1] == '\n') {
            input[read - 1] = '\0';
        }
        
        char *start = input;
        while (*start == ' ') start++;
        if (strlen(start) == 0) {
            printf("> ");
            fflush(stdout);
            continue;
        }
        
        if (strcmp(start, "help") == 0) {
            show_help();
            show_rules();
            printf("> ");
            fflush(stdout);
            continue;
        }
        
        if (strcmp(start, "quit") == 0 || strcmp(start, "exit") == 0) {
            printf("正在退出...\n");
            running = 0;
            break;
        }
        
        send_to_server(start);
        printf("> ");
        fflush(stdout);
    }
    
    free(input);
    running = 0;
    if (sock_fd > 0) {
        close(sock_fd);
    }
    pthread_join(recv_thread, NULL);
    
    printf("已退出游戏\n");
    return 0;
}
