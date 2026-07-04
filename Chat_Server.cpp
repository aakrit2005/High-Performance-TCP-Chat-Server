#include <iostream>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <cerrno>
#include <vector>
#include <string>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <unordered_map>
#include <cstring>
#include <fstream>
#include <algorithm>
#include <csignal>
#include <atomic>
#include <memory>

using namespace std;

struct ClientState {
    vector<char> buffer;
    vector<char> send_buffer;
    
    mutex send_mutex;
    recursive_mutex state_mutex; 
    
    bool is_downloading = false;
    uint32_t bytes_remaining = 0;
    ofstream file_stream;
    string username;
    string downloading_filename; 
};

vector<int> clients;
unordered_map<int, shared_ptr<ClientState>> active_clients;
unordered_map<string, int> username_to_fd;

mutex key;
mutex key2;
mutex print_mutex; 
condition_variable cv;
queue<pair<int, uint32_t>> task_queue;

atomic<bool> server_running(true);
vector<thread> thread_pool;

void handle_client_data_write(int triggered_fd, const string& message, int epoll_fd);
void send_direct_message(int target_fd, const string& message, int epoll_fd);

void thread_log(const string& msg) {
    lock_guard<mutex> lock(print_mutex);
    cout << msg << "\n";
}

void set_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl(F_GETFL)");
        return;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl(F_SETFL)");
    }
}

void handle_shutdown(int sig) {
    (void)sig;
    thread_log("\n[SERVER] Shutdown signal received. Closing all connections...");
    server_running = false;

    {
        lock_guard<mutex> lock(key2);
        for (int fd : clients) {
            auto it = active_clients.find(fd);
            if (it == active_clients.end()) continue;
            string bye = "[SERVER]: Server is shutting down. Goodbye!";
            auto &state = it->second;
            
            lock_guard<recursive_mutex> clock(state->state_mutex);
            lock_guard<mutex> slock(state->send_mutex);
            auto &buf = state->send_buffer;
            
            uint8_t type = 0;
            buf.push_back(type);
            uint8_t fname_len = 0;
            buf.push_back(fname_len);
            uint32_t payload_len = htonl(bye.size());
            char* lp = reinterpret_cast<char*>(&payload_len);
            buf.insert(buf.end(), lp, lp + sizeof(payload_len));
            buf.insert(buf.end(), bye.begin(), bye.end());
            
            write(fd, buf.data(), buf.size());
        }
    }
    cv.notify_all();
}

void handle_new_connection(int epoll_fd, int server_fd) {
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("accept");
            break;
        }

        set_non_blocking(client_fd);

        {
            lock_guard<mutex> lock(key2);
            clients.push_back(client_fd);
            string default_name = "Guest_" + to_string(client_fd);
            active_clients[client_fd] = make_shared<ClientState>();
            active_clients[client_fd]->username = default_name;
            username_to_fd[default_name] = client_fd;
        }

        epoll_event event;
        event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        event.data.fd = client_fd;

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event) == -1) {
            perror("epoll_ctl ADD");
            close(client_fd);
            continue;
        }
        thread_log("New client connected! Ticket: " + to_string(client_fd));
    }
}

void disconnect_client(int epoll_fd, int client_fd) {
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
    {
        lock_guard<mutex> lock(key2);
        for (auto it = clients.begin(); it != clients.end(); ++it) {
            if (*it == client_fd) {
                clients.erase(it);
                break;
            }
        }
        auto it = active_clients.find(client_fd);
        if (it != active_clients.end()) {
            username_to_fd.erase(it->second->username);
            active_clients.erase(it);
        }
    }
    close(client_fd);
}

void flush_send_buffer(int client_fd, shared_ptr<ClientState> state) {
    lock_guard<mutex> lock(state->send_mutex);
    auto &buffer = state->send_buffer;

    while (!buffer.empty()) {
        int bytes_sent = write(client_fd, buffer.data(), buffer.size());

        if (bytes_sent > 0) {
            buffer.erase(buffer.begin(), buffer.begin() + bytes_sent);
        } else if (bytes_sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break; 
        }
    }
}

void broadcast_file(int sender_fd, const string& filename, int epoll_fd) {
    ifstream f("server_" + filename, ios::binary);
    if (!f) return;
    vector<char> file_data((istreambuf_iterator<char>(f)), istreambuf_iterator<char>());

    vector<pair<int, shared_ptr<ClientState>>> targets;
    {
        lock_guard<mutex> lock(key2);
        for (int fd : clients) {
            if (fd == sender_fd) continue;
            auto it = active_clients.find(fd);
            if (it == active_clients.end()) continue;
            targets.push_back({fd, it->second});
        }
    }

    for (auto& [fd, target_state] : targets) {
        {
            lock_guard<mutex> lock(target_state->send_mutex);
            auto &buf = target_state->send_buffer;
            
            uint8_t type = 1; 
            buf.push_back(type);
            
            uint8_t fname_len = static_cast<uint8_t>(filename.size());
            buf.push_back(fname_len);
            
            uint32_t payload_len = htonl(file_data.size());
            char* length_ptr = reinterpret_cast<char*>(&payload_len);
            buf.insert(buf.end(), length_ptr, length_ptr + sizeof(payload_len));
            
            buf.insert(buf.end(), filename.begin(), filename.end());
            buf.insert(buf.end(), file_data.begin(), file_data.end());
        }

        {
            lock_guard<recursive_mutex> lock(target_state->state_mutex);
            flush_send_buffer(fd, target_state);
            epoll_event event;
            event.data.fd = fd;
            event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            {
                lock_guard<mutex> slock(target_state->send_mutex);
                if (!target_state->send_buffer.empty()) event.events |= EPOLLOUT;
            }
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event);
        }
    }
    thread_log("[SERVER]: Forwarded file '" + filename + "' to all other clients.");
}

bool handle_client_data_read(int epoll_fd, int client_fd, shared_ptr<ClientState> state) {
    char temp_buff[4096];

    while (true) {
        int bytes = read(client_fd, temp_buff, sizeof(temp_buff));

        if (bytes > 0) {
            if (state->is_downloading) {
                uint32_t to_write = min((uint32_t)bytes, state->bytes_remaining);
                state->file_stream.write(temp_buff, to_write);
                state->bytes_remaining -= to_write;

                if (state->bytes_remaining == 0) {
                    state->file_stream.close();
                    state->is_downloading = false;
                    thread_log("File fully received from client " + to_string(client_fd) + "!");
                    
                    broadcast_file(client_fd, state->downloading_filename, epoll_fd);

                    if (bytes > (int)to_write) {
                        state->buffer.insert(state->buffer.end(),
                            temp_buff + to_write, temp_buff + bytes);
                    }
                }
            } else {
                state->buffer.insert(state->buffer.end(), temp_buff, temp_buff + bytes);

                while (state->buffer.size() >= 6) {
                    int type = state->buffer[0];
                    int filename_length = state->buffer[1];

                    uint32_t network_payload_length;
                    memcpy(&network_payload_length, state->buffer.data() + 2, sizeof(uint32_t));
                    uint32_t payload_length = ntohl(network_payload_length);
                    size_t header_size = 6;

                    if (type == 1) {
                        if (state->buffer.size() >= header_size + (size_t)filename_length) {
                            string filename(state->buffer.begin() + header_size,
                                            state->buffer.begin() + header_size + filename_length);

                            state->downloading_filename = filename; 
                            state->file_stream.open("server_" + filename, ios::binary);
                            state->is_downloading = true;
                            state->bytes_remaining = payload_length;

                            size_t total_header_size = header_size + filename_length;
                            size_t leftover_bytes = state->buffer.size() - total_header_size;

                            if (leftover_bytes > 0) {
                                size_t bytes_to_write = min(leftover_bytes, (size_t)payload_length);
                                state->file_stream.write(state->buffer.data() + total_header_size, bytes_to_write);
                                state->bytes_remaining -= bytes_to_write;
                            }

                            state->buffer.erase(state->buffer.begin(),
                                state->buffer.begin() + total_header_size + min(leftover_bytes, (size_t)payload_length));

                            if (state->bytes_remaining == 0) {
                                state->file_stream.close();
                                state->is_downloading = false;
                                thread_log("Small file fully received from client " + to_string(client_fd) + "!");
                                
                                broadcast_file(client_fd, state->downloading_filename, epoll_fd);
                            }

                            if (state->is_downloading) break;
                        } else {
                            break;
                        }
                    } else if (type == 0) {
                        if (state->buffer.size() >= header_size + payload_length) {
                            string message(state->buffer.begin() + header_size,
                                           state->buffer.begin() + header_size + payload_length);

                            if (message.rfind("/msg ", 0) == 0) {
                                size_t second_space = message.find(' ', 5);

                                if (second_space != string::npos) {
                                    string target_user = message.substr(5, second_space - 5);
                                    string private_msg = message.substr(second_space + 1);

                                    int target_fd = -1;
                                    {
                                        lock_guard<mutex> lock(key2);
                                        auto it = username_to_fd.find(target_user);
                                        if (it != username_to_fd.end()) {
                                            target_fd = it->second;
                                        }
                                    }

                                    if (target_fd != -1) {
                                        string formatted = "[Private from " + state->username + "]: " + private_msg;
                                        send_direct_message(target_fd, formatted, epoll_fd);
                                        string echo = "[Sent to " + target_user + "]: " + private_msg;
                                        send_direct_message(client_fd, echo, epoll_fd);
                                    } else {
                                        send_direct_message(client_fd, "[SERVER]: User '" + target_user + "' not found.", epoll_fd);
                                    }
                                }
                            } else if (message.rfind("/nick ", 0) == 0) {
                                string new_name = message.substr(6);
                                new_name.erase(new_name.find_last_not_of(" \n\r\t") + 1);

                                string old_name = state->username;
                                {
                                    lock_guard<mutex> lock(key2);
                                    username_to_fd.erase(old_name);
                                    username_to_fd[new_name] = client_fd;
                                    state->username = new_name;
                                }

                                thread_log(old_name + " changed name to " + new_name);
                                string system_msg = "[SERVER]: " + old_name + " is now known as " + new_name;
                                handle_client_data_write(-1, system_msg, epoll_fd);
                            } else {
                                string formatted = "[" + state->username + "]: " + message;
                                thread_log(formatted);
                                handle_client_data_write(client_fd, formatted, epoll_fd);
                            }

                            state->buffer.erase(state->buffer.begin(),
                                state->buffer.begin() + header_size + payload_length);
                        } else {
                            break;
                        }
                    }
                }
            }
        } else if (bytes == 0) {
            string leave_msg = "[SERVER]: " + state->username + " left the chat";
            handle_client_data_write(client_fd, leave_msg, epoll_fd);
            thread_log("Client " + to_string(client_fd) + " disconnected.");
            disconnect_client(epoll_fd, client_fd);
            return false;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            thread_log("Client " + to_string(client_fd) + " read error.");
            disconnect_client(epoll_fd, client_fd);
            return false;
        }
    }
    return true;
}

void send_direct_message(int target_fd, const string& message, int epoll_fd) {
    shared_ptr<ClientState> state;
    {
        lock_guard<mutex> lock(key2);
        auto it = active_clients.find(target_fd);
        if (it == active_clients.end()) return;
        state = it->second;
    }

    {
        lock_guard<mutex> lock(state->send_mutex);
        auto &buf = state->send_buffer;
        uint8_t type = 0;
        buf.push_back(type);
        uint8_t fname_len = 0;
        buf.push_back(fname_len);
        uint32_t payload_len = htonl(message.size());
        char* length_ptr = reinterpret_cast<char*>(&payload_len);
        buf.insert(buf.end(), length_ptr, length_ptr + sizeof(payload_len));
        buf.insert(buf.end(), message.begin(), message.end());
    }

    {
        lock_guard<recursive_mutex> lock(state->state_mutex);
        flush_send_buffer(target_fd, state);
        
        epoll_event event;
        event.data.fd = target_fd;
        event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        {
            lock_guard<mutex> slock(state->send_mutex);
            if (!state->send_buffer.empty()) event.events |= EPOLLOUT;
        }
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, target_fd, &event);
    }
}

void handle_client_data_write(int triggered_fd, const string& message, int epoll_fd) {
    vector<pair<int, shared_ptr<ClientState>>> targets;

    {
        lock_guard<mutex> lock(key2);
        for (int fd : clients) {
            if (fd == triggered_fd) continue;
            auto it = active_clients.find(fd);
            if (it == active_clients.end()) continue;
            targets.push_back({fd, it->second});
        }
    }

    for (auto& [fd, state] : targets) {
        {
            lock_guard<mutex> lock(state->send_mutex);
            auto &buf = state->send_buffer;
            uint8_t type = 0;
            buf.push_back(type);
            uint8_t fname_len = 0;
            buf.push_back(fname_len);
            uint32_t payload_len = htonl(message.size());
            char* length_ptr = reinterpret_cast<char*>(&payload_len);
            buf.insert(buf.end(), length_ptr, length_ptr + sizeof(payload_len));
            buf.insert(buf.end(), message.begin(), message.end());
        }

        {
            lock_guard<recursive_mutex> lock(state->state_mutex);
            flush_send_buffer(fd, state);
            
            epoll_event event;
            event.data.fd = fd;
            event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            {
                lock_guard<mutex> slock(state->send_mutex);
                if (!state->send_buffer.empty()) event.events |= EPOLLOUT;
            }
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event);
        }
    }
}

void worker_threads(int epoll_fd) {
    while (server_running || !task_queue.empty()) {
        int fd = -1;
        uint32_t events = 0;

        {
            unique_lock<mutex> lock(key);
            cv.wait(lock, []{ return !task_queue.empty() || !server_running; });

            if (task_queue.empty() && !server_running) return;

            if (!task_queue.empty()) {
                fd = task_queue.front().first;
                events = task_queue.front().second;
                task_queue.pop();
            }
        }

        if (fd == -1) continue;

        shared_ptr<ClientState> state;
        {
            lock_guard<mutex> lock(key2);
            auto it = active_clients.find(fd);
            if (it == active_clients.end()) continue;
            state = it->second;
        }

        {
            lock_guard<recursive_mutex> client_lock(state->state_mutex);
            
            bool alive = true;

            if (events & EPOLLIN) {
                alive = handle_client_data_read(epoll_fd, fd, state);
            }

            if (alive && (events & EPOLLOUT)) {
                flush_send_buffer(fd, state);
            } 
            
            if (alive) {
                epoll_event event;
                event.data.fd = fd;
                event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                {
                    lock_guard<mutex> lock(state->send_mutex);
                    if (!state->send_buffer.empty()) event.events |= EPOLLOUT;
                }
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event);
            }
        }
    }
}

int main() {
    signal(SIGINT, handle_shutdown);
    signal(SIGTERM, handle_shutdown);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(5000);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 5) == -1) {
        perror("listen");
        return 1;
    }

    set_non_blocking(server_fd);

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("EPOLL_CREATE");
        return 1;
    }

    epoll_event event;
    event.data.fd = server_fd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
        perror("EPOLL_CTL");
        return 1;
    }

    for (int i = 0; i < 8; i++) {
        thread_pool.emplace_back(worker_threads, epoll_fd);
    }

    constexpr int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    thread_log("Server listening on port 5000...\nPress Ctrl+C to initiate graceful shutdown.");

    while (server_running) {
        int ready = epoll_wait(epoll_fd, events, MAX_EVENTS, 500);
        if (ready == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < ready; i++) {
            int triggered_fd = events[i].data.fd;
            uint32_t triggered_events = events[i].events;

            if (triggered_fd == server_fd) {
                handle_new_connection(epoll_fd, server_fd);
            } else {
                {
                    lock_guard<mutex> lock(key);
                    task_queue.push({triggered_fd, triggered_events});
                }
                cv.notify_one();
            }
        }
    }

    thread_log("[SERVER] Stopping thread pool...");
    cv.notify_all();
    for (auto& worker : thread_pool) {
        if (worker.joinable()) worker.join();
    }

    thread_log("[SERVER] Closing sockets...");
    {
        lock_guard<mutex> lock(key2);
        for (auto& p : active_clients) {
            if (p.second->is_downloading && p.second->file_stream.is_open()) {
                p.second->file_stream.close();
                thread_log("Saved partial file for " + p.second->username);
            }
            close(p.first);
        }
    }

    close(server_fd);
    close(epoll_fd);
    thread_log("[SERVER] Shutdown complete. Goodbye!");
    return 0;
}