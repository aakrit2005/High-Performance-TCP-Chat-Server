#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <algorithm>
#include <thread>
#include <mutex>
#include <csignal>
#include <atomic>

using namespace std;

struct ClientState{
    vector<char> recv_buffer;
    vector<char> send_buffer;
    mutex send_mutex;

    bool is_downloading = false;
    uint32_t bytes_remaining = 0;
    ofstream file_stream;
    string downloading_filename; 
};

atomic<bool> client_running(true);

void handle_shutdown(int sig) {
    (void)sig;
    cout << "\n[CLIENT] Shutdown signal received. Disconnecting safely...\n";
    client_running = false;
}

void set_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void enqueue_file(ClientState& state, const string& filename, const vector<char>& file_data) {
    lock_guard<mutex> lock(state.send_mutex);
    uint8_t type = 1;
    state.send_buffer.push_back(type);

    uint8_t fname_len = static_cast<uint8_t>(filename.size());
    state.send_buffer.push_back(fname_len);

    uint32_t payload_len = htonl(file_data.size());
    char* length_ptr = reinterpret_cast<char*>(&payload_len);
    state.send_buffer.insert(state.send_buffer.end(), length_ptr, length_ptr + sizeof(payload_len));

    state.send_buffer.insert(state.send_buffer.end(), filename.begin(), filename.end());
    state.send_buffer.insert(state.send_buffer.end(), file_data.begin(), file_data.end());
}

void enqueue_text(ClientState& state, const string& message) {
    lock_guard<mutex> lock(state.send_mutex);
    uint8_t type = 0;
    state.send_buffer.push_back(type);

    uint8_t fname_len = 0;
    state.send_buffer.push_back(fname_len);

    uint32_t payload_len = htonl(message.size());
    char* length_ptr = reinterpret_cast<char*>(&payload_len);
    state.send_buffer.insert(state.send_buffer.end(), length_ptr, length_ptr + sizeof(payload_len));

    state.send_buffer.insert(state.send_buffer.end(), message.begin(), message.end());
}

void handle_reading(int sock_fd, ClientState& state) {
    while (client_running) {
        char temp_buff[4096];
        int bytes = read(sock_fd, temp_buff, sizeof(temp_buff));

        if (bytes > 0) {
            if (state.is_downloading) {
                uint32_t to_write = min((uint32_t)bytes, state.bytes_remaining);
                state.file_stream.write(temp_buff, to_write);
                state.bytes_remaining -= to_write;

                if (state.bytes_remaining == 0) {
                    state.file_stream.close();
                    state.is_downloading = false;
                    cout << "[CLIENT] Image/File '" << state.downloading_filename << "' fully downloaded!\n";

                    if (bytes > (int)to_write) {
                        state.recv_buffer.insert(state.recv_buffer.end(),
                            temp_buff + to_write, temp_buff + bytes);
                    }
                }
            } else {
                state.recv_buffer.insert(state.recv_buffer.end(), temp_buff, temp_buff + bytes);

                while (state.recv_buffer.size() >= 6) {
                    int type = state.recv_buffer[0];
                    int filename_length = state.recv_buffer[1];

                    uint32_t network_payload_length;
                    memcpy(&network_payload_length, state.recv_buffer.data() + 2, sizeof(uint32_t));
                    uint32_t payload_length = ntohl(network_payload_length);
                    size_t header_size = 6;

                    if (type == 1) { 
                        if (state.recv_buffer.size() >= header_size + (size_t)filename_length) {
                            string filename(state.recv_buffer.begin() + header_size,
                                            state.recv_buffer.begin() + header_size + filename_length);

                            state.downloading_filename = filename;
                            state.file_stream.open("client_" + filename, ios::binary);
                            state.is_downloading = true;
                            state.bytes_remaining = payload_length;

                            size_t total_header_size = header_size + filename_length;
                            size_t leftover_bytes = state.recv_buffer.size() - total_header_size;

                            if (leftover_bytes > 0) {
                                size_t bytes_to_write = min(leftover_bytes, (size_t)payload_length);
                                state.file_stream.write(state.recv_buffer.data() + total_header_size, bytes_to_write);
                                state.bytes_remaining -= bytes_to_write;
                            }

                            state.recv_buffer.erase(state.recv_buffer.begin(),
                                state.recv_buffer.begin() + total_header_size + min(leftover_bytes, (size_t)payload_length));

                            if (state.bytes_remaining == 0) {
                                state.file_stream.close();
                                state.is_downloading = false;
                                cout << "[CLIENT] Image/File '" << filename << "' fully downloaded and saved as 'client_" << filename << "'!\n";
                            }

                            if (state.is_downloading) break;
                        } else {
                            break;
                        }
                    } else if (type == 0) {
                        if (state.recv_buffer.size() >= header_size + payload_length) {
                            string message(state.recv_buffer.begin() + header_size,
                                           state.recv_buffer.begin() + header_size + payload_length);

                            cout << ">> " << message << "\n";

                            state.recv_buffer.erase(state.recv_buffer.begin(),
                                state.recv_buffer.begin() + header_size + payload_length);
                        } else {
                            break;
                        }
                    }
                }
            }
        } else if (bytes == 0) {
            cout << "Disconnected from server.\n";
            client_running = false;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("read error");
            client_running = false;
            return;
        }
    }
}

void handle_writing(int sock_fd, ClientState& state) {
    lock_guard<mutex> lock(state.send_mutex);
    if (state.send_buffer.empty()) return;

    while (!state.send_buffer.empty()) {
        int bytes_sent = write(sock_fd, state.send_buffer.data(), state.send_buffer.size());

        if (bytes_sent > 0) {
            state.send_buffer.erase(state.send_buffer.begin(), state.send_buffer.begin() + bytes_sent);
        } else if (bytes_sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("write error");
            client_running = false;
            return;
        }
    }
}

void handle_stdin(ClientState& state) {
    string line;
    if (!getline(cin, line)) {
        client_running = false;
        return;
    }

    if (line.empty()) return;

    if (line.rfind("/send ", 0) == 0) {
        string filename = line.substr(6);
        ifstream f(filename, ios::binary);
        if (!f) {
            cout << "Could not open file: " << filename << "\n";
            return;
        }
        vector<char> file_data((istreambuf_iterator<char>(f)), istreambuf_iterator<char>());
        size_t slash = filename.rfind('/');
        string bare = (slash == string::npos) ? filename : filename.substr(slash + 1);
        enqueue_file(state, bare, file_data);
        cout << "Queued " << bare << " (" << file_data.size() << " bytes) for sending...\n";
    } else {
        enqueue_text(state, line);
        
        cout << "[You]: " << line << endl; 
    }
}

void read_worker_thread(int sock_fd, ClientState& state) {
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[10];

    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = sock_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &ev);

    while (client_running) {
        int nfds = epoll_wait(epoll_fd, events, 10, 500);
        if (nfds == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait reader");
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            if (events[i].events & EPOLLIN) {
                handle_reading(sock_fd, state);
            }
        }
    }
    close(epoll_fd);
}

void write_worker_thread(int sock_fd, ClientState& state) {
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[10];

    ev.events = EPOLLIN;
    ev.data.fd = STDIN_FILENO;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &ev);

    ev.events = EPOLLOUT | EPOLLET;
    ev.data.fd = sock_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &ev);

    while (client_running) {
        int nfds = epoll_wait(epoll_fd, events, 10, 500);
        if (nfds == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait writer");
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == STDIN_FILENO) {
                handle_stdin(state);
                handle_writing(sock_fd, state);
            } else if (fd == sock_fd && (events[i].events & EPOLLOUT)) {
                handle_writing(sock_fd, state);
            }
        }
    }
    close(epoll_fd);
}

int main() {
    signal(SIGINT, handle_shutdown);
    signal(SIGTERM, handle_shutdown);

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(5000);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        return 1;
    }

    cout << "Connected. Type a message or /send <filename> to send a file.\n";
    cout << "Press Ctrl+C to disconnect safely.\n";

    set_non_blocking(sock_fd);

    ClientState state;

    thread reader(read_worker_thread, sock_fd, std::ref(state));
    thread writer(write_worker_thread, sock_fd, std::ref(state));

    reader.join();
    writer.join();

    if (state.is_downloading && state.file_stream.is_open()) {
        state.file_stream.close();
        cout << "[CLIENT] Saved partial file chunk safely.\n";
    }

    close(sock_fd);
    cout << "[CLIENT] Disconnected successfully.\n";
    return 0;
}