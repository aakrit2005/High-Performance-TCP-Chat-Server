# HYPERCHAT - High-Performance TCP Chat Server
> A scalable multi-client TCP chat server built from scratch in modern C++17 using the Linux socket API and epoll.

The protocol is length-prefixed rather than delimiter-based, allowing arbitrary binary payloads without ambiguity.

## Why this project?
This project was built to understand how high-level networking frameworks work internally by implementing asynchronous TCP networking directly on top of Linux system calls rather than relying on networking libraries. The core focus is on asynchronous concurrency, efficient buffer management, and protocol correctness under heavy load.

## Technical Highlights
* Non-blocking TCP sockets
* Edge-triggered epoll
* EPOLLONESHOT synchronization
* Thread pool architecture
* Length-prefixed binary protocol
* Per-client receive buffers
* Per-client send buffers
* Partial read handling
* Partial write handling
* Binary file transfer

## Core Features
* **High-Concurrency Architecture:** Utilizes edge-triggered `epoll` with `EPOLLONESHOT` to safely distribute network events across an 8-thread worker pool.
* **Custom Binary Protocol:** Replaces standard text delimiters with a length-prefixed binary framing protocol to support arbitrary payloads.
* **Reliable Stream Handling:** Independent, persistent read/write buffers per client to correctly handle partial socket reads, fragmented packets, and `EAGAIN` blocking without corrupting data.
* **Asynchronous File Transfers:** Streams binary files (JPEG, PNG, PDF, MP4, etc.) directly to disk without exhausting server RAM.
* **Application Layer:** Supports real-time broadcast chat, private messaging (`/msg`), dynamic user nicknames (`/nick`), and graceful `SIGINT` shutdowns.
* **Reference Client:** Includes a dedicated two-thread console client (read/write) for immediate testing.

## Server in Action

<img width="730" alt="image" src="https://github.com/user-attachments/assets/17c195b1-1660-4f65-8ff3-b2839b797a9e" />

<img width="730" alt="image" src="https://github.com/user-attachments/assets/c18f536a-3862-4915-ba70-c6827cbca837" />

<img width="730" alt="image" src="https://github.com/user-attachments/assets/eb9a6ad6-0d98-4fd2-8708-b449ecc269e1" />

## Stress Testing & Performance
The server's asynchronous reactor pattern was validated using a custom Python-based load-testing client capable of spawning hundreds of concurrent TCP connections. to simulate high-traffic environments. 

**Benchmark Results:**
* 200 concurrent TCP clients
* 100 broadcast messages per client
* 20,000 incoming messages
* ≈4 million message deliveries

During sustained load, the server successfully maintained protocol correctness, properly buffered partial writes via `EPOLLOUT`, and remained entirely responsive with zero deadlocks or dropped packets.


## Architecture Overview
The server operates on a classic **Reactor Pattern**. The main thread acts exclusively as the event loop, completely decoupled from application logic.

```text
           Main Thread (epoll_wait loop)
                        │
            Ready File Descriptors
                        │
                Shared Task Queue
                        │
     ┌──────────────────┼──────────────────┐
     │                  │                  │
  Worker 1           Worker 2           Worker 8
```

Worker threads wake up via condition variables, lock the required state utilizing fine-grained `std::mutex` synchronization, parse the binary packets, route the data, and re-arm the socket for the next event.

## Protocol & Stream Parsing
Because TCP provides a continuous byte stream rather than discrete messages, raw `read()` calls can return fragmented data. This server reconstructs payloads using a custom 6-byte header:

```text
+--------+--------------+----------------+
| Type   | Filename Len | Payload Length |
| 1 byte | 1 byte       | 4 bytes        |
+--------+--------------+----------------+
| Filename (optional, variable length)   |
+----------------------------------------+
| Payload (variable length)              |
+----------------------------------------+
```

The protocol is length-prefixed rather than delimiter-based, allowing arbitrary binary payloads without ambiguity.

| Type |      Meaning         |
|------|----------------------|
| 0    | Text Message         |
| 1    | Binary File Transfer |

Bytes are dynamically appended to a client's `std::vector<char>` receive buffer. Only when the buffer size meets the decoded `Payload Length` is the packet dispatched to the application layer.

## Getting Started

### Prerequisites
* Linux environment
* GCC (g++) supporting **C++17**
* POSIX Threads (`pthreads`)

### Build
Compile the server and client using the provided commands:
```bash
g++ -std=c++17 -Wall -Wextra -O3 -pthread -o server server.cpp
g++ -std=c++17 -Wall -Wextra -O3 -pthread -o client client.cpp
```

### Usage
1. Start the server in your primary terminal:
```bash
./server
```
2. Launch one or more clients in separate terminals:
```bash
./client
```

### Client Commands
| Command             |                  Description                           |
|---------------------|--------------------------------------------------------|
| `Hello!`            | Broadcasts a message to all connected users.           |
| `/nick <name>`      | Changes your display name.                             |
| `/msg <name> <msg>` | Sends a private, routed message to a specific user.    |
| `/send <filepath>`  | Uploads and broadcasts a binary file.                  |
| `Ctrl+C`            | Initiates a graceful shutdown of the client or server. |

## Future Improvements
* Unified streaming packet parser
* Dedicated chat rooms / channels
* User authentication
* TLS encryption
* Chunked large-file transfers for multi-gigabyte files
* WebSocket gateway for browser-based clients

## Lessons Learned
This project provided practical experience with:
- TCP as a byte stream rather than a message protocol
- Event-driven programming with epoll
- Designing binary application-layer protocols
- Synchronization between worker threads
- Handling partial reads and writes
- Building scalable network services without external networking libraries

## Acknowledgements
This project was designed and implemented independently to master the Linux Socket API, `epoll`, and POSIX Threads. AI tools were utilized strictly as a debugging and learning aid, analogous to consulting documentation or conducting code reviews.
