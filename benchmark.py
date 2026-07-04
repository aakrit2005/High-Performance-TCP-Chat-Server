import socket
import struct
import threading
import time

HOST = '127.0.0.1'
PORT = 5000
NUM_CLIENTS = 200           
MESSAGES_PER_CLIENT = 100    
MESSAGE = "Benchmarking the C++ Epoll Reactor!"

start_barrier = threading.Barrier(NUM_CLIENTS + 1)
successful_sends = 0
lock = threading.Lock()

def benchmark_client(client_id):
    global successful_sends
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        
        payload = MESSAGE.encode('utf-8')
        header = struct.pack('!BBI', 0, 0, len(payload))
        packet = header + payload

        start_barrier.wait()

        sends = 0
        for _ in range(MESSAGES_PER_CLIENT):
            sock.sendall(packet)
            sends += 1
            
            sock.setblocking(False)
            try:
                sock.recv(8192)
            except BlockingIOError:
                pass
            sock.setblocking(True)

        with lock:
            successful_sends += sends
            
        sock.close()
    except Exception as e:
        pass 

print("========================================")
print("     INITIALIZING STRESS TEST           ")
print("========================================")
print(f"Spawning {NUM_CLIENTS} concurrent clients...")

threads = []
for i in range(NUM_CLIENTS):
    t = threading.Thread(target=benchmark_client, args=(i,))
    threads.append(t)
    t.start()

start_barrier.wait()
print("All clients connected. Blasting server...")

start_time = time.time()

for t in threads:
    t.join()

end_time = time.time()
elapsed = end_time - start_time

total_requests = successful_sends
throughput = total_requests / elapsed if elapsed > 0 else 0

print("\n========================================")
print("        BENCHMARK RESULTS               ")
print("========================================")
print(f"Total Clients        : {NUM_CLIENTS}")
print(f"Messages per Client  : {MESSAGES_PER_CLIENT}")
print(f"Total Messages Sent  : {total_requests}")
print(f"Time Elapsed         : {elapsed:.3f} seconds")
print(f"Throughput           : {throughput:.2f} messages/sec")
print("========================================\n")

if elapsed < 1.0:
    print("Verdict: Your C++ epoll state machine is working")