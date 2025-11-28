# Node server for Distributed File System (DFS)
# Stores file chunks sent by master

import socket
import threading
import os
import time

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000

HEARTBEAT_INTERVAL = 3
BUFFER_SIZE = 1024 * 64  # 64 KB

# Create folder for chunk storage
if not os.path.exists("chunks"):
    os.makedirs("chunks")

def send_heartbeat(node_id):
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            s.send(node_id.encode())
            s.close()
        except:
            pass
        time.sleep(HEARTBEAT_INTERVAL)

def handle_chunk(conn):
    data = conn.recv(BUFFER_SIZE)

    if not data:
        return

    parts = data.split(b"||", 2)
    filename = parts[0].decode()
    chunk_index = int(parts[1].decode())
    chunk_data = parts[2]

    chunk_path = os.path.join("chunks", f"{filename}_chunk{chunk_index}")

    with open(chunk_path, "wb") as f:
        f.write(chunk_data)

    print(f"[{NODE_ID}] Stored chunk {chunk_index} for file '{filename}'")

def start_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, port))
    server.listen(5)
    print(f"[{NODE_ID}] Chunk Server Running on port {port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_chunk, args=(conn,)).start()


if __name__ == "__main__":
    NODE_ID = input("Enter Node Name (Example: node1): ")
    NODE_PORT = int(input("Enter chunk port (Example: 6000): "))

    # Register node to master
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MASTER_HOST, MASTER_PORT))
    s.send(f"REGISTER::{NODE_ID}::{NODE_PORT}".encode())
    s.close()

    # Start heartbeat thread
    threading.Thread(target=send_heartbeat, args=(NODE_ID,), daemon=True).start()

    # Start chunk server
    start_server(NODE_PORT)
