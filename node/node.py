# Storage node for Distributed File System (DFS)
# Will store actual file data.

import socket
import threading
import os
import time

NODE_ID = input("Enter Node Name (Example: node1): ")

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
NODE_PORT = 6000 if NODE_ID == "node1" else (6001 if NODE_ID == "node2" else 6002)

os.makedirs("node_storage", exist_ok=True)

def heartbeat():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, MASTER_PORT))
    while True:
        sock.send(NODE_ID.encode())
        time.sleep(3)

def chunk_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", NODE_PORT))
    server.listen(5)
    print(f"[{NODE_ID}] Chunk Server Running on port {NODE_PORT}")

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024).decode()
        filename, chunk_index, chunk_data = data.split("||")
        path = f"node_storage/{filename}_chunk{chunk_index}"
        with open(path, "w") as f:
            f.write(chunk_data)
        print(f"[{NODE_ID}] Stored {path}")
        conn.send("OK".encode())
        conn.close()

threading.Thread(target=heartbeat, daemon=True).start()
threading.Thread(target=chunk_server).start()


