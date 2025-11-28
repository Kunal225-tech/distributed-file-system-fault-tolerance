# Master server for Distributed File System (DFS)
# Will handle metadata and coordination.

# Master server for Distributed File System (DFS)

import socket
import threading
import time
import os

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
HEARTBEAT_TIMEOUT = 10

active_nodes = {}
node_ports = {}
file_metadata = {}  # filename → [(node, chunk_index)]

CHUNK_SIZE = 64 * 1024  # 64 KB
lock = threading.Lock()


def remove_dead_nodes():
    while True:
        time.sleep(2)
        with lock:
            now = time.time()
            for node, last_seen in list(active_nodes.items()):
                if now - last_seen > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] ❌ Node timeout → Removing: {node}")
                    active_nodes.pop(node, None)
                    node_ports.pop(node, None)


def handle_node(conn, addr):
    while True:
        try:
            data = conn.recv(1024).decode()
            if not data:
                break

            parts = data.split("::")

            # REGISTER message
            if parts[0] == "REGISTER":
                node_id = parts[1]
                port = int(parts[2])
                with lock:
                    active_nodes[node_id] = time.time()
                    node_ports[node_id] = port
                print(f"[MASTER] 🟢 Node Registered: {node_id} (Port: {port})")

            # NODE FILE CHUNK message
            elif parts[0] == "FILE_CHUNK":
                filename = parts[1]
                chunk_index = int(parts[2])
                node_id = parts[3]
                file_metadata.setdefault(filename, []).append((node_id, chunk_index))
                print(f"[MASTER] Stored metadata: {filename}, chunk {chunk_index} on {node_id}")

            # Heartbeat
            else:
                node_id = data
                with lock:
                    active_nodes[node_id] = time.time()

        except:
            break
    conn.close()


def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(5)
    print(f"[MASTER] Running on {MASTER_HOST}:{MASTER_PORT}")

    threading.Thread(target=remove_dead_nodes, daemon=True).start()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_node, args=(conn, addr)).start()


if __name__ == "__main__":
    start_master()


