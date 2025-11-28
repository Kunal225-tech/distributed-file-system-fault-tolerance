# Master server for Distributed File System (DFS)
# Will handle metadata and coordination.

import socket
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
                    del active_nodes[node]
                    del node_ports[node]

def handle_node(conn, addr):
    while True:
        try:
            data = conn.recv(1024).decode()
            if not data:
                break

            parts = data.split("::")
            if parts[0] == "REGISTER":
                node_id = parts[1]
                port = int(parts[2])
                with lock:
                    active_nodes[node_id] = time.time()
                    node_ports[node_id] = port
                print(f"[MASTER] 🟢 Node Registered: {node_id} on port {port}")

            else:  # Heartbeat update
                node_id = data
                with lock:
                    active_nodes[node_id] = time.time()

        except:
            break
    conn.close()


def distribute_chunks(filename):
    print(f"[MASTER] Splitting file into chunks...")
    chunk_nodes = []
    chunk_index = 0

    with open(filename, "rb") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            assigned_nodes = list(active_nodes.keys())[:3]  # 3 nodes but replicate only on first 2

            # Store chunk metadata
            file_metadata.setdefault(filename, []).append((assigned_nodes[:2], chunk_index))

            # Send chunk to two nodes
            for i, node in enumerate(assigned_nodes[:2]):
                send_chunk(node, filename, chunk_index, chunk)

            chunk_index += 1
            chunk = f.read(CHUNK_SIZE)

    print(f"[MASTER] ✔ File distributed successfully.")


def send_chunk(node_id, filename, chunk_index, chunk_data):
    port = node_ports[node_id]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, port))
    data = f"{filename}||{chunk_index}||".encode() + chunk_data
    sock.send(data)
    sock.close()
    print(f"[MASTER] Sent chunk {chunk_index} to {node_id}")


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


