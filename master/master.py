# Master server for Distributed File System (DFS)
# Handles metadata, node tracking, and chunk distribution

import socket
import threading
import time
import os

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
HEARTBEAT_TIMEOUT = 12  # seconds

active_nodes = {}      # node_id → last heartbeat time
node_ports = {}        # node_id → chunk server port
file_metadata = {}     # filename → [(node, chunk_index)]

CHUNK_SIZE = 64 * 1024  # 64 KB

lock = threading.Lock()


# 🗑 Remove dead nodes when heartbeat stops
def remove_dead_nodes():
    while True:
        time.sleep(2)
        with lock:
            now = time.time()
            for node, last_seen in list(active_nodes.items()):
                if now - last_seen > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] ❌ Node timeout → Removing: {node}")
                    del active_nodes[node]
                    if node in node_ports:
                        del node_ports[node]


# 📨 Handle connections from nodes
def handle_node(conn, addr):
    while True:
        try:
            data = conn.recv(1024).decode()
            if not data:
                break

            parts = data.split("::")

            # When a node registers first time
            if parts[0] == "REGISTER":
                node_id = parts[1]
                port = int(parts[2])

                with lock:
                    active_nodes[node_id] = time.time()
                    node_ports[node_id] = port

                print(f"[MASTER] 🟢 Node Registered: {node_id} on port {port}")

            else:
                node_id = data.strip()

                # Accept only real node names (avoid removing LIST_NODES)
                if node_id.startswith("node"):
                    with lock:
                        active_nodes[node_id] = time.time()

        except:
            break

    conn.close()


# 📦 Break file into chunks and send to nodes
def distribute_chunks(filename):
    print(f"[MASTER] Splitting and distributing file: {filename}")

    chunk_index = 0
    file_metadata[filename] = []

    with open(filename, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break

            # Pick first 2 nodes for replication
            selected_nodes = list(active_nodes.keys())[:2]
            file_metadata[filename].append((selected_nodes, chunk_index))

            for node in selected_nodes:
                send_chunk(node, filename, chunk_index, chunk)

            chunk_index += 1

    print(f"[MASTER] ✔ File distribution completed!")


# 📤 Send chunk to node chunk server
def send_chunk(node_id, filename, chunk_index, chunk_data):
    try:
        port = node_ports[node_id]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((MASTER_HOST, port))

        header = f"{filename}||{chunk_index}||".encode()
        sock.send(header + chunk_data)
        sock.close()

        print(f"[MASTER] Sent chunk {chunk_index} to {node_id}")

    except:
        print(f"[MASTER] ⚠ Failed to send chunk {chunk_index} to {node_id}")


# ▶ Start Master Server
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



