# Master server for Distributed File System (DFS)
# Handles metadata, node tracking, and chunk distribution

# master/master.py
# Simple DFS Master: tracks nodes and heartbeats, answers LIST_NODES.

import socket
import threading
import time

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
HEARTBEAT_TIMEOUT = 10  # seconds

# node_id -> last_heartbeat_time
active_nodes = {}
lock = threading.Lock()


def remove_dead_nodes():
    """Background thread: remove nodes that stopped sending heartbeats."""
    while True:
        time.sleep(2)
        now = time.time()
        with lock:
            for node_id, last_seen in list(active_nodes.items()):
                if now - last_seen > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] ❌ Node timeout → Removing: {node_id}")
                    del active_nodes[node_id]


def handle_client(conn, addr):
    """Handle any incoming message (from node or client)."""
    try:
        data = conn.recv(1024).decode().strip()
        if not data:
            return

        # Node registration: REGISTER::node1
        if data.startswith("REGISTER::"):
            node_id = data.split("::", 1)[1]
            with lock:
                active_nodes[node_id] = time.time()
            print(f"[MASTER] 🟢 Node registered: {node_id}")

        # Node heartbeat: HEARTBEAT::node1
        elif data.startswith("HEARTBEAT::"):
            node_id = data.split("::", 1)[1]
            with lock:
                if node_id in active_nodes:
                    active_nodes[node_id] = time.time()
            # no print every heartbeat to keep output clean

        # Client request: LIST_NODES
        elif data == "LIST_NODES":
            with lock:
                nodes_list = ",".join(active_nodes.keys())
            response = f"NODES::{nodes_list}"
            conn.send(response.encode())

        # Unknown message → ignore
        else:
            print(f"[MASTER] Unknown message from {addr}: {data}")

    finally:
        conn.close()


def start_master():
    """Start the master TCP server."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(5)
    print(f"[MASTER] Running on {MASTER_HOST}:{MASTER_PORT}")

    # Start timeout cleaner thread
    threading.Thread(target=remove_dead_nodes, daemon=True).start()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    start_master()


