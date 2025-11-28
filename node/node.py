# node/node.py
# Simple DFS Node: registers with master and sends heartbeats.

import socket
import threading
import time

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
HEARTBEAT_INTERVAL = 3  # seconds


def register_node(node_id):
    """Send REGISTER message to master once."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(f"REGISTER::{node_id}".encode())
        s.close()
        print(f"[{node_id}] Registered with master.")
    except Exception as e:
        print(f"[{node_id}] Failed to register with master: {e}")


def heartbeat_loop(node_id):
    """Send heartbeats to master periodically."""
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            s.send(f"HEARTBEAT::{node_id}".encode())
            s.close()
        except:
            pass
        time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    node_id = input("Enter Node Name (e.g., node1): ").strip()

    # 1) register once
    register_node(node_id)

    # 2) start heartbeat thread
    threading.Thread(target=heartbeat_loop, args=(node_id,), daemon=True).start()

    print(f"[{node_id}] Heartbeat started. Press Ctrl+C to stop this node.")

    # Keep the process alive forever
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Shutting down.")

