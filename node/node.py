# Storage node for Distributed File System (DFS)
# Will store actual file data.

import socket
import time

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5000
NODE_ID = "node1"

def register_node():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MASTER_HOST, MASTER_PORT))
    s.send(f"REGISTER_NODE:{NODE_ID}".encode())
    print("[NODE] Register Response:", s.recv(1024).decode())
    s.close()

def send_heartbeat():
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((MASTER_HOST, MASTER_PORT))
            s.send(f"REGISTER_NODE:{NODE_ID}".encode())
        except:
            print("[NODE] Cannot reach master!")
        s.close()
        time.sleep(5)

if __name__ == "__main__":
    register_node()
    print("[NODE] Heartbeats started...")
    send_heartbeat()


