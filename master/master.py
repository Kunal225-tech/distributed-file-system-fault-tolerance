# Master server for Distributed File System (DFS)
# Will handle metadata and coordination.

import socket
import threading
import time

HOST = '127.0.0.1'
PORT = 5000

active_nodes = {}

def handle_client(conn, addr):
    message = conn.recv(1024).decode()
    print(f"[MASTER] Received: {message} from {addr}")

    if message.startswith("REGISTER_NODE"):
        node_id = message.split(":")[1]
        active_nodes[node_id] = time.time()
        print(f"[MASTER] Node registered: {node_id}")
        conn.send("NODE_REGISTERED".encode())
    elif message == "HELLO":
        conn.send("WELCOME CLIENT".encode())
    elif message == "LIST_NODES":
        node_list = ",".join(active_nodes.keys())
        conn.send(node_list.encode() if node_list else "NO_NODES".encode())
    else:
        conn.send("UNKNOWN REQUEST".encode())

    conn.close()

def check_heartbeats():
    while True:
        current_time = time.time()
        for node_id, last_time in list(active_nodes.items()):
            if current_time - last_time > 10:
                print(f"[MASTER] Node lost: {node_id}")
                del active_nodes[node_id]
        time.sleep(5)

def start_master():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"[MASTER] Running on {HOST}:{PORT}")

    threading.Thread(target=check_heartbeats, daemon=True).start()

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()

