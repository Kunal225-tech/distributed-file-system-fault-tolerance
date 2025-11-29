# node/node.py
# DFS Node
# - Master ko register karega
# - Chunk server run karega (STORE / GET)
# - Heartbeat bhejega

import socket
import threading
import time
import os

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000

BASE_CHUNK_PORT = 7000   # node chunk server yaha se ports lega
HEARTBEAT_INTERVAL = 3   # seconds

storage_folder = "chunks"


def ensure_storage_folder():
    if not os.path.exists(storage_folder):
        os.makedirs(storage_folder)


def register_with_master(node_id, chunk_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, MASTER_PORT))
    msg = f"REGISTER::{node_id}::{chunk_port}"
    sock.sendall(msg.encode())
    sock.close()


def heartbeat_loop(node_id):
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((MASTER_HOST, MASTER_PORT))
            msg = f"HEARTBEAT::{node_id}"
            sock.sendall(msg.encode())
            sock.close()
        except Exception as e:
            # Master might be down temporarily
            pass
        time.sleep(HEARTBEAT_INTERVAL)


def handle_master_request(conn, addr):
    """
    Protocol from master:
      STORE||filename||chunk_index||size\n<bytes>
      GET||filename||chunk_index\n
    """
    try:
        header_bytes = b""
        while b"\n" not in header_bytes:
            chunk = conn.recv(1024)
            if not chunk:
                break
            header_bytes += chunk

        if not header_bytes:
            return

        header, remaining = header_bytes.split(b"\n", 1)
        header = header.decode()
        parts = header.split("||")
        cmd = parts[0]

        if cmd == "STORE":
            filename = parts[1]
            chunk_index = int(parts[2])
            total_size = int(parts[3])

            data = remaining
            while len(data) < total_size:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk

            ensure_storage_folder()
            path = os.path.join(storage_folder, f"{filename}.chunk{chunk_index}")
            with open(path, "wb") as f:
                f.write(data)

            print(f"[NODE] Stored {path} ({len(data)} bytes)")
            conn.sendall(b"OK")

        elif cmd == "GET":
            filename = parts[1]
            chunk_index = int(parts[2])
            path = os.path.join(storage_folder, f"{filename}.chunk{chunk_index}")
            if not os.path.exists(path):
                conn.sendall(b"SIZE::0\n")
                return

            with open(path, "rb") as f:
                data = f.read()

            header = f"SIZE::{len(data)}\n".encode()
            conn.sendall(header + data)
            print(f"[NODE] Sent {path} ({len(data)} bytes)")

        else:
            print(f"[NODE] Unknown command from master: {header}")
    finally:
        conn.close()


def start_chunk_server(chunk_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, chunk_port))
    server.listen(5)
    print(f"[NODE] Chunk server listening on {MASTER_HOST}:{chunk_port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_master_request, args=(conn, addr), daemon=True).start()


def main():
    node_id = input("Enter node name (e.g., node1): ").strip()
    port_offset = int(input("Enter port offset (e.g., 0 for 7000, 1 for 7001): "))
    chunk_port = BASE_CHUNK_PORT + port_offset

    ensure_storage_folder()
    register_with_master(node_id, chunk_port)
    print(f"[NODE] Registered as {node_id}, chunk port {chunk_port}")

    threading.Thread(target=heartbeat_loop, args=(node_id,), daemon=True).start()
    start_chunk_server(chunk_port)


if __name__ == "__main__":
    main()

