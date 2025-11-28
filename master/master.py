# master.py — DFS Master Server with REST API

import socket
import threading
import time
import os
from flask import Flask, request, jsonify, send_file

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
NODE_PORT_BASE = 6000
HEARTBEAT_TIMEOUT = 10
CHUNK_SIZE = 64 * 1024

active_nodes = {}
node_ports = {}
file_metadata = {}
lock = threading.Lock()

app = Flask(__name__)


# ===========================
#   HEARTBEAT MONITOR
# ===========================
def remove_dead_nodes():
    while True:
        time.sleep(2)
        with lock:
            now = time.time()
            for node, last_seen in list(active_nodes.items()):
                if now - last_seen > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] ❌ Node offline → Removing: {node}")
                    del active_nodes[node]
                    del node_ports[node]


def handle_node(conn):
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
                print(f"[MASTER] 🟢 Node Registered: {node_id}")

            else:
                node_id = data
                with lock:
                    active_nodes[node_id] = time.time()

        except:
            break
    conn.close()


# ===========================
#   REST API
# ===========================

@app.route("/api/nodes", methods=["GET"])
def get_nodes():
    return jsonify(list(active_nodes.keys()))


@app.route("/api/upload", methods=["POST"])
def upload_file():
    file = request.files["file"]
    filename = file.filename
    chunk_index = 0

    if not active_nodes:
        return jsonify({"msg": "No active nodes available"}), 500

    while True:
        chunk = file.stream.read(CHUNK_SIZE)
        if not chunk:
            break

        nodes_list = list(active_nodes.keys())
        primary = nodes_list[chunk_index % len(nodes_list)]
        send_chunk(primary, filename, chunk_index, chunk)

        file_metadata.setdefault(filename, []).append((primary, chunk_index))
        chunk_index += 1

    print(f"[MASTER] 📁 Uploaded file: {filename}")
    return jsonify({"msg": "File uploaded successfully"})


@app.route("/api/download/<filename>", methods=["GET"])
def download_file(filename):
    if filename not in file_metadata:
        return jsonify({"msg": "File not found"}), 404

    output_file = f"download_{filename}"
    with open(output_file, "wb") as f:
        for node_id, chunk_index in file_metadata[filename]:
            chunk = request_chunk(node_id, filename, chunk_index)
            f.write(chunk)

    return send_file(output_file, as_attachment=True)


def send_chunk(node_id, filename, chunk_index, chunk_data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, node_ports[node_id]))
    sock.send(f"{filename}||{chunk_index}||".encode() + chunk_data)
    sock.close()


def request_chunk(node_id, filename, chunk_index):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, node_ports[node_id]))
    sock.send(f"GET||{filename}||{chunk_index}".encode())
    data = sock.recv(CHUNK_SIZE)
    sock.close()
    return data


# ===========================
#   MASTER START
# ===========================
def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(5)
    print(f"[MASTER] Running on {MASTER_HOST}:{MASTER_PORT}")

    threading.Thread(target=remove_dead_nodes, daemon=True).start()

    threading.Thread(target=lambda: app.run(port=8000, debug=False), daemon=True).start()

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_node, args=(conn,)).start()


if __name__ == "__main__":
    start_master()



