# master/master.py
# Distributed File System Master
# - Registers nodes
# - Tracks heartbeats
# - Stores file metadata (which chunk is on which node)
# - Handles client commands: list nodes, upload, download

import socket
import threading
import time

MASTER_HOST = "127.0.0.1"
NODE_PORT = 5000        # Nodes yaha register + heartbeat bhejenge
CLIENT_PORT = 6000      # Client yaha commands bhejega

HEARTBEAT_TIMEOUT = 10  # seconds
CHUNK_SIZE = 64 * 1024  # 64 KB
REPLICATION_FACTOR = 2  # Har chunk ki kitni copies

# node_id -> {"last_seen": t, "port": chunk_server_port}
active_nodes = {}
# filename -> { chunk_index: [node_id1, node_id2, ...] }
file_metadata = {}

lock = threading.Lock()


# ===============================
#  HEARTBEAT CLEANER
# ===============================
def remove_dead_nodes():
    while True:
        time.sleep(2)
        with lock:
            now = time.time()
            for node_id, info in list(active_nodes.items()):
                if now - info["last_seen"] > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] ❌ Node timeout → Removing: {node_id}")
                    del active_nodes[node_id]


# ===============================
#  NODE CONNECTION HANDLER
# ===============================
def handle_node(conn, addr):
    """
    Protocol from node:
    1) "REGISTER::node1::6000"
    2) Then periodic "HEARTBEAT::node1"
    """
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            msg = data.decode().strip()
            parts = msg.split("::")

            if parts[0] == "REGISTER":
                node_id = parts[1]
                chunk_port = int(parts[2])
                with lock:
                    active_nodes[node_id] = {
                        "last_seen": time.time(),
                        "port": chunk_port
                    }
                print(f"[MASTER] 🟢 Node registered: {node_id} on port {chunk_port}")

            elif parts[0] == "HEARTBEAT":
                node_id = parts[1]
                with lock:
                    if node_id in active_nodes:
                        active_nodes[node_id]["last_seen"] = time.time()
            else:
                print(f"[MASTER] Unknown message from node: {msg}")
    finally:
        conn.close()


def start_node_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, NODE_PORT))
    server.listen(5)
    print(f"[MASTER] Node listener on {MASTER_HOST}:{NODE_PORT}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_node, args=(conn, addr), daemon=True).start()


# ===============================
#  CLIENT COMMAND HANDLER
# ===============================
def handle_client(conn, addr):
    """
    Protocol from client (first line - header terminated by '\n'):
        LIST_NODES
        UPLOAD::filename::size\n<file_bytes>
        DOWNLOAD::filename\n  -> server responds: SIZE::n\n<file_bytes>
    """
    try:
        # read header line
        header_bytes = b""
        while b"\n" not in header_bytes:
            chunk = conn.recv(1024)
            if not chunk:
                break
            header_bytes += chunk

        if not header_bytes:
            return

        header, remaining = header_bytes.split(b"\n", 1)
        header = header.decode().strip()
        parts = header.split("::")
        cmd = parts[0]

        if cmd == "LIST_NODES":
            with lock:
                nodes_list = list(active_nodes.keys())
            response = "NODES::" + ",".join(nodes_list)
            conn.sendall(response.encode())

        elif cmd == "UPLOAD":
            filename = parts[1]
            total_size = int(parts[2])

            # Collect full file bytes
            file_bytes = remaining
            while len(file_bytes) < total_size:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                file_bytes += chunk

            if len(file_bytes) != total_size:
                conn.sendall(b"ERROR::SIZE_MISMATCH")
                return

            print(f"[MASTER] 📁 Upload request for {filename}, size={total_size} bytes")
            ok = process_upload(filename, file_bytes)
            if ok:
                conn.sendall(b"OK")
            else:
                conn.sendall(b"ERROR::NO_NODES")

        elif cmd == "DOWNLOAD":
            filename = parts[1]
            if filename not in file_metadata:
                conn.sendall(b"ERROR::NO_SUCH_FILE")
                return

            file_bytes = process_download(filename)
            header = f"SIZE::{len(file_bytes)}\n".encode()
            conn.sendall(header + file_bytes)

        else:
            conn.sendall(b"ERROR::UNKNOWN_COMMAND")

    finally:
        conn.close()


def start_client_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((MASTER_HOST, CLIENT_PORT))
    server.listen(5)
    print(f"[MASTER] Client listener on {MASTER_HOST}:{CLIENT_PORT}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ===============================
#  FILE UPLOAD / DOWNLOAD LOGIC
# ===============================
def choose_nodes_for_chunk(chunk_index):
    with lock:
        node_ids = list(active_nodes.keys())
    if not node_ids:
        return []

    chosen = []
    for i in range(REPLICATION_FACTOR):
        node_id = node_ids[(chunk_index + i) % len(node_ids)]
        if node_id not in chosen:
            chosen.append(node_id)
        if len(chosen) == len(node_ids):
            break
    return chosen


def send_chunk_to_node(node_id, filename, chunk_index, chunk_bytes):
    info = active_nodes[node_id]
    port = info["port"]

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, port))

    header = f"STORE||{filename}||{chunk_index}||{len(chunk_bytes)}\n".encode()
    sock.sendall(header + chunk_bytes)

    ack = sock.recv(64)
    sock.close()

    if ack.startswith(b"OK"):
        print(f"[MASTER] Sent chunk {chunk_index} ({len(chunk_bytes)} B) to {node_id}")
        return True
    else:
        print(f"[MASTER] Failed to store chunk {chunk_index} on {node_id}")
        return False


def request_chunk_from_node(node_id, filename, chunk_index):
    info = active_nodes[node_id]
    port = info["port"]

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, port))

    header = f"GET||{filename}||{chunk_index}\n".encode()
    sock.sendall(header)

    # first line: SIZE::<n>\n
    header_bytes = b""
    while b"\n" not in header_bytes:
        chunk = sock.recv(64)
        if not chunk:
            break
        header_bytes += chunk
    if not header_bytes:
        sock.close()
        raise IOError("No header from node")

    header_line, rest = header_bytes.split(b"\n", 1)
    parts = header_line.decode().split("::")
    if parts[0] != "SIZE":
        sock.close()
        raise IOError("Bad header from node")

    total_size = int(parts[1])
    data = rest
    while len(data) < total_size:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk

    sock.close()
    if len(data) != total_size:
        raise IOError("Incomplete chunk from node")
    return data


def process_upload(filename, file_bytes):
    with lock:
        if not active_nodes:
            print("[MASTER] ❌ No active nodes, cannot upload.")
            return False

    num_chunks = 0
    file_metadata[filename] = {}

    offset = 0
    chunk_index = 0
    while offset < len(file_bytes):
        chunk = file_bytes[offset:offset + CHUNK_SIZE]
        offset += CHUNK_SIZE

        target_nodes = choose_nodes_for_chunk(chunk_index)
        if not target_nodes:
            print("[MASTER] ❌ No nodes available for this chunk.")
            return False

        file_metadata[filename][chunk_index] = []

        for nid in target_nodes:
            try:
                if send_chunk_to_node(nid, filename, chunk_index, chunk):
                    file_metadata[filename][chunk_index].append(nid)
            except Exception as e:
                print(f"[MASTER] Error sending chunk to {nid}: {e}")

        num_chunks += 1
        chunk_index += 1

    print(f"[MASTER] ✅ File {filename} stored in {num_chunks} chunks.")
    return True


def process_download(filename):
    print(f"[MASTER] 📥 Download request for {filename}")
    chunks_info = file_metadata[filename]
    result_bytes = b""

    for chunk_index in sorted(chunks_info.keys()):
        nodes_for_chunk = chunks_info[chunk_index]
        chunk_data = None
        for nid in nodes_for_chunk:
            try:
                chunk_data = request_chunk_from_node(nid, filename, chunk_index)
                break
            except Exception as e:
                print(f"[MASTER] Failed chunk {chunk_index} from {nid}: {e}")
        if chunk_data is None:
            raise IOError(f"Chunk {chunk_index} missing on all replicas.")
        result_bytes += chunk_data

    print(f"[MASTER] ✅ Reconstructed file {filename} ({len(result_bytes)} bytes).")
    return result_bytes


# ===============================
#  MAIN
# ===============================
def main():
    print("[MASTER] Starting DFS Master...")
    threading.Thread(target=remove_dead_nodes, daemon=True).start()
    threading.Thread(target=start_node_listener, daemon=True).start()
    threading.Thread(target=start_client_listener, daemon=True).start()

    # keep main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()




