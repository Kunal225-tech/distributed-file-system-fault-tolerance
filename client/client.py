# client/client.py
# DFS Client
# - Talk to master on CLIENT_PORT
# - Show active nodes
# - Upload file (master splits + replicates)
# - Download file (master reconstructs)

import socket
import os

MASTER_HOST = "127.0.0.1"
CLIENT_PORT = 6000


def send_command(header, payload=b""):
    """
    header: string (without '\n')
    payload: bytes
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_HOST, CLIENT_PORT))
    message = header.encode() + b"\n" + payload
    sock.sendall(message)
    return sock


def list_nodes():
    sock = send_command("LIST_NODES")
    data = sock.recv(1024).decode()
    sock.close()

    if not data.startswith("NODES::"):
        print("Error from master:", data)
        return

    nodes_str = data.split("::", 1)[1]
    if not nodes_str:
        print("No active nodes.")
        return

    nodes = nodes_str.split(",")
    print("Active nodes:")
    for n in nodes:
        print(" -", n)


def upload_file():
    path = input("Enter path of file to upload: ").strip()
    if not os.path.exists(path):
        print("File not found.")
        return

    filename = os.path.basename(path)
    with open(path, "rb") as f:
        file_bytes = f.read()

    header = f"UPLOAD::{filename}::{len(file_bytes)}"
    sock = send_command(header, file_bytes)
    resp = sock.recv(1024).decode()
    sock.close()

    if resp == "OK":
        print(f"✅ File '{filename}' uploaded successfully.")
    else:
        print("Error from master:", resp)


def download_file():
    filename = input("Enter filename to download: ").strip()
    header = f"DOWNLOAD::{filename}"
    sock = send_command(header)

    # first line = SIZE::n\n or ERROR::
    header_bytes = b""
    while b"\n" not in header_bytes:
        chunk = sock.recv(1024)
        if not chunk:
            break
        header_bytes += chunk

    if not header_bytes:
        print("No response from master.")
        sock.close()
        return

    line, rest = header_bytes.split(b"\n", 1)
    line = line.decode()

    if line.startswith("ERROR::"):
        print("Master error:", line)
        sock.close()
        return

    parts = line.split("::")
    if parts[0] != "SIZE":
        print("Bad response from master:", line)
        sock.close()
        return

    total_size = int(parts[1])
    data = rest
    while len(data) < total_size:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk

    sock.close()
    if len(data) != total_size:
        print("Download incomplete!")
        return

    out_name = "downloaded_" + filename
    with open(out_name, "wb") as f:
        f.write(data)

    print(f"✅ Downloaded file saved as {out_name} ({len(data)} bytes).")


def main_menu():
    while True:
        print("\n===== DFS CLIENT MENU =====")
        print("1. Show Active Nodes")
        print("2. Upload File")
        print("3. Download File")
        print("4. Exit")
        choice = input("Enter your choice: ").strip()

        if choice == "1":
            list_nodes()
        elif choice == "2":
            upload_file()
        elif choice == "3":
            download_file()
        elif choice == "4":
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main_menu()

