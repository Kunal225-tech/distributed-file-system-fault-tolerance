# Master server for Distributed File System (DFS)
# Will handle metadata and coordination.

import socket

HOST = '127.0.0.1'
PORT = 5000

def start_master():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"[MASTER] Running on {HOST}:{PORT}")

    while True:
        conn, addr = server_socket.accept()
        print(f"[MASTER] Connected with {addr}")

        data = conn.recv(1024).decode()
        print(f"[MASTER] Received: {data}")

        if data == "HELLO":
            conn.send("WELCOME CLIENT".encode())
        else:
            conn.send("UNKNOWN REQUEST".encode())

        conn.close()

if __name__ == "__main__":
    start_master()

