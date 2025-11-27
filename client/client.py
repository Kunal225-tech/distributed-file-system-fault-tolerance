# Client for Distributed File System (DFS)
# Code will be added step by step.

import socket

HOST = '127.0.0.1'
PORT = 5000

def send_hello():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((HOST, PORT))

    client_socket.send("HELLO".encode())
    reply = client_socket.recv(1024).decode()

    print("[CLIENT] Server says:", reply)
    client_socket.close()

if __name__ == "__main__":
    send_hello()

