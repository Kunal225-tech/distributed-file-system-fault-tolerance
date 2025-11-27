# Client for Distributed File System (DFS)
# Code will be added step by step.

import socket

HOST = '127.0.0.1'
PORT = 5000

def list_nodes():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.send("LIST_NODES".encode())
    print("[CLIENT] Active Nodes:", s.recv(1024).decode())
    s.close()

if __name__ == "__main__":
    list_nodes()

