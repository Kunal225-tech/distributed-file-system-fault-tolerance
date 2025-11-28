# client/client.py
# Simple DFS Client: asks master for list of active nodes.

import socket

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000


def ask_list_nodes():
    """Send LIST_NODES to master and print result."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(b"LIST_NODES")
        data = s.recv(1024).decode()
        s.close()

        if data.startswith("NODES::"):
            nodes_str = data.split("::", 1)[1]
            if nodes_str.strip() == "":
                print("No active nodes right now.")
            else:
                nodes = nodes_str.split(",")
                print("Active nodes:")
                for n in nodes:
                    print("  -", n)
        else:
            print("Unexpected response from master:", data)
    except Exception as e:
        print("Could not contact master:", e)


def main():
    print("===== DFS CLIENT =====")
    print("1. Show Active Nodes")
    print("2. Exit")

    while True:
        choice = input("Enter your choice: ").strip()
        if choice == "1":
            ask_list_nodes()
        elif choice == "2":
            print("Bye!")
            break
        else:
            print("Invalid option.")


if __name__ == "__main__":
    main()


