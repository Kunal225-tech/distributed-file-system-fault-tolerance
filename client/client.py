import requests

MASTER_URL = "http://127.0.0.1:5000"

def show_menu():
    print("\n===== DFS CLIENT MENU =====")
    print("1. Show Active Nodes")
    print("2. Upload File")
    print("3. Download File")
    print("4. Exit")
    return input("Enter your choice: ")

def show_nodes():
    try:
        res = requests.get(f"{MASTER_URL}/nodes")
        print("\nActive Nodes:", res.json())
    except:
        print("⚠ Could not connect to Master!")

def upload_file():
    filepath = input("Enter file path to upload: ")
    try:
        with open(filepath, "rb") as f:
            files = {"file": f}
            res = requests.post(f"{MASTER_URL}/upload", files=files)
            print(res.text)
    except:
        print("⚠ File not found!")

def download_file():
    filename = input("Enter filename to download: ")

    try:
        res = requests.get(f"{MASTER_URL}/download/{filename}")
        if res.status_code == 200:
            with open("output_" + filename, "wb") as f:
                f.write(res.content)
            print("✔ File downloaded successfully!")
        else:
            print("⚠ File not found in DFS!")
    except:
        print("⚠ Error contacting server!")

if __name__ == "__main__":
    while True:
        choice = show_menu()
        if choice == "1":
            show_nodes()
        elif choice == "2":
            upload_file()
        elif choice == "3":
            download_file()
        elif choice == "4":
            print("Bye!")
            break
        else:
            print("Invalid Input!")
