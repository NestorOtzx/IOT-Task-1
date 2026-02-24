import requests
import os

API_HOST = os.getenv("API_HOST", "api")
BASE_URL = f"http://{API_HOST}:8000"

def get_task():
    task_id = input("Enter Task ID: ")
    response = requests.get(f"{BASE_URL}/tasks/{task_id}")

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")

def get_tasks():
    response = requests.get(f"{BASE_URL}/tasks")
    print(response.json())

def get_orders():
    response = requests.get(f"{BASE_URL}/orders")
    print(response.json())

def menu():

    while True:

        print("\n===== CLIENT MENU =====")
        print("1. Get Task by ID")
        print("2. Get All Tasks")
        print("3. Get All Orders")
        print("4. Exit")

        option = input("Select option: ")

        if option == "1":
            get_task()
        elif option == "2":
            get_tasks()
        elif option == "3":
            get_orders()
        elif option == "4":
            break
        else:
            print("Invalid option")

if __name__ == "__main__":
    menu()