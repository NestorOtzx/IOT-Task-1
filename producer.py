#producer

import requests
import random
from datetime import datetime

NAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank"
]

def generate_order():
    return {
        "customer_name": random.choice(NAMES),
        "order_date": datetime.utcnow().isoformat()
    }

def send_order(order):
    api_url = "http://api:8000/orders"

    response = requests.post(api_url, json=order)

    if response.status_code == 200 or response.status_code == 201:
        print(f"[✔] Order sent successfully: {order}")
    else:
        print(f"[✖] Failed to send order: {response.status_code} - {response.text}")

if __name__ == "__main__":
    order = generate_order()
    send_order(order)