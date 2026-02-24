import requests
import random
from datetime import datetime
import os

NAMES = ["Alice", "Bob", "Charlie", "Diana"]

API_URL = f"http://{os.getenv('API_HOST', 'api')}:8000/orders"

def generate_order():
    return {
        "customer_name": random.choice(NAMES),
        "order_date": datetime.utcnow().isoformat()
    }

order = generate_order()

response = requests.post(API_URL, json=order)

if response.status_code == 202:
    data = response.json()
    print(f"[✔] Task accepted with ID: {data['task_id']}")
else:
    print(f"[✖] Error: {response.status_code} - {response.text}")