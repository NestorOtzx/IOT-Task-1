from fastapi import FastAPI, status
from pydantic import BaseModel
import pika
import json
import os
import uuid

app = FastAPI()

class Order(BaseModel):
    customer_name: str
    order_date: str

class Task(BaseModel):
    task_id: str
    payload: Order

def publish_to_rabbitmq(task: dict):

    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASSWORD")
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            credentials=credentials
        )
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange='tasks',
        exchange_type='fanout'
    )

    channel.basic_publish(
        exchange='tasks',
        routing_key='',
        body=json.dumps(task)
    )

    connection.close()

@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
def receive_order(order: Order):

    task_id = str(uuid.uuid4())

    task = Task(
        task_id=task_id,
        payload=order
    )

    task_dict = task.dict()

    print(f"[✔] Order received, Task created: {task_dict}")

    publish_to_rabbitmq(task_dict)

    return {
        "task_id": task_id,
        "status": "accepted"
    }