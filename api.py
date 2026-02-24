from fastapi import FastAPI
from pydantic import BaseModel
import pika
import json
import os

app = FastAPI()

class Order(BaseModel):
    customer_name: str
    order_date: str

def publish_to_rabbitmq(message: dict):
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
        exchange='logs',
        exchange_type='fanout'
    )

    channel.basic_publish(
        exchange='logs',
        routing_key='',
        body=json.dumps(message)
    )

    connection.close()

@app.post("/orders")
def receive_order(order: Order):

    order_dict = order.dict()

    print(f"[✔] Order received by API: {order_dict}")

    publish_to_rabbitmq(order_dict)

    return {
        "status": "Order received and published to RabbitMQ"
    }