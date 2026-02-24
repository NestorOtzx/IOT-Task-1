from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
import pika
import json
import os
import uuid
import psycopg2

app = FastAPI()

# ---------- DB CONNECTION ----------

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True
cursor = conn.cursor()

# ---------- MODELS ----------

class Order(BaseModel):
    customer_name: str
    order_date: str

class Task(BaseModel):
    task_id: str
    payload: Order

# ---------- RABBITMQ ----------

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

# ---------- POST ORDER ----------

@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
def receive_order(order: Order):

    task_id = str(uuid.uuid4())

    task = Task(
        task_id=task_id,
        payload=order
    )

    publish_to_rabbitmq(task.dict())

    return {
        "task_id": task_id,
        "status": "accepted"
    }

# ---------- GET TASK BY ID ----------

@app.get("/tasks/{task_id}")
def get_task(task_id: str):

    cursor.execute(
        "SELECT task_id, status FROM tasks WHERE task_id=%s",
        (task_id,)
    )

    task = cursor.fetchone()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": task[0],
        "status": task[1]
    }

# ---------- GET ALL TASKS ----------

@app.get("/tasks")
def get_tasks():

    cursor.execute("SELECT task_id, status FROM tasks")

    tasks = cursor.fetchall()

    return [
        {"task_id": t[0], "status": t[1]}
        for t in tasks
    ]

# ---------- GET ALL ORDERS ----------

@app.get("/orders")
def get_orders():

    cursor.execute("""
        SELECT task_id, customer_name, order_date
        FROM orders
    """)

    orders = cursor.fetchall()

    return [
        {
            "task_id": o[0],
            "customer_name": o[1],
            "order_date": o[2]
        }
        for o in orders
    ]