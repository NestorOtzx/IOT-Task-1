import pika
import json
import os
import time
import psycopg2

PROCESSING_TIME = 50

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS tasks (
    task_id UUID PRIMARY KEY,
    status TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    task_id UUID,
    customer_name TEXT,
    order_date TEXT
);
""")

print("[✔] DB Ready")

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

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(
    exchange='tasks',
    queue=queue_name
)

print('[*] Worker waiting for tasks...')

def callback(ch, method, properties, body):

    task = json.loads(body)

    task_id = task["task_id"]
    order = task["payload"]

    print(f"[▶] Received Task: {task_id}")

    cursor.execute(
        "INSERT INTO tasks (task_id, status) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (task_id, "PENDING")
    )

    cursor.execute(
        "UPDATE tasks SET status=%s WHERE task_id=%s",
        ("PROCESSING", task_id)
    )

    print(f"[▶] Processing Task: {task_id}")
    time.sleep(PROCESSING_TIME)

    cursor.execute(
        """
        INSERT INTO orders (task_id, customer_name, order_date)
        VALUES (%s, %s, %s)
        """,
        (
            task_id,
            order["customer_name"],
            order["order_date"]
        )
    )

    cursor.execute(
        "UPDATE tasks SET status=%s WHERE task_id=%s",
        ("DONE", task_id)
    )

    print(f"[✔] Finished Task: {task_id}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback
)

channel.start_consuming()