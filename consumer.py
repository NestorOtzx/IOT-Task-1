import pika
import json
import os
import time

PROCESSING_TIME = 5

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

    print(f"[▶] Started processing Task: {task_id}")
    print(f"     Order data: {order}")

    time.sleep(PROCESSING_TIME)

    print(f"[✔] Finished processing Task: {task_id}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback
)

channel.start_consuming()