import pika
import os
import json
from modules import apk_handling

#RabbitMQ Connection
credentials = pika.PlainCredentials("guest", "guest")
connection_params = pika.ConnectionParameters("localhost", 5672, '/', credentials)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

ROUTING_MAP= { "process.apk": apk_handling }

def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    task_type = message.get("task_type")
    ROUTING_MAP.get(task_type).run(message)
    ch.basic_ack(delivery_tag=method.delivery_tag)

queue = os.getenv("TASK_QUEUE")
test = os.getenv("REDIS_PASSWORD")
channel.basic_consume(queue=os.getenv("TASK_QUEUE"), on_message_callback=callback)
channel.start_consuming()

