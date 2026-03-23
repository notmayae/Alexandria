import pika
import os
import json
import redis
from modules import handling_json, handling_log
from datetime import datetime

#RabbitMQ Connection
credentials = pika.PlainCredentials("guest", "guest")
connection_params = pika.ConnectionParameters("localhost", 5672, '/', credentials)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

#Redis Connection
r = redis.Redis(
    host='redis-11633.crce288.eu-central-1-1.ec2.cloud.redislabs.com',
    port=11633,
    decode_responses=True,
    username="default",
    password=os.getenv("REDIS_PASSWORD"),
)

ROUTING_MAP= { "process.json": handling_json,
                "process.log": handling_log }

def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        task_type = message.get("task_type")
        job_id = message.get("job_id")
        module = ROUTING_MAP.get(task_type)
        if not module:
            print(f"this task type: {task_type} is not supported")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        module.run(message,ch)
        if job_id:
            r.set((job_id), json.dumps({ "status": "completed", "task_type": task_type, "last_update": datetime.now().isoformat()}))

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(e)
        if job_id:
            r.set((job_id), json.dumps({ "status": "failed", 
                                        "error": str(e), 
                                        "task_type": task_type, 
                                        "last_update": datetime.now().isoformat()}))
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


queue_name = "log_analyzing"
channel.queue_declare(queue=queue_name, durable=True)
channel.basic_consume(queue=queue_name, on_message_callback=callback)
channel.start_consuming()

