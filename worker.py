import pika
import os
import json
import redis
from datetime import datetime
from dotenv import load_dotenv

# Import pluggable task modules
# Modules encapsulate specific business logic (e.g., Unity compilation, Log analysis)
from modules import handling_json, handling_log

# This finds the exact folder worker.py is sitting in
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')

# Force dotenv to load from this specific file path
load_dotenv(dotenv_path=env_path)

# RabbitMQ Connection
credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USERNAME"), os.getenv("RABBITMQ_PASSWORD"))
connection_params = pika.ConnectionParameters(os.getenv("RABBITMQ_HOST"), int(os.getenv("RABBITMQ_PORT")), '/', credentials, heartbeat=3600, 
    blocked_connection_timeout=3600)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Redis Connection
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
    username=os.getenv("REDIS_USERNAME"),
    password=os.getenv("REDIS_PASSWORD"),
)

# Maps incoming routing keys to their specific execution modules.
# *** CHANGE THIS TO MATCH YOUR MODULES ***
ROUTING_MAP= { "process.json": handling_json,
                "process.log": handling_log }

def callback(ch, method, properties, body):
    """
    Core consumer loop. Validates messages, routes to modules, 
    manages Redis state, and handles fault tolerance.
    """
    message = {}
    try:
        # 1. Parse the incoming RabbitMQ ticket
        message = json.loads(body.decode())
        task_type = message.get("task_type")
        job_id = message.get("job_id")
        module = ROUTING_MAP.get(task_type)

        # 2. Validate the task type
        if not module:
            print(f"this task type: {task_type} is not supported")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        
        # 3. Execute the module logic (Passing 'ch' allows modules to publish chained events)
        module.run(message,ch)

        # 4. Update the centralized state ledger upon success
        if job_id:
            r.set((job_id), json.dumps({ "status": "completed", "task_type": task_type, "last_update": datetime.now().isoformat()}))
        # 5. Acknowledge success to RabbitMQ (deletes message from queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(e) 
        
        # Update ledger so the frontend client knows the job failed
        if job_id:
            r.set((job_id), json.dumps({ "status": "failed", 
                                        "error": str(e), 
                                        "task_type": task_type, 
                                        "last_update": datetime.now().isoformat()}))
            
            # NACK the message so it doesn't get stuck as a "zombie" in RabbitMQ memory
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        message = json.loads(body.decode())
        file_location = message.get("file_location")
        print(file_location)
        # If the file exists, wipe it from the hard drive
        if file_location and os.path.exists(file_location):
            os.remove(file_location)

# Read target queue from environment, allowing for horizontal scaling of specific task types
queue_name = os.getenv("TASK_QUEUE")
channel.queue_declare(queue=queue_name, durable=True)
channel.basic_consume(queue=queue_name, on_message_callback=callback)
channel.start_consuming()

