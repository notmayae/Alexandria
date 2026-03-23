from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
import uuid
import redis
from datetime import datetime
import json
import pika
import os
import shutil
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

# Redis Connection
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
    username=os.getenv("REDIS_USERNAME"),
    password=os.getenv("REDIS_PASSWORD"),
)

# RabbitMQ Connection
credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USERNAME"), os.getenv("RABBITMQ_PASSWORD"))
connection_params = pika.ConnectionParameters(os.getenv("RABBITMQ_HOST"), int(os.getenv("RABBITMQ_PORT")), '/', credentials)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Routing Map by file type
# *** CHANGE THIS TO MATCH YOUR RABBITMQ BINDING ***
ROUTING_MAP = {
    "application/json": "process.json",
    "application/vnd.android.package-archive": "process.apk",
    "application/zip": "build.unity", 
    "text/plain": "process.logs"
}

@app.post("/api/jobs")
async def createJob(upload_file: UploadFile = File(...)):

    file_type = upload_file.content_type
    routing_key = ROUTING_MAP.get(file_type, "process.unassigned")

    if routing_key == "process.unassigned":
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_type}")
    
    job_id = str(uuid.uuid4()) # Generate unique id to each job for state tracking

    # Initialize the job state in the Redis Ledger
    r.set((job_id), json.dumps({ "status": "processing", "task_type": routing_key, "last_update": datetime.now().isoformat()}))


    MAX_DIRECT_PAYLOAD_SIZE = 1 * 1024 * 1024 # Memory threshold for Claim Check pattern (1 Megabyte)
    
    if upload_file.size >= MAX_DIRECT_PAYLOAD_SIZE:
        # Save file to disk/cloud, send only the file path in message
        save_directory = "/temp/alexandria_assets/"
        os.makedirs(save_directory, exist_ok=True)
        file_path = f"{save_directory}{job_id}_{upload_file.filename}"

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
        ticket_payload = {
            "job_id": job_id,
            "task_type": routing_key,
            "payload_type": "ticket",
            "file_location": file_path 
        }
        

    else:
        # SMALL PAYLOAD (Direct Messaging)
        # Safe to pass directly through the RabbitMQ exchange
        file_content = await upload_file.read()
        ticket_payload = {
        "job_id": job_id,
        "task_type": routing_key,
        "payload_type": "direct",
        "data": file_content.decode("utf-8") 
        }

    # Dispatch the asynchronous task to the worker pool
    channel.basic_publish(
        exchange=os.getenv("RABBITMQ_EXCHANGE"),
        routing_key=routing_key,
        body=json.dumps(ticket_payload)
        )
    
    # Instantly return the Job ID so the client isn't blocked waiting for computation
    return {
        "job_id": job_id, 
        "routed_to": routing_key,
        "filename": upload_file.filename
    }

@app.get("/api/jobs/{job_id}")
async def jobStatus(job_id: str):
    job_data = r.get(job_id)
    if job_data:
        return json.loads(job_data)
    return {"error": "Job not found"}

@app.get("/")
async def main():
    return {"message": "working"}    