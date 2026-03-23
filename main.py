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

#Redis Connection
r = redis.Redis(
    host='redis-11633.crce288.eu-central-1-1.ec2.cloud.redislabs.com',
    port=11633,
    decode_responses=True,
    username="default",
    password=os.getenv("REDIS_PASSWORD"),
)

#RabbitMQ Connection
credentials = pika.PlainCredentials("guest", "guest")
connection_params = pika.ConnectionParameters("localhost", 5672, '/', credentials)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

#Routing Map by file type
ROUTING_MAP = {
    "application/json": "process.json", 
    "application/zip": "build.unity", 
    "text/plain": "process.log"
}

@app.post("/api/jobs")
async def createJob(upload_file: UploadFile = File(...)):

    file_type = upload_file.content_type
    routing_key = ROUTING_MAP.get(file_type, "process.unassigned")
    if routing_key == "process.unassigned":
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_type}")
    
    job_id = str(uuid.uuid4())
    r.set((job_id), json.dumps({ "status": "processing", "task_type": routing_key, "last_update": datetime.now().isoformat()}))


    MAX_DIRECT_PAYLOAD_SIZE = 1 * 1024 * 1024 # 1 Megabyte
    
    if upload_file.size >= MAX_DIRECT_PAYLOAD_SIZE:
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
        file_content = await upload_file.read()
        ticket_payload = {
        "job_id": job_id,
        "task_type": routing_key,
        "payload_type": "direct",
        "data": file_content.decode("utf-8") 
        }
    
    channel.basic_publish(
        exchange="Alexandria",
        routing_key=routing_key,
        body=json.dumps(ticket_payload)
        )
    
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