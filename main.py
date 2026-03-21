from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
import uuid
import redis
from datetime import datetime
import json
import pika


app = FastAPI()

#Redis Connection
r = redis.Redis(
    host='redis-11633.crce288.eu-central-1-1.ec2.cloud.redislabs.com',
    port=11633,
    decode_responses=True,
    username="default",
    password="iAgAWTIigIKzgku2OIhIHR9J91Axrb9F",
)

#RabbitMQ Connection
credentials = pika.PlainCredentials("guest", "guest")
connection_params = pika.ConnectionParameters("localhost", 5672, '/', credentials)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

#Routing Map by file type
ROUTING_MAP = {
    "application/json": "process.blueprint",
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
    
    job_id = uuid.uuid4()
    
    r.set(str(job_id), json.dumps({ "status": "processing", "task_type": routing_key, "last_update": datetime().isoformat()}))

    file_content = await upload_file.read()
    channel.basic_publish(
        exchange="Alexandria",
        routing_key=routing_key,
        body=file_content)
    
    return {
        "job_id": job_id, 
        "routed_to": routing_key,
        "filename": upload_file.filename
    }

@app.get("/api/jobs/{job_id}")
async def jobStatus(job_id: str):
    r = redis.Redis(
    host='redis-11633.crce288.eu-central-1-1.ec2.cloud.redislabs.com',
    port=11633,
    decode_responses=True,
    username="default",
    password="iAgAWTIigIKzgku2OIhIHR9J91Axrb9F",
)
    return (r.get(job_id))


@app.get("/")
async def main():
    return {"message": "working"}    