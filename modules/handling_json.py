import json
import subprocess
import os

def run(message,ch):
    blueprint = message.get("data")
    job_id = message.get("job_id")

    project_path = "/Users/mayaeven/Documents/GameEngineCompiler"

    current_dir = os.getcwd()
    json_path = os.path.join(current_dir,f"./temp/alexandria_assets/jsons/{job_id}_blueprint.json")
    export_path = os.path.join(current_dir, f"./temp/alexandria_assets/apks/{job_id}_game.apk")
    log_path = os.path.join(current_dir,f"./temp/alexandria_assets/logs/{job_id}_build.log")

    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    os.makedirs(os.path.dirname(export_path), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(json_path,"w") as blueprint:

        json.dump(message.get("data"), blueprint, indent=4)
        print("json saved")

    file_location = {"file_location": json_path}
    message.update(file_location)
    unity_command = ["/Applications/Unity/Hub/Editor/2022.3.62f3/Unity.app/Contents/MacOS/Unity",
    "-quit",
    "-batchmode",
    "-projectPath",project_path,
    "-executeMethod", "CloudBuilder.PerformAndroidBuild",
    "-logFile", log_path,
    "-blueprintPath", json_path,
    "-exportPath", export_path]
    result = subprocess.run(unity_command, capture_output=True, text=True)
    print("command ran")
    send_log_to_rabbitmq(log_path, job_id, result,ch)
    return(message)

def send_log_to_rabbitmq(log_path, job_id, result,ch):
    payload = {
        "job_id": job_id,
        "task_type": "process.log",
        "file_location": log_path,
        "result_value": result.returncode    
    }
    ch.basic_publish(
        exchange=os.getenv("RABBITMQ_EXCHANGE"),
        routing_key="build.log",
        body=json.dumps(payload)
        )

