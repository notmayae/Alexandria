import json
def run(message,ch):
    log_location = message.get("file_location")
    result = message.get("result_value")
    job_id = message.get("job_id")

    with open (log_location, "r") as log:
        log_content = log.read()

    if result == 0:
        utp_loc = log_content.find("##utp:")
        physx_loc = log_content[utp_loc:].find("[PhysX]") + utp_loc
        utp_stats = json.loads(log_content[utp_loc+6:physx_loc-1])
        
        
        building_gradle_line = log_content.find('"Building Gradle project" took')
        building_gradle_ms = log_content[building_gradle_line:].find("ms") + building_gradle_line
        building_gradle_stats = float(log_content[building_gradle_line+30:building_gradle_ms-1])
        
        
        android_sdk_line = log_content.find('"Detecting Android SDK" took')
        android_sdk_ms = log_content[android_sdk_line:].find("ms") + android_sdk_line
        android_sdk_stats = float(log_content[android_sdk_line+29:android_sdk_ms-1])
        
        optimized_json = {"utp_ stats": utp_stats, "building_gradle_stats": building_gradle_stats, "android_sdk_stats": android_sdk_stats}

        with open (f"/Users/mayaeven/Documents/GameEngineCompiler/Logs/Optimizations/{job_id}_optimized_json.json", "w") as file:
            json.dump(optimized_json, file)
        