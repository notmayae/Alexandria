#!/bin/bash

echo "🔍 Checking Python Environment..."
# 1. The Smart Check: Does the 'venv' folder exist?
if [ ! -d "venv" ]; then
    echo "⚙️  No virtual environment found. Building it now..."
    python3 -m venv venv
    
    echo "📦 Installing required Python libraries..."
    # Use the specific pip inside the new venv!
    venv/bin/pip install -r requirements.txt
    echo "✅ Python environment built successfully!"
else
    echo "✅ Virtual environment already exists. Skipping installation."
fi

echo "----------------------------------------"

echo "🚀 Booting Alexandria Control Plane (Docker)..."
docker-compose up -d

echo "⏳ Waiting for RabbitMQ and Redis to initialize..."
sleep 5 

# *** CHANGE THIS TO MATCH YOUR RMQ QUEUES ***
echo "👷 Booting Jsons Worker (Native Mac)..."
export TASK_QUEUE="data_processing" 
venv/bin/python worker.py & 

echo "👷 Booting Logs Worker (Native Mac)..."
export TASK_QUEUE="io_tasks"
venv/bin/python worker.py &

echo "✅ Alexandria is fully operational!"
echo "(Press CTRL+C to stop all native workers when you are done)"

# Keeps the script alive to catch your CTRL+C command
wait