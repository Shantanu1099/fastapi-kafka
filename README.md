# fastapi-kafka-pub-sub

A simple pub/sub demo using:
- FastAPI Producer service
- FastAPI Consumer service
- Kafka + ZooKeeper via Docker Compose

## Project Structure

- docker-compose.yml
- fastapi-producer
- fastapi-consumer

## What This Project Does

1. Producer API receives a message from HTTP.
2. Producer sends that message to Kafka topic fastapi-topic.
3. Consumer API starts a polling task to read messages from the same topic.
4. Consumer prints received messages in its terminal logs.

## Prerequisites

- Git
- Docker Desktop (or Docker Engine + Compose)
- Python 3.14+
- PowerShell (commands below are PowerShell-friendly)

Optional:
- uv package manager

## 1) Clone the Repository

Run these in PowerShell:

    git clone <your-repo-url> fastapi-kafka
    cd fastapi-kafka

If you already cloned into fastApi-kafka, use that folder directly.

## 2) Start Kafka and ZooKeeper

From project root (folder containing docker-compose.yml):

    docker compose up -d

Check containers:

    docker compose ps

View broker logs:

    docker logs broker --tail 100

## 3) Run Services with uv (Recommended)

Open two terminals.

Terminal A: Producer

    cd fastapi-producer
    uv sync
    uv run fastapi dev main.py --port 8000

Terminal B: Consumer

    cd fastapi-consumer
    uv sync
    uv run fastapi dev main.py --port 8001

## 4) Run Services with Python + pip

Open two terminals.

Terminal A: Producer

    cd fastapi-producer
    py -3.14 -m venv .venv
    . .venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install -e .
    fastapi dev main.py --port 8000

Terminal B: Consumer

    cd fastapi-consumer
    py -3.14 -m venv .venv
    . .venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install -e .
    fastapi dev main.py --port 8001

If py -3.14 is unavailable, install Python 3.14 first or use your installed Python executable path.

## 5) Use the APIs

### 5.1 Start Consumer Polling

In a new terminal:

    Invoke-RestMethod -Method GET -Uri "http://127.0.0.1:8001/trigger-polling"

Expected response:

    {"message":"Kafka Polling started"}

### 5.2 Publish a Message

Send a message to producer:

    Invoke-RestMethod -Method POST -Uri "http://127.0.0.1:8000/produce/message" -ContentType "application/json" -Body '{"message":"Hello from FastAPI"}'

Expected response:

    {"status":"Message is being produced to Kafka..."}

### 5.3 Verify Consumer Output

Check the consumer terminal logs. You should see received message details.

### 5.4 Stop Consumer Polling

    Invoke-RestMethod -Method GET -Uri "http://127.0.0.1:8001/stop-trigger"

## 6) Stop Everything

Stop FastAPI services with Ctrl+C in their terminals.

Then stop Kafka stack from project root:

    docker compose down

To remove volumes too:

    docker compose down -v

## API Summary

Producer (port 8000):
- POST /produce/message
- Request JSON:

    {
      "message": "your text"
    }

Consumer (port 8001):
- GET /trigger-polling
- GET /stop-trigger

## Troubleshooting

1. Kafka connection issues
- Ensure docker compose up -d is running.
- Confirm broker is reachable on localhost:9092.
- Check broker logs: docker logs broker --tail 200.

2. Import errors (for example pydantic not found)
- Confirm you activated the correct virtual environment.
- Confirm dependencies are installed in the same interpreter you run FastAPI with.

3. Port already in use
- Change API ports, for example:

    fastapi dev main.py --port 8010

4. PowerShell activation policy errors
- In current shell only:

    Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

## Notes

- Kafka topic is auto-created by producer startup logic if missing.
- Current topic name is fastapi-topic.
- Broker endpoint used by services is localhost:9092.
- https://www.youtube.com/watch?v=K4jjF8uWUmg
