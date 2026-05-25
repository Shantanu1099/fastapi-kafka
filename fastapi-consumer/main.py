from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

# Constant value section
KAFKA_BROKER = "localhost:9092"  # Kafka broker address
KAFKA_TOPIC = "fastapi-topic"  # Kafka topic to produce messages to
KAFKA_CONSUMER_ID = "fastapi-consumer"  # Client ID for the Kafka consumer client

print("Starting Kafka consumer...")  # Log startup message

stop_polling_event = asyncio.Event()  # Event to signal when to stop polling
app = FastAPI()


def json_deserializer(data):
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as error:
        print(f"Error deserializing message: {error}")
        return None


def create_kafka_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        client_id=KAFKA_CONSUMER_ID,
        value_deserializer=json_deserializer,
        auto_offset_reset="earliest",  # Start consuming from the earliest message
        enable_auto_commit=True,  # Enable automatic offset commits
    )


async def poll_consumer(consumer: KafkaConsumer):
    try:
        while not stop_polling_event.is_set():
            print("Polling for messages...")
            message_pack = consumer.poll(
                timeout_ms=1000, max_records=10
            )  # Poll for messages
            if message_pack:
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        print(f"message: {message}")
                        print(
                            f"Received message: {message.value} from topic: {message.topic}, partition: {message.partition}, offset: {message.offset}"
                        )
            await asyncio.sleep(2)  # Sleep briefly to avoid tight loop
    except Exception as error:
        print(f"Error while polling messages: {error}")
    finally:
        print("Stopping Kafka consumer...")  # Log shutdown message
        consumer.close()  # Ensure the consumer is closed


tasklist = []  # List to keep track of polling tasks


@app.get("/trigger-polling", tags=["Consumer"])
async def trigger_polling():
    if not tasklist:  # Only start polling if it's not already running
        stop_polling_event.clear()  # Clear the stop event to allow polling

        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer))
        tasklist.append(task)  # Add consumer to task list to track it
        return {"message": "Kafka Polling started"}
    return {"message": "Kafka Polling is already running"}


@app.get("/stop-trigger", tags=["Consumer"])
async def stop_trigger():
    if tasklist:  # Only stop polling if it's currently running
        stop_polling_event.set()  # Signal the polling task to stop
        await asyncio.gather(*tasklist)  # Wait for all polling tasks to finish
        tasklist.clear()  # Clear the task list after stopping
        return {"message": "Kafka Polling stopped"}
    return {"message": "Kafka Polling is not running"}
