from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json

# Constant value section
KAFKA_BROKER = "localhost:9092"  # Kafka broker address
KAFKA_TOPIC = "fastapi-topic"  # Kafka topic to produce messages to
PRODUCER_CLIENT_ID = "fastapi-producer"  # Client ID for the Kafka producer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    client_id=PRODUCER_CLIENT_ID,
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # Serialize message to JSON
)


def produce_kafka_message(messageReq: ProduceMessage):
    try:
        # Test connection to Kafka broker
        producer.bootstrap_connected()
        print("Successfully connected to Kafka broker, sending message...")
        # Send message to Kafka topic
        producer.send(KAFKA_TOPIC, messageReq.model_dump())
        producer.flush()  # Ensure all messages are sent
    except Exception as error:
        print(f"Error connecting to Kafka broker: {error}")
        raise HTTPException(
            status_code=500, detail=f"Failed to connect to Kafka broker: {error}"
        )
