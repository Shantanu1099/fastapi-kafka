from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from produce_schema import ProduceMessage

# Constant value section
KAFKA_BROKER = "localhost:9092"  # Kafka broker address
KAFKA_TOPIC = "fastapi-topic"  # Kafka topic to produce messages to
KAFKA_ADMIN_CLIENT_ID = "fastapi-admin-client"  # Client ID for the Kafka admin client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Kafka admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id=KAFKA_ADMIN_CLIENT_ID,
    )
    try:
        # Check if the topic already exists
        existing_topics = admin_client.list_topics()
        if KAFKA_TOPIC not in existing_topics:
            print(f"Creating Kafka topic: {KAFKA_TOPIC}")
            topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
        else:
            print(f"Kafka topic '{KAFKA_TOPIC}' already exists")
    except Exception as error:
        print(f"Error creating Kafka topic: {error}")
    finally:
        admin_client.close()  # Ensure the admin client is closed

    yield  # Yield control to the application
    print("Shutting down Kafka producer...")  # Log shutdown message


app = FastAPI(lifespan=lifespan)


@app.post("/produce/message", tags=["Producer"])
async def produce_message(
    messageReq: ProduceMessage, background_tasks: BackgroundTasks
):
    background_tasks.add_task(produce_kafka_message, messageReq)
    return {"status": "Message is being produced to Kafka..."}
