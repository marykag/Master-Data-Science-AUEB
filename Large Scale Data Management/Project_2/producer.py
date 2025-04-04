import json
import asyncio
import pandas as pd
import random
from aiokafka import AIOKafkaProducer
from faker import Faker
from datetime import datetime

# Create a Faker instance
fake = Faker()

# Load movies from CSV
movies_df = pd.read_csv("/vagrant/data/movies.csv", header=0)
movies = movies_df.iloc[:, 0].tolist()  

# List of people (10 fake + 1 real name)
users = [fake.name() for _ in range(10)] + ["Maria"]

# Kafka configuration
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "test"


# Serialization function - Converts Python dictionaries into JSON before sending to Kafka.
def serializer(value):
    return json.dumps(value).encode()

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=serializer,
        compression_type="gzip"
    )

    await producer.start()
    try:
        while True:
            user = random.choice(users)
            movie = random.choice(movies)
            rating = random.randint(1, 10)
            timestamp = datetime.utcnow().isoformat()

            data = {
                "user": user,
                "movie": movie,
                "rating": rating,
                "timestamp": timestamp
            }

            await producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {data}")

            # Sleep (random interval up to 1 min)
            await asyncio.sleep(random.randint(5, 60))

    finally:
        await producer.stop()

# Run the async loop
if __name__ == "__main__":
    asyncio.run(produce())



