from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from importlib.metadata import metadata
import logging
import random
import time
import uuid
import json



KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

TOPIC_NAME = "test_topic"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

prodcuer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'linger.ms': 50,
    'acks': 1,
    'compression.type': 'gzip',
}

producer = Producer(prodcuer_conf)

def create_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} created successfully.")
                except  Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
        else:
            logger.info(f"Topic {topic_name} already exists.")
    except Exception as e:
        logger.error(f"Error creating Topic: {e}")

def generate_transaction():
    return dict(
        transaction_id= str(uuid.uuid4()),
        user_id = f"user_{random.randint(1, 1000)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time())
        merchant_id = f"merchant_{random.randint(1, 100)}",
        transaction_type=random.choice(["purchase", "refund"]),
        location= f'location_{random.randint(1, 50)}',
        payment_method=random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
        is_international=random.choice(["True", "False"]),
        currency=random.chocie(["USD", "EUR", "GBP", "JPY"]),
    )               

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {msg}")
    else:
        print(f"Record {msg.key()} successfully produced")

if __name__ == "__main__":

    create_topic(TOPIC_NAME)
    
    while True:
        transaction = generate_transaction()

        try:
            producer.produce(
                topic = TOPIC_NAME,
                key=transaction['user_id'],
                value=json.dumps(transaction).encode('utf-8'),
                on_delivery=delivery_report
            )
            print(f'Produced record: {transaction}')
            producer.flush()
        except Exception as e:
            print(f"Error sending transaction: {e}")