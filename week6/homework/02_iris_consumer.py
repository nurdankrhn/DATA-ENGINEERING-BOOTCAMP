
from pathlib import Path
from confluent_kafka import Consumer, KafkaException
from config import config

TOPIC = "topic1"
OUTPUT_DIR = Path("/tmp/kafka_out")

SPECIES_FILES = {
    "iris-setosa": "setosa_out.txt",
    "iris-versicolor": "versicolor_out.txt",
    "iris-virginica": "virginica_out.txt",
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Create all expected output files.
for filename in [*SPECIES_FILES.values(), "other_out.txt"]:
    (OUTPUT_DIR / filename).touch(exist_ok=True)

consumer_config = {
    **config,
    "group.id": "iris-homework-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        value = msg.value().decode("utf-8")
        key = msg.key().decode("utf-8") if msg.key() else ""

        species = value.rsplit(",", 1)[-1].strip().lower()
        filename = SPECIES_FILES.get(species, "other_out.txt")

        record = (
            f"{msg.topic()}|{msg.partition()}|"
            f"{msg.offset()}|{key}|{value}\n"
        )

        with (OUTPUT_DIR / filename).open("a", encoding="utf-8") as file:
            file.write(record)

        consumer.commit(message=msg, asynchronous=False)
        print(f"{filename}: {record.strip()}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
