from confluent_kafka import Producer
from config import config

producer = Producer(config)

key ="1234"

def callback(err, event):
    if err:
        # Delivery failed: log which topic and key failed so we can debug/retry.
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        # Delivery succeeded: value() returns bytes, so decode to a readable string.
        val = event.value().decode('utf8')
        key = event.key().decode('utf-8')
        # Print which partition the broker chose for this message.
        # (Partition is decided by the key's hash when a key is provided.)
        print(f'{key} - {val} sent to partition {event.partition()}.')

def say_hello(producer, key):
    # Build the message value (payload).
    value = f"Merhaba {key}!"
    producer.produce('hello_topic', value, key, on_delivery=callback)
    producer.poll(0) # non-blocking: fire any callbacks that are ready, free the buffer



keys = ['Amy', 'Brenda', 'Cindy', 'Derrick', 'Elaine', 'Fred']

for person in keys:
    say_hello(producer=producer, key=person)
    
producer.flush()
