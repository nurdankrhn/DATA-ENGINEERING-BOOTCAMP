import json

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import config


TOPIC = 'orders'

VIP, DOMESTIC, INTERNATIONAL = 0, 1, 2

VIP_CUSTOMERS = {'C-1001', 'C-1042'}

def choose_partition(order):
    """The business rule. Returns the partition number for this order."""
    if order['customer_id'] in VIP_CUSTOMERS:
        return VIP                      # VIP wins even if the order is international
    if order['country'] == 'TR':
        return DOMESTIC
    return INTERNATIONAL

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        order = json.loads(event.value())
        print(f"order {event.key().decode()} ({order['customer_id']}, {order['country']}) "
              f"-> partition {event.partition()}")

def create_topic_if_missing(partitions=3):
    # If the topic does not exist, auto.create.topics.enable would create it with the
    # broker default (1 partition) on the first produce, and partition=2 would then fail
    # with "Unknown partition". So we create it ourselves with exactly 3 partitions.
    admin = AdminClient(config)
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=3)])
    futures[TOPIC].result()             # wait; raises if creation failed
    print(f'Created topic {TOPIC} with {partitions} partitions')

if __name__ == '__main__':
    create_topic_if_missing()
    producer = Producer(config)

    orders = [
        {'order_id': 'O-1', 'customer_id': 'C-1001', 'country': 'TR', 'amount': 1200.0},   # VIP
        {'order_id': 'O-2', 'customer_id': 'C-2210', 'country': 'TR', 'amount': 85.5},     # domestic
        {'order_id': 'O-3', 'customer_id': 'C-3307', 'country': 'DE', 'amount': 310.0},    # international
        {'order_id': 'O-4', 'customer_id': 'C-1042', 'country': 'US', 'amount': 2400.0},   # VIP (not international!)
        {'order_id': 'O-5', 'customer_id': 'C-4415', 'country': 'TR', 'amount': 42.0},     # domestic
        {'order_id': 'O-6', 'customer_id': 'C-5520', 'country': 'FR', 'amount': 199.0},    # international
        {'order_id': 'O-5', 'customer_id': 'C-1001', 'country': 'USA', 'amount': 42.0},     # VIP
        {'order_id': 'O-6', 'customer_id': 'C-2220', 'country': 'AU', 'amount': 199.0}    # international
    ]

    for order in orders:
        producer.produce(
            TOPIC,
            key=order['order_id'],
            value=json.dumps(order),
            partition=choose_partition(order),   # <- our decision, not hash(key)
            on_delivery=callback,
        )
        producer.poll(0)

    producer.flush()