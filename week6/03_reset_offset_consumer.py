from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from config import config

def set_consumer_configs():
    config['group.id'] = 'reset_group'
    # Still needed: it is the fallback when the offset we ask for no longer exists (see below).
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False


START_OFFSETS = {0: 1, 1: 5, 2: 3}

def assignment_callback(consumer, partitions):
    # Called by poll() with the partitions the group gave us. Each p.offset is OFFSET_INVALID
    # (-1001) here: "use the committed offset, or auto.offset.reset if there is none".
    # We overwrite it, then call assign() ourselves with the modified list.
    for p in partitions:
        p.offset = START_OFFSETS.get(p.partition, OFFSET_BEGINNING)
        print(f'Assigned to {p.topic}, partition {p.partition}, starting at offset {p.offset}')

    consumer.assign(partitions)

if __name__ == '__main__':
    set_consumer_configs()
    consumer = Consumer(config)

    consumer.subscribe(['hello_topic'], on_assign=assignment_callback)

    try:
        while True:
            event = consumer.poll(1.0)

            if event is None:
                continue

            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                # offset() is this message's position in its partition. Compare it with
                # START_OFFSETS to see that the first message of each partition is the one we asked for.
                print(f'Received: {val} from partition {event.partition()} offset {event.offset()}')
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()