# Consumer: the class that reads messages. KafkaException: raised when poll() hands us an error.
from confluent_kafka import Consumer, KafkaException

# The same config dict as the producer: bootstrap.servers = localhost:19092,29092,39092.
# Consumers need a few more settings, added in set_consumer_configs() below.
from config import config


def set_consumer_configs():
    # group.id: consumers with the same group id SHARE the topic's partitions between them.
    # Kafka also stores "how far did this group read" (the committed offsets) under this name.
    config['group.id'] = 'hello_group2'

    # Where to start when this group has NO committed offset yet (first run, or offsets expired):
    #   'earliest' -> read the topic from the beginning
    #   'latest'   -> only messages produced after we started (the default)
    # It is NOT used once the group has a committed offset: then we continue from there.
    config['auto.offset.reset'] = 'latest'

    # By default the client commits offsets automatically every 5 s, from inside poll().
    # That can commit a message BEFORE our code finished processing it (crash -> message lost).
    # False = we decide when to commit (see consumer.commit() at the bottom).
    # Careful: with False and no commit() call, every restart re-reads from 'earliest'.
    config['enable.auto.commit'] = False


def assignment_callback(consumer, partitions):
    # Called by poll() when the group coordinator gives this consumer its partitions.
    # Start a second instance of this script and watch the partitions get split between them.
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')


if __name__ == '__main__':
    set_consumer_configs()
    consumer = Consumer(config)

    # subscribe() does not fetch anything yet. It registers interest in the topic; the actual
    # group join and partition assignment happen during the first poll() calls.
    consumer.subscribe(['orders'], on_assign=assignment_callback)

    try:
        while True:
            # poll() does everything: joins the group, sends heartbeats, fetches messages, fires
            # callbacks and (with auto commit) commits offsets. Call it continuously.
            # 1.0 = wait at most 1 second for a message. Short on purpose: Ctrl-C is only noticed
            # between two poll() calls, so a long timeout means a slow Ctrl-C.
            event = consumer.poll(1.0)

            if event is None:
                # No message arrived within 1 second. Not an error, just poll again.
                continue

            if event.error():
                # poll() also returns broker/topic problems as a Message with error() set
                # (unknown topic, authorization, ...). Its value() is the error text, not data.
                raise KafkaException(event.error())
            else:
                # A real message. value() is bytes (Kafka stores bytes), so decode it ourselves.
                val = event.value().decode('utf8')
                # The producer chose the partition from the key: hash(key) % number_of_partitions.
                # Same key -> same partition -> same consumer. That is how per-key ordering works.
                partition = event.partition()
                print(f'Received: {val} from partition {partition} ')

                # Manual commit: "I have fully processed this message, remember its offset".
                # Uncomment it and the next run continues after the last committed message.
                # Leave it commented and every run starts again from the beginning (see above).
                # commit(event) is synchronous: it waits for the broker, so it costs a round trip
                # per message. In real code commit every N messages or every few seconds instead.
                # consumer.commit(event)
    except KeyboardInterrupt:
        # Ctrl-C: leave the loop quietly instead of printing a traceback.
        print('Canceled by user.')
    finally:
        # Always runs, on Ctrl-C and on errors alike. close() leaves the group immediately so
        # the partitions are reassigned at once instead of after session.timeout.ms (45 s).
        # Note: with enable.auto.commit=False, close() does NOT commit anything for you.
        consumer.close()