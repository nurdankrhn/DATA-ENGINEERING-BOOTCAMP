
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_a_new_topic_if_not_exists(
    admin_client,
    topic_name="example-topic",
    num_partitions=1,
    replication_factor=1
):
    existing_topics = admin_client.list_topics()

    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        admin_client.create_topics(new_topics=[new_topic])
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")


if __name__ == "__main__":
    admin_client = KafkaAdminClient(
        bootstrap_servers=[
            "localhost:19092",
            "localhost:29092",
            "localhost:39092"
        ],
        client_id="week6-admin"
    )

    try:
        create_a_new_topic_if_not_exists(
            admin_client,
            topic_name="week6-test",
            num_partitions=3,
            replication_factor=1
        )

        # Call again to verify that an existing topic is not recreated.
        create_a_new_topic_if_not_exists(
            admin_client,
            topic_name="week6-test",
            num_partitions=3,
            replication_factor=1
        )
    finally:
        admin_client.close()
