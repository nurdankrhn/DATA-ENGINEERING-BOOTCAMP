
from confluent_kafka import Producer
from config import config

TOPIC = "stock-prices"

stocks = [
    {"symbol": "AAPL", "price": 182.35, "direction": "UP"},
    {"symbol": "MSFT", "price": 415.20, "direction": "DOWN"},
    {"symbol": "TSLA", "price": 248.17, "direction": "UP"},
    {"symbol": "GOOGL", "price": 159.88, "direction": "DOWN"},
    {"symbol": "NVDA", "price": 875.10, "direction": "UP"},
]


def delivery_callback(err, msg):
    if err is not None:
        print(
            f"Delivery failed for key {msg.key()}: {err}"
        )
    else:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")

        print(
            f"Key: {key}, Value: {value}, "
            f"Partition: {msg.partition()}"
        )


def send_stock_price(producer, stock):
    key = stock["symbol"]
    value = f"{stock['price']:.2f},{stock['direction']}"

    producer.produce(
        topic=TOPIC,
        key=key,
        value=value,
        on_delivery=delivery_callback
    )

    producer.poll(0)


def main():
    producer = Producer(config)

    for stock in stocks:
        send_stock_price(producer, stock)

    remaining = producer.flush()

    if remaining > 0:
        print(f"{remaining} messages still pending.")


if __name__ == "__main__":
    main()
