
import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from .config import INPUT_TOPIC, python_kafka_options


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--building-id", required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument(
        "--test-alert",
        choices=["hot", "cold", "dry", "wet"],
        default=None,
        help="Generate controlled readings for alert testing",
    )

    args = parser.parse_args()

    producer = KafkaProducer(
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        **python_kafka_options(),
    )

    try:
        while True:
            temperature = round(random.uniform(25, 45), 2)
            humidity = round(random.uniform(15, 85), 2)

            if args.test_alert == "hot":
                temperature = 45.0
            elif args.test_alert == "cold":
                temperature = 20.0
            elif args.test_alert == "dry":
                humidity = 20.0
            elif args.test_alert == "wet":
                humidity = 80.0

            record = {
                "id": args.sensor_id,
                "building_id": args.building_id,
                "temperature": temperature,
                "humidity": humidity,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(INPUT_TOPIC, value=record).get(timeout=15)

            print(
                f"SENT topic={INPUT_TOPIC} {record}",
                flush=True,
            )

            time.sleep(args.interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
