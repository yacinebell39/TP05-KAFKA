"""Publish synthetic smart-campus events (30 by default, one per second)."""

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


BUILDINGS = {
    "sensor-01": "building-A",
    "sensor-02": "building-B",
    "sensor-03": "building-C",
    "sensor-04": "building-D",
}


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def nonnegative_seconds(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="campus-sensors")
    parser.add_argument("--count", type=positive_int, default=30)
    parser.add_argument("--interval", type=nonnegative_seconds, default=1.0)
    parser.add_argument("--seed", type=int, help="Repeat the synthetic readings")
    parser.add_argument(
        "--include-building", action="store_true",
        help="Add campus_building using a fixed sensor-to-building mapping",
    )
    return parser.parse_args()


def make_event(event_id, rng, include_building=False):
    sensor_id = f"sensor-{rng.randint(1, 4):02d}"
    event = {
        "event_id": event_id,
        "sensor_id": sensor_id,
        "temperature_c": round(rng.uniform(18, 42), 2),
        "humidity_pct": round(rng.uniform(25, 85), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if include_building:
        event["campus_building"] = BUILDINGS[sensor_id]
    return event


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    producer = None
    sent = 0
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )
        print(f"Publishing {args.count} events to {args.topic}", flush=True)
        for event_id in range(1, args.count + 1):
            event = make_event(event_id, rng, args.include_building)
            # Wait for the broker acknowledgement before claiming success.
            metadata = producer.send(args.topic, value=event).get(timeout=30)
            sent += 1
            print(
                f"Sent: {json.dumps(event)} | "
                f"partition={metadata.partition} offset={metadata.offset}",
                flush=True,
            )
            if event_id < args.count:
                time.sleep(args.interval)
        producer.flush(timeout=30)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.", flush=True)
    except KafkaError as error:
        print(f"Kafka error: {error}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=10)
        print(f"Acknowledged events: {sent}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
