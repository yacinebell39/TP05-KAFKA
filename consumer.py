"""Consume campus readings, flag high temperatures, and optionally analyze/export."""

import argparse
import csv
import json
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError


CSV_FIELDS = [
    "event_id", "sensor_id", "temperature_c", "humidity_pct", "timestamp",
]


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="campus-sensors")
    parser.add_argument("--group-id", default="tp05-campus-monitor")
    parser.add_argument(
        "--running-average", action="store_true",
        help="Average ALL events received by this consumer, before building filtering",
    )
    parser.add_argument(
        "--building", choices=["building-A", "building-B", "building-C", "building-D"],
        help="Display and export only events from this building",
    )
    parser.add_argument(
        "--csv", type=Path, metavar="NEW_FILE",
        help="Export displayed events; refuses to overwrite an existing file",
    )
    parser.add_argument(
        "--max-messages", type=positive_int,
        help="Stop after this many received events, including filtered-out events",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    consumer = None
    csv_file = None
    writer = None
    received = displayed = alerts = missing_building = 0
    temperature_sum = 0.0
    try:
        if args.csv is not None:
            args.csv.parent.mkdir(parents=True, exist_ok=True)
            csv_file = args.csv.open("x", newline="", encoding="utf-8")
            writer = csv.DictWriter(
                csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore",
            )
            writer.writeheader()
            csv_file.flush()

        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_server,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=args.group_id,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        print(f"Listening to topic: {args.topic} | group={args.group_id}", flush=True)
        if args.building:
            print(f"Building filter: {args.building}", flush=True)

        for message in consumer:
            event = message.value
            temperature = event["temperature_c"]
            received += 1
            temperature_sum += temperature
            average = temperature_sum / received

            if args.building and "campus_building" not in event:
                missing_building += 1
                if missing_building == 1:
                    print(
                        "WARNING: Events without campus_building are filtered out. "
                        "Use producer.py --include-building for this experiment.",
                        flush=True,
                    )

            matches = args.building is None or event.get("campus_building") == args.building
            if matches:
                displayed += 1
                line = (
                    f'{event["timestamp"]} | {event["sensor_id"]} | '
                    f'event_id={event["event_id"]} | T={temperature} C | '
                    f'RH={event["humidity_pct"]}% | '
                    f'partition={message.partition} offset={message.offset}'
                )
                if "campus_building" in event:
                    line += f' | building={event["campus_building"]}'
                if args.running_average:
                    line += f" | running_average_all_received={average:.2f} C (n={received})"
                print(line, flush=True)

                if temperature >= 35:
                    alerts += 1
                    print("ALERT: High temperature detected!", flush=True)
                if writer is not None:
                    writer.writerow(event)
                    csv_file.flush()

            if args.max_messages is not None and received >= args.max_messages:
                break

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.", flush=True)
    except (KafkaError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"Consumer error: {error}", file=sys.stderr)
        return 1
    finally:
        try:
            if consumer is not None:
                consumer.close()
        finally:
            if csv_file is not None:
                csv_file.close()
            print(
                f"Summary: received={received}, displayed={displayed}, "
                f"high_temperature_alerts={alerts}, "
                f"exported={displayed if writer is not None else 0}",
                flush=True,
            )
            if args.running_average and received:
                print(
                    f"Average of all received events: {temperature_sum / received:.2f} C",
                    flush=True,
                )
            if missing_building:
                print(f"Events missing campus_building: {missing_building}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
