#!/usr/bin/env python3
"""
Generate sample events and send to Kafka
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer  # type: ignore
import argparse
from typing import Dict, Any

# Event types and regions
EVENT_TYPES = ["purchase", "view", "click", "add_to_cart", "checkout", "refund"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-north"]
USER_IDS = [f"user_{i:04d}" for i in range(100)]


def generate_event() -> Dict[str, Any]:
    """Generate a random event"""
    event_type = random.choice(EVENT_TYPES)

    # Adjust amount based on event type
    if event_type == "purchase":
        amount = round(random.uniform(10, 500), 2)
    elif event_type == "refund":
        amount = -round(random.uniform(10, 200), 2)
    elif event_type in ["add_to_cart", "checkout"]:
        amount = round(random.uniform(5, 300), 2)
    else:
        amount = 0.0

    # Flatten the structure for Timeplus
    return {
        "event_id": f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
        "user_id": random.choice(USER_IDS),
        "event_type": event_type,
        "amount": amount,
        "timestamp": datetime.now().isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate test events for Kafka")
    parser.add_argument("--rate", type=int, default=10, help="Events per second (default: 10)")
    parser.add_argument(
        "--duration", type=int, default=60, help="Duration in seconds (default: 60)"
    )
    parser.add_argument("--batch", type=int, default=100, help="Events per batch (default: 100)")
    args = parser.parse_args()

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",
    )

    print(f"📤 Starting event generation...")
    print(f"   Rate: {args.rate} events/second")
    print(f"   Duration: {args.duration} seconds")
    print(f"   Batch size: {args.batch} events")
    print()

    start_time = time.time()
    event_count = 0

    try:
        while time.time() - start_time < args.duration:
            batch_start = time.time()

            # Generate and send batch
            for _ in range(args.batch):
                event = generate_event()
                producer.send("e2e_events", value=event)
                event_count += 1

                # Rate limiting
                if event_count % args.rate == 0:
                    elapsed = time.time() - batch_start
                    if elapsed < 1.0:
                        time.sleep(1.0 - elapsed)
                    batch_start = time.time()
                    print(f"   Generated {event_count} events...", end="\r")

            producer.flush()

        print(f"\n✅ Generated {event_count} events in {args.duration} seconds")

    except KeyboardInterrupt:
        print(f"\n⚠️ Interrupted! Generated {event_count} events")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
