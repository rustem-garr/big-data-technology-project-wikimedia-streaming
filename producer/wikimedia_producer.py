import json
import sys
import time

import requests
from kafka import KafkaProducer

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BROKER = "kafka-server:9092"
KAFKA_TOPIC = "wikimedia-events"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        acks="all",
        retries=5,
        linger_ms=100)

def main():
    producer = create_producer()
    sent_count = 0

    while True:
        try:
            print("Connecting to Wikimedia event stream...", flush=True)

            headers = {
                "User-Agent": "Rustem-MIU-BDT-FinalProject/1.0 (student project; contact: rustem@example.com)",
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache"
            }

            with requests.get(
                WIKIMEDIA_URL,
                headers=headers,
                stream=True,
                timeout=60
                ) as response:
                response.raise_for_status()

                for raw_line in response.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    if raw_line.startswith(":"):
                        continue

                    if raw_line.startswith("data:"):
                        data_str = raw_line[len("data:"):].strip()

                        if not data_str:
                            continue

                        try:
                            event = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue

                        event_type = event.get("type", "unknown")
                        producer.send(
                            KAFKA_TOPIC,
                            key=event_type,
                            value=event
                        )

                        sent_count += 1

                        if sent_count % 10 == 0:
                            print(
                                f"Sent {sent_count} events to Kafka topic '{KAFKA_TOPIC}'", flush=True)

        except KeyboardInterrupt:
            print("\nStopping producer...", flush=True)
            break
        except Exception as e:
            print(f"Producer error: {e}", flush=True)
            print("Retrying in 5 seconds...", flush=True)
            time.sleep(5)
        finally:
            producer.flush()

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()