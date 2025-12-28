import os
import time
import json
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def parse_proc_stat():
    snapshot = {
        "timestamp": int(time.time() * 1000),
        "cpus": [],
        "intr": [],
        "softirq": [],
        "ctxt": 0,
        "btime": 0,
        "processes": 0,
        "procs_running": 0,
        "procs_blocked": 0,
    }

    with open("/proc/stat", "r") as f:
        for line in f:
            parts = line.split()

            if parts[0].startswith("cpu"):
                if parts[0] == "cpu" or parts[0][3:].isdigit():
                    snapshot["cpus"].append({
                        "id": parts[0],
                        "user": int(parts[1]),
                        "nice": int(parts[2]),
                        "system": int(parts[3]),
                        "idle": int(parts[4]),
                        "iowait": int(parts[5]),
                        "irq": int(parts[6]),
                        "softirq": int(parts[7]),
                        "steal": int(parts[8]),
                        "guest": int(parts[9]),
                        "guest_nice": int(parts[10]),
                    })

            elif parts[0] == "intr":
                snapshot["intr"] = [int(x) for x in parts[1:]]

            elif parts[0] == "softirq":
                snapshot["softirq"] = [int(x) for x in parts[1:]]

            elif parts[0] == "ctxt":
                snapshot["ctxt"] = int(parts[1])
            elif parts[0] == "btime":
                snapshot["btime"] = int(parts[1])
            elif parts[0] == "processes":
                snapshot["processes"] = int(parts[1])
            elif parts[0] == "procs_running":
                snapshot["procs_running"] = int(parts[1])
            elif parts[0] == "procs_blocked":
                snapshot["procs_blocked"] = int(parts[1])

    return snapshot


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def main():
    registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("TOPIC", "procstat_snapshots")
    sleep_seconds = float(os.getenv("SLEEP_SECONDS", "1"))

    schema_registry = SchemaRegistryClient({"url": registry_url})

    schema_path = Path(__file__).parent / "procstat_schema.avsc"
    with open(schema_path, "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=schema_str,
    )

    producer_conf = {
        "bootstrap.servers": bootstrap,
        "value.serializer": avro_serializer,
    }

    producer = SerializingProducer(producer_conf)

    while True:
        record = parse_proc_stat()
        producer.produce(
            topic=topic,
            value=record,
            on_delivery=delivery_report,
        )
        producer.flush()
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
