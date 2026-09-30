# consumer/main_consumer.py
import multiprocessing as mp
import os
from typing import Optional
from multiprocessing.synchronize import Event as EventType


from consumer.kafka_consumer import KafkaStreamConsumer
from consumer.sink_factory import get_lakehouse_sink, SinkType


def run_consumer_loop(
        buffer_size: int,
        flush_interval_sec: float,
        metrics_csv_path: str,
        stop_event: Optional[EventType] = None,
        broker: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        sink_type: SinkType = "iceberg",
    ):
    """Punto de entrada compatible invocado por el proceso de pipeline.py."""
    kafka_broker = broker or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "100.111.214.11:9092"
    )
    kafka_topic = topic or os.getenv("KAFKA_TOPIC", "telemetria.trolebuses")
    consumer_group = group_id or "lakehouse-sink-benchmark"
    sink = get_lakehouse_sink(sink_type)

    consumer = KafkaStreamConsumer(
        broker=kafka_broker,
        topic=kafka_topic,
        group_id=consumer_group,
        sink=sink,
        buffer_size=buffer_size,
        flush_interval_sec=flush_interval_sec,
        metrics_output=metrics_csv_path,
        stop_event=stop_event,
    )

    consumer.start_loop()