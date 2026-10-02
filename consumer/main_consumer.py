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
    entity_type: str = "trolebuses",
    table_name: Optional[str] = None,
    raw_path: Optional[str] = None,
):
    """Punto de entrada compatible invocado por el proceso de pipeline.py."""
    kafka_broker = broker or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "100.111.214.11:9092"
    )
    kafka_topic = topic or os.getenv("KAFKA_TOPIC", "telemetria.trolebuses")
    
    # Grupos aislados por entidad para evitar interferencias
    if group_id is None:
        consumer_group = f"lakehouse-sink-{entity_type}"
    else:
        consumer_group = f"harness_{group_id}_{entity_type}"

    print(f"[Consumer Loop] Iniciando consumidor Kafka para tópico '{kafka_topic}'"
          f" con group.id='{consumer_group}' y sink={sink_type}...")
    # Construcción dinámica del sink según la entidad/ruta
    sink = get_lakehouse_sink(
        sink_type=sink_type,
        entity_type=entity_type,
        table_name=table_name,
        raw_path=raw_path,
    )

    consumer = KafkaStreamConsumer(
        broker=kafka_broker,
        topic=kafka_topic,
        group_id=consumer_group,
        sink=sink,
        entity_type=entity_type,
        buffer_size=buffer_size,
        flush_interval_sec=flush_interval_sec,
        metrics_output=metrics_csv_path,
        stop_event=stop_event,
    )

    consumer.start_loop()