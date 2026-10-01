# consumer/kafka_consumer.py
import time
from typing import Any, Optional
from confluent_kafka import Consumer, KafkaError
from consumer.arrow_transformer import ArrowTransformer
from consumer.metrics_tracker import MetricsTracker


class KafkaStreamConsumer:
    def __init__(
        self,
        broker: str,
        topic: str,
        group_id: str,
        sink,
        buffer_size: int = 1000,
        flush_interval_sec: float = 10.0,
        metrics_output: str = "metricas_experimento.csv",
        stop_event: Optional[Any] = None,
        entity_type: str = "trolebuses",
        ):
        self.topic = topic
        self.entity_type = entity_type
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec
        self.stop_event = stop_event

        # Módulos dependientes
        self.transformer = ArrowTransformer()
        self.sink = sink
        self.metrics = MetricsTracker(metrics_output)

        # Configuración Kafka
        conf = {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Commit coordinado tras el snapshot
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    def _flush(
        self, raw_buffer: list[bytes], buffer_wait_ms: float
    ) -> tuple[int, float]:
        """Ejecuta la transformación, el commit a Iceberg y el registro de métricas."""
        if not raw_buffer:
            return 0, 0.0

        # Fase 1: Transformación PyArrow
        t0_trans = time.perf_counter()
        arrow_table, raw_bytes = self.transformer.transform(raw_buffer, entity_type=self.entity_type)
        t_trans_ms = (time.perf_counter() - t0_trans) * 1000

        print(f"\n[DEBUG CONSUMER] raw_buffer len: {len(raw_buffer)}")
        print(f"[DEBUG CONSUMER] arrow_table rows: {arrow_table.num_rows if arrow_table is not None else 'None'}")
        print(f"[DEBUG CONSUMER] Instancia del sink: {self.sink.__class__.__module__}.{self.sink.__class__.__name__}")

        # Fase 2: Ingesta en Lakehouse (S3 + REST Commit)
        t0_commit = time.perf_counter()
        arrow_bytes = self.sink.append_batch(arrow_table)
        t_commit_ms = (time.perf_counter() - t0_commit) * 1000
        print(f"[DEBUG CONSUMER] arrow_bytes retornados: {arrow_bytes} en {t_commit_ms:.4f} ms\n")
        
        # Fase 3: Commit del offset en Kafka (Garantía At-Least-Once)
        self.consumer.commit()

        # Fase 4: Registro de telemetría experimental
        m = self.metrics.log_batch(
            records_count=len(raw_buffer),
            buffer_wait_ms=buffer_wait_ms,
            transform_time_ms=t_trans_ms,
            commit_time_ms=t_commit_ms,
            raw_bytes_size=raw_bytes,
            arrow_bytes_size=arrow_bytes,
        )

        print(
            f"[Batch #{m.batch_id}] {m.records_count} rows | "
            f"Trans: {m.transform_time_ms:.1f}ms | Commit: {m.commit_time_ms:.1f}ms"
            f" | Total: {m.total_cycle_ms:.1f}ms"
        )

        return m.records_count, m.total_cycle_ms

    def start_loop(self):
        print(
            f"[Consumer] Iniciado en tópico '{self.topic}'. Buffer:"
            f" {self.buffer_size} eventos / {self.flush_interval_sec}s..."
        )
        raw_buffer = []
        t_buffer_start = time.perf_counter()

        try:
            # El bucle se mantiene vivo mientras no se reciba la señal de parada
            while not (self.stop_event and self.stop_event.is_set()):
                msg = self.consumer.poll(timeout=0.5)

                if msg is not None:
                    if msg.error():
                        err = msg.error()
                        if err is not None:
                            if err.code() == KafkaError._PARTITION_EOF:
                                continue
                        print(f"[Error Kafka]: {msg.error()}")
                        break
                    raw_buffer.append(msg.value())

                    # Evaluación de políticas de corte (Volumen vs Tiempo)
                    elapsed_sec = time.perf_counter() - t_buffer_start
                    buffer_lleno = len(raw_buffer) >= self.buffer_size
                    timeout_alcanzado = raw_buffer and (
                        elapsed_sec >= self.flush_interval_sec
                    )

                    if buffer_lleno or timeout_alcanzado:
                        wait_ms = elapsed_sec * 1000
                        self._flush(raw_buffer, wait_ms)
                        raw_buffer.clear()
                        t_buffer_start = time.perf_counter()

        except KeyboardInterrupt:
            print("\n[Consumer] Interrupción manual recibida.")
        finally:
            if raw_buffer:
                print(f"[Consumer] Vaciando {len(raw_buffer)} eventos residuales...")
                wait_ms = (time.perf_counter() - t_buffer_start) * 1000
                self._flush(raw_buffer, wait_ms)
            self.consumer.close()
            print("[Consumer] Cerrado de forma segura.")