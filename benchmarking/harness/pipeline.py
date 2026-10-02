"""
Arnés de supervisión y gestión del ciclo de vida del consumidor de streaming.
"""

import time
import multiprocessing as mp
from typing import Optional
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from typing import Literal


# Importamos la función de entrada o la clase del consumidor ya existente
# (Asumiendo que el script principal o módulo expone un método run_consumer)
from consumer.main_consumer import run_consumer_loop
from benchmarking.config.treatment import TreatmentConfig, SinkType


def _worker_process_target(    
    topic: str,
    entity_type: str,
    table_name: str,
    raw_path: str,
    buffer_size: int,
    flush_interval_sec: float,
    metrics_csv_path: str,
    sink_type: SinkType,
    stop_event: EventType,
    kafka_group_id: str,
):
    """Punto de entrada aislado para el worker de consumo."""
    run_consumer_loop(
          topic=topic,
          entity_type=entity_type,
          table_name=table_name,
          raw_path=raw_path,
          buffer_size=buffer_size,
          flush_interval_sec=flush_interval_sec,
          metrics_csv_path=metrics_csv_path,
          sink_type=sink_type,
          stop_event=stop_event,
          group_id=kafka_group_id
      )
    

class PipelineHarness:
    """Administra el proceso de ingesta continua en segundo plano."""
    def __init__(self, drain_timeout_sec: float = 15.0):
        self.drain_timeout_sec = drain_timeout_sec
        self._proc_trolebuses: Optional[mp.Process] = None
        self._proc_estaciones: Optional[mp.Process] = None
        self._stop_event: Optional[EventType] = None

    def start(self, treatment: TreatmentConfig) -> None:
        """Arranca los consumidores de trolebuses y estaciones en procesos secundarios supervisados."""
        if (self._proc_trolebuses is not None and self._proc_trolebuses.is_alive()) or (
            self._proc_estaciones is not None and self._proc_estaciones.is_alive()
        ):
            raise RuntimeError(
                "Uno o más procesos de ingesta ya se encuentran en ejecución."
            )

        self._stop_event = mp.Event()

        # Rutas diferenciadas para no sobreescribir telemetría de ingesta
        metrics_dir = Path(treatment.results_dir)
        metrics_dir.mkdir(parents=True, exist_ok=True)
        csv_trolebuses = (
            f"{treatment.results_dir}/ingest_{treatment.run_id}_trolebuses.csv"
        )
        csv_estaciones = (
            f"{treatment.results_dir}/ingest_{treatment.run_id}_estaciones.csv"
        )

        # Construcción de group.id de Kafka aislado por tratamiento y entidad para evitar interferencias
        kafka_group_id = f"{treatment.run_id}"

        # 1. Proceso Consumidor Trolebuses
        self._proc_trolebuses = mp.Process(
            target=_worker_process_target,
            kwargs={
                "topic": treatment.kafka_topic_trolebuses,
                "entity_type": "trolebuses",
                "table_name": treatment.table_trolebuses,
                "raw_path": treatment.raw_trolebuses_path,
                "buffer_size": treatment.buffer_size,
                "flush_interval_sec": treatment.flush_interval_sec,
                "metrics_csv_path": csv_trolebuses,
                "sink_type": treatment.sink_type,
                "stop_event": self._stop_event,
                "kafka_group_id": kafka_group_id,
            },
            daemon=True,
        )

        # 2. Proceso Consumidor Estaciones
        self._proc_estaciones = mp.Process(
            target=_worker_process_target,
            kwargs={
                "topic": treatment.kafka_topic_estaciones,
                "entity_type": "estaciones",
                "table_name": treatment.table_estaciones,
                "raw_path": treatment.raw_estaciones_path,
                "buffer_size": treatment.buffer_size,
                "flush_interval_sec": treatment.flush_interval_sec,
                "metrics_csv_path": csv_estaciones,
                "sink_type": treatment.sink_type,
                "stop_event": self._stop_event,
                "kafka_group_id": kafka_group_id,
            },
            daemon=True,
        )

        self._proc_trolebuses.start()
        self._proc_estaciones.start()

        print(
            f"[PipelineHarness] Consumidor Trolebuses iniciado (PID:"
            f" {self._proc_trolebuses.pid})."
        )
        print(
            f"[PipelineHarness] Consumidor Estaciones iniciado (PID:"
            f" {self._proc_estaciones.pid})."
        )

    def wait_until_drained(self) -> None:
        """
        Espera un margen prudente para que los mensajes residuales en Kafka
        sean consumidos y comiteados a Iceberg tras finalizar la simulación.
        """
        print(
            f"[PipelineHarness] Drenando buffer remanente"
            f" ({self.drain_timeout_sec} s)..."
        )
        time.sleep(self.drain_timeout_sec)

    def stop(self) -> None:
        """Detiene el consumidor de forma limpia (graceful shutdown)."""
        if self._stop_event is not None:
              self._stop_event.set()
        
        processes = [
            ("Trolebuses", self._proc_trolebuses),
            ("Estaciones", self._proc_estaciones),
        ]
    
        for name, proc in processes:
            if proc is not None:
                proc.join(timeout=10.0)
                if proc.is_alive():
                    print(
                        f"[PipelineHarness Warning] El proceso {name} no respondió;"
                        " forzando terminación."
                    )
                    proc.terminate()
                    proc.join()
                print(f"[PipelineHarness] ✓ Consumidor {name} finalizado.")
    
        self._proc_trolebuses = None
        self._proc_estaciones = None
        self._stop_event = None