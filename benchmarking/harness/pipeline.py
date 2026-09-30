"""
Arnés de supervisión y gestión del ciclo de vida del consumidor de streaming.
"""

import time
import multiprocessing as mp
from typing import Optional
from multiprocessing.synchronize import Event as EventType


# Importamos la función de entrada o la clase del consumidor ya existente
# (Asumiendo que el script principal o módulo expone un método run_consumer)
from consumer.main_consumer import run_consumer_loop
from benchmarking.config.treatment import TreatmentConfig


def _worker_process_target(treatment: TreatmentConfig, stop_event: EventType):
    """Punto de entrada aislado para el worker de consumo."""
    run_consumer_loop(
        buffer_size=treatment.buffer_size,
        flush_interval_sec=treatment.flush_interval_sec,
        metrics_csv_path=treatment.metrics_csv_path,
        stop_event=stop_event,
        sink_type=getattr(
            treatment, "sink_type", "iceberg"
        ),
    )


class PipelineHarness:
    """Administra el proceso de ingesta continua en segundo plano."""

    def __init__(self, drain_timeout_sec: float = 15.0):
        self.drain_timeout_sec = drain_timeout_sec
        self._process: Optional[mp.Process] = None
        self._stop_event: Optional[EventType] = None

    def start(self, treatment: TreatmentConfig) -> None:
        """Arranca el consumidor en un proceso secundario supervisado."""
        if self._process is not None and self._process.is_alive():
            raise RuntimeError("El proceso de ingesta ya se encuentra en ejecución.")

        self._stop_event = mp.Event()
        self._process = mp.Process(
            target=_worker_process_target,
            args=(treatment, self._stop_event),
            daemon=True,
        )
        self._process.start()
        print(f"[PipelineHarness] Consumidor iniciado (PID: {self._process.pid}).")
        # Breve espera para que el consumidor se suscriba a Kafka
        time.sleep(2.0)

    def wait_until_drained(self) -> None:
        """
        Espera un margen prudente para que los mensajes residuales en Kafka
        sean consumidos y comiteados a Iceberg tras finalizar la simulación.
        """
        print(f"[PipelineHarness] Drenando buffer remanente ({self.drain_timeout_sec} s)...")
        time.sleep(self.drain_timeout_sec)

    def stop(self) -> None:
        """Detiene el consumidor de forma limpia (graceful shutdown)."""
        if self._stop_event is not None:
            self._stop_event.set()

        if self._process is not None:
            self._process.join(timeout=10.0)
            if self._process.is_alive():
                print("[PipelineHarness Warning] El proceso no respondió; forzando terminación.")
                self._process.terminate()
                self._process.join()
            print("[PipelineHarness] ✓ Consumidor finalizado.")

        self._process = None
        self._stop_event = None