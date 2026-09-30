"""Módulo de configuración y definición de tratamientos para el benchmark del Lakehouse."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


from consumer.sink_factory import SinkType


@dataclass(frozen=True)
class TreatmentConfig:
    """Representa la configuración inmutable de una corrida experimental."""

    # Variables independientes de carga e ingesta
    k_factor: int  # Factor de escala estocástica de trolebuses sintéticos
    buffer_size: int  # Umbral de registros por microbatch (N)
    flush_interval_sec: float  # Tiempo máximo de espera antes de commit forzado (T)
    sim_steps: int  # Duración de la simulación SUMO en pasos

    # Identificación y réplica
    replica_id: int  # Número de réplica (1, 2, 3...)
    seed: int = 42  # Semilla para reproducibilidad de simulación

    # Coordenadas del Lakehouse
    namespace: str = "zacatenco"
    table_name: str = "telemetria_trolebuses"

    # Rutas base
    results_dir: str = "results"

    # Kafka topic
    kafka_topic: str = "telemetria.trolebuses"

    # Elección de destino de ingesta
    sink_type: SinkType = "iceberg"  # "iceberg" para data lakehouse o "raw" para data lake

    # Etiqueta para diferenciar experimentos
    tag: Optional[str] = None

    @property
    def run_id(self) -> str:
        """Identificador canónico único para la corrida experimental."""
        base = f"{self.sink_type}_k{self.k_factor}_b{self.buffer_size}_t{int(self.flush_interval_sec)}_rep{self.replica_id}"
        if self.tag:
            return f"{base}_{self.tag}"
        return base

    @property
    def metrics_csv_path(self) -> str:
        """Ruta al archivo CSV de telemetría de ingesta para esta corrida."""
        Path(self.results_dir).mkdir(parents=True, exist_ok=True)
        return f"{self.results_dir}/ingest_{self.run_id}.csv"

    @property
    def evaluation_json_path(self) -> str:
        """Ruta al archivo JSON con las métricas de lectura y auditoría de almacenamiento."""
        Path(self.results_dir).mkdir(parents=True, exist_ok=True)
        return f"{self.results_dir}/eval_{self.run_id}.json"