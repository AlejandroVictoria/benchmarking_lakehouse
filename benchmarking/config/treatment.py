from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional


# Definición del tipo de sink
SinkType = Literal["iceberg", "raw"]


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

    # Coordenadas y Namespace del Lakehouse
    namespace: str = "zacatenco"
    raw_bucket: str = "telemetria-lake"
    raw_base_prefix: str = "raw_parquet/zacatenco"

    # Nombres canónicos de tablas (Iceberg)
    table_trolebuses: str = "telemetria_trolebuses"
    table_estaciones: str = "telemetria_estaciones"

    # Tópicos de Kafka asociados
    kafka_topic_trolebuses: str = "telemetria.trolebuses"
    kafka_topic_estaciones: str = "telemetria.estaciones"

    # Rutas de resultados
    results_dir: str = "results"

    # Elección de destino de ingesta
    sink_type: SinkType = "iceberg"  # "iceberg" para Lakehouse o "raw" para Data Lake

    # Etiqueta opcional para diferenciar corridas experimentales
    tag: Optional[str] = None

    # --- Compatibilidad hacia atrás (por si algún módulo aún invoca estas variables) ---
    @property
    def table_name(self) -> str:
        """Alias de compatibilidad para telemetria_trolebuses."""
        return self.table_trolebuses

    @property
    def kafka_topic(self) -> str:
        """Alias de compatibilidad para el tópico de trolebuses."""
        return self.kafka_topic_trolebuses

    # --- Identificador Canónico ---
    @property
    def run_id(self) -> str:
        """Identificador canónico único para la corrida experimental."""
        base = f"{self.sink_type}_k{self.k_factor}_b{self.buffer_size}_t{int(self.flush_interval_sec)}_rep{self.replica_id}"
        if self.tag:
            return f"{base}_{self.tag}"
        return base

    # --- Particionamiento Hive para Raw ---
    @property
    def partition_date(self) -> str:
        """Fecha canónica de particionamiento Hive (UTC YYYY-MM-DD)."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @property
    def raw_trolebuses_path(self) -> str:
        """Ruta física en MinIO para trolebuses particionada por fecha."""
        return f"{self.raw_base_prefix}/trolebuses/date={self.partition_date}"

    @property
    def raw_estaciones_path(self) -> str:
        """Ruta física en MinIO para estaciones particionada por fecha."""
        return f"{self.raw_base_prefix}/estaciones/date={self.partition_date}"

    # --- Rutas de Auditoría y Métricas ---
    @property
    def metrics_csv_path(self) -> str:
        """Ruta al archivo CSV consolidado de telemetría de ingesta."""
        Path(self.results_dir).mkdir(parents=True, exist_ok=True)
        Path(f"{self.results_dir}/ingest").mkdir(parents=True, exist_ok=True)
        return f"{self.results_dir}/ingest/ingest_{self.run_id}"

    @property
    def evaluation_json_path(self) -> str:
        """Ruta al archivo JSON con las métricas de lectura y auditoría."""
        Path(self.results_dir).mkdir(parents=True, exist_ok=True)
        Path(f"{self.results_dir}/eval").mkdir(parents=True, exist_ok=True)
        return f"{self.results_dir}/eval/eval_{self.run_id}.json"