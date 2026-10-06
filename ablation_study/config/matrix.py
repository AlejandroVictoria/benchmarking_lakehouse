"""Generador de la matriz de tratamientos para el estudio de ablación.

Define los 18 tratamientos x 5 réplicas (90 corridas en total).
"""

from typing import List
from benchmarking.config.treatment import TreatmentConfig
from consumer.sink_factory import SinkType

def build_ablation_matrix(
    replicas: int = 5,
    sim_steps: int = 600,
) -> List[TreatmentConfig]:
  """Construye las 90 combinaciones del diseño experimental factorial."""
  sinks: List[SinkType] = ["raw", "iceberg"]
  k_factors = [1, 5, 10]
  buffer_profiles = [
      (1, 0.0),  # Streaming puro
      (50, 2.0),  # Microbatch intermedio
      (200, 5.0),  # Microbatch estándar
  ]

  matrix: List[TreatmentConfig] = []

  for sink in sinks:
    for k in k_factors:
      for buf_size, flush_sec in buffer_profiles:
        for rep in range(1, replicas + 1):
          treatment = TreatmentConfig(
              k_factor=k,
              buffer_size=buf_size,
              flush_interval_sec=flush_sec,
              sim_steps=sim_steps,
              replica_id=rep,
              namespace="zacatenco",
              table_trolebuses="telemetria_trolebuses",
              table_estaciones="telemetria_estaciones",
              sink_type=sink,
              tag="ablation",
          )
          matrix.append(treatment)

  return matrix

def build_validation_matrix(sim_steps: int = 600) -> List[TreatmentConfig]:
  k_factors = [1, 5, 10]
  buffer_profiles = [
      (1, 0.0),  # Streaming puro
      (50, 2.0),  # Microbatch intermedio
      (200, 5.0),  # Microbatch estándar
  ]

  # 1. Definición explícita de los 4 tratamientos de validación
  validation_specs = [
      # Tratamiento 1: Raw en carga nominal con batch grande
      {
          "sink": "raw",
          "k": k_factors[0],
          "buf": buffer_profiles[2][0],
          "flush": buffer_profiles[2][1],
      },
      # Tratamiento 2: Raw bajo máxima carga con streaming inmediato (estrés de small files)
      {
          "sink": "raw",
          "k": k_factors[2],
          "buf": buffer_profiles[0][0],
          "flush": buffer_profiles[0][1],
      },
      # Tratamiento 3: Iceberg bajo carga baja con streaming inmediato (estrés de commits REST)
      {
          "sink": "iceberg",
          "k": k_factors[0],
          "buf": buffer_profiles[0][0],
          "flush": buffer_profiles[0][1],
      },
      # Tratamiento 4: Iceberg bajo máxima carga con microbatching estándar
      {
          "sink": "iceberg",
          "k": k_factors[2],
          "buf": buffer_profiles[2][0],
          "flush": buffer_profiles[2][1],
      },
  ]

  matrix: List[TreatmentConfig] = []

  for spec in validation_specs:
    sink: SinkType = spec["sink"]
    treatment = TreatmentConfig(
        k_factor=spec["k"],
        buffer_size=spec["buf"],
        flush_interval_sec=spec["flush"],
        sim_steps=sim_steps,
        replica_id=1,
        namespace="zacatenco",
        table_trolebuses="telemetria_trolebuses",
        table_estaciones="telemetria_estaciones",
        sink_type=sink,
        tag="first_run",
    )
    matrix.append(treatment)

  return matrix