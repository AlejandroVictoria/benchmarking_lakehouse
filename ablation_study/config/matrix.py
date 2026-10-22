"""Generador de la matriz de tratamientos para el estudio de ablación.

Define los 18 tratamientos x 5 réplicas (90 corridas en total).
"""

from typing import List
from benchmarking.config.treatment import TreatmentConfig
from consumer.sink_factory import SinkType

def build_ablation_matrix(
    replicas: int = 5,
    sim_steps: int = 1200,
) -> List[TreatmentConfig]:
  """Construye las 300 combinaciones del diseño experimental factorial."""
  sinks: List[SinkType] = ["raw", "iceberg"]
  k_factors = [1, 5, 10, 20, 50, 100]  # Ajuste de carga nominal y máxima para el estudio de ablación
  buffer_profiles = [
      (1, 0.0),  # Streaming puro
      (50, 2.0),  # Microbatch intermedio
      (200, 5.0),  # Microbatch estándar
      (1000000, 2.0),
      (1000000, 5.0),
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
              tag="ablation_2",
          )
          matrix.append(treatment)

  return matrix

def build_validation_matrix(sim_steps: int = 1200) -> List[TreatmentConfig]:
    k_factors = [1, 5, 10, 20, 50, 100]  # Ajuste de carga nominal y máxima
    buffer_profiles = [
        (1, 0.0),  # Streaming puro
        (50, 2.0),  # Microbatch intermedio
        (200, 5.0),  # Microbatch estándar
        (1000000, 2.0),
        (1000000, 5.0),
    ]

    # Definición explícita de los tratamientos de validación aislados para Iceberg
    validation_specs = [
        # Tratamiento 1: Iceberg en streaming puro (Estrés de red y commits REST)
        # Equivalente a la primera falla: iceberg_k1_b1_t0
        {
            "sink": "iceberg",
            "k": k_factors[0],             # K = 1
            "buf": buffer_profiles[0][0],  # Buffer = 1
            "flush": buffer_profiles[0][1],# Flush = 0.0s
        },
        # Tratamiento 2: Iceberg bajo máxima carga con buffer gigante (Estrés de memoria)
        # Equivalente a la última falla: iceberg_k100_b1000000_t5
        {
            "sink": "iceberg",
            "k": k_factors[5],             # K = 100
            "buf": buffer_profiles[4][0],  # Buffer = 1,000,000
            "flush": buffer_profiles[4][1],# Flush = 5.0s
        },
    ]

    matrix: List[TreatmentConfig] = []

    for spec in validation_specs:
        sink = spec["sink"]
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
            tag="iceberg_isolation_test", # Tag representativo de la prueba de diagnóstico
        )
        matrix.append(treatment)

    return matrix