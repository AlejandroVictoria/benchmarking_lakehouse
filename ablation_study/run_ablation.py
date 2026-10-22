"""Orquestador Maestro para el Estudio de Ablación Formal (90 corridas).

Ejecuta secuencialmente la matriz factorial completa con idempotencia,
aislamiento limpio de almacenamiento y recolección analítica automatizada.
"""

from datetime import datetime
import os
from pathlib import Path
import sys
import time

# Asegurar visibilidad del root del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from ablation_study.config.matrix import build_ablation_matrix, build_validation_matrix
from benchmarking.core.lifecycle import LakehouseLifecycleManager
from benchmarking.evaluation.evaluator import ReadEvaluator
from benchmarking.harness.pipeline import PipelineHarness
from benchmarking.harness.workload import WorkloadDriver


def execute_single_run(
    treatment, lifecycle: LakehouseLifecycleManager, idx: int, total: int
) -> bool:
  """Ejecuta una corrida completa de un tratamiento de manera aislada."""
  print("\n" + "=" * 70)
  print(f"[{idx}/{total}] TRATAMIENTO: {treatment.run_id}")
  print(
      f"      Sink: {treatment.sink_type.upper()} | Demanda K: {treatment.k_factor} | "
      f"Buffer: {treatment.buffer_size} msg / {treatment.flush_interval_sec}s | Réplica: {treatment.replica_id}"
  )
  print("=" * 70)

  # 1. Reset de almacenamiento en MinIO / Catálogo para este tratamiento
  lifecycle.reset_for_treatment(treatment)
  print(f"[{treatment.run_id}] ✓ Estado de almacenamiento limpio y aislado.")
  # 2. Despliegue de consumidores Kafka en segundo plano
  pipeline = PipelineHarness(drain_timeout_sec=12.0)
  print(f"[{treatment.run_id}] ✓ Pipeline de consumidores desplegado y listo. Inicializando...")
  pipeline.start(treatment)
  print(f"[{treatment.run_id}] ✓ Pipeline de consumidores activo y escuchando tópicos Kafka.")
  print(f"[{treatment.run_id}] ✓ Inicializando workload... Comunicándose con SUMO vía FastAPI.")
  # 3. Disparo del workload SUMO vía FastAPI
  workload = WorkloadDriver(api_base_url="https://simulacion.viccoding.dev")
  sim_stats = workload.trigger_and_wait(treatment)

  pasos_procesados = sim_stats.get("progreso", {}).get(
      "paso_actual"
  ) or sim_stats.get("paso_actual", 0)
  print(
      f"[{treatment.run_id}] ✓ Simulación terminada con {pasos_procesados} pasos."
  )

  # 4. Drenar colas pendientes y cerrar pipeline
  pipeline.wait_until_drained()
  pipeline.stop()

  # Pausa técnica para consolidación de metadatos en S3/MinIO
  time.sleep(1.5)

  # 5. Auditoría física y benchmark analítico OLAP (DuckDB)
  evaluator = ReadEvaluator()
  report = evaluator.evaluate_and_persist(treatment, workload_stats=sim_stats)

  # Métricas rápidas en consola
  storage = report.get("storage_audit", {})
  bench = report.get("query_benchmark", {})
  status = bench.get("status", "unknown")

  print(f"[{treatment.run_id}] ✓ Archivos Parquet: {storage.get('total_parquet_files')} | "
        f"Registros: {storage.get('total_records')} | OLAP: {status}")

  return True


def run_ablation_study():
  print("#" * 70)
  print("##     INICIANDO SUITE DE ABLACIÓN (60 CONFIGS x 5 RÉPLICAS = 300)    ##")
  print("#" * 70)

  lifecycle = LakehouseLifecycleManager()

  # 1. Comprobación inicial de infraestructura
#   print("\n[*] Verificando servicios base (MinIO S3, REST Catalog, Kafka)...")
  if not lifecycle.verify_environment():
    print(
        "[!] ERROR CRÍTICO: Los servicios base no están disponibles. Abortando suite."
    )
    sys.exit(1)

  # 2. Preparación limpia y única de los tópicos de Kafka
#   print("[*] Asegurando estado limpio de los tópicos en Kafka...")
  lifecycle.ensure_clean_kafka_topics()

  # 3. Cargar la matriz completa
  treatments = build_ablation_matrix(sim_steps=1200)
#   treatments = build_validation_matrix(sim_steps=1200)
  total_runs = len(treatments)
  failed_runs = []

  t_start_suite = time.time()

  # 4. Bucle secuencial de experimentos
  for idx, treatment in enumerate(treatments, start=1):
    json_path = Path(treatment.evaluation_json_path)

    # Verificación de idempotencia
    if json_path.exists() and json_path.stat().st_size > 0:
      print(
          f"[{idx}/{total_runs}] Saltando '{treatment.run_id}' (reporte ya existe: {json_path.name})"
      )
      continue

    try:
      execute_single_run(treatment, lifecycle, idx, total_runs)
    except KeyboardInterrupt:
      print("\n[!] Suite interrumpida manualmente por el usuario. Saliendo...")
      sys.exit(0)
    except Exception as e:
      print(f"\n[!] ERROR en la corrida [{treatment.run_id}]: {e}")
      failed_runs.append((treatment.run_id, str(e)))

    # Pausa de amortiguación para evitar congestión de conexiones TCP
    time.sleep(1.0)

  # Resumen final
  elapsed_min = (time.time() - t_start_suite) / 60.0
  print("\n" + "#" * 70)
  print("##                 RESUMEN DE LA SUITE DE ABLACIÓN                 ##")
  print("#" * 70)
  print(f"-> Tiempo total transcurrido: {elapsed_min:.2f} minutos")
  print(f"-> Corridas completadas con éxito: {total_runs - len(failed_runs)}/{total_runs}")

  if failed_runs:
    print(f"[!] Se registraron {len(failed_runs)} ejecuciones fallidas:")
    for run_id, err in failed_runs:
      print(f"    - {run_id}: {err}")
  else:
    print("[✓✓] Todas las corridas (300/300) finalizaron exitosamente.")


if __name__ == "__main__":
  run_ablation_study()