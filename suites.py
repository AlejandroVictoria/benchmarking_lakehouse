"""Script ejecutable principal para orquestar la suite de experimentos."""

import json
import time
from benchmark.config.treatment import TreatmentConfig
from benchmark.core.lifecycle import LakehouseLifecycleManager
from benchmark.evaluation.evaluator import ReadEvaluator
from benchmark.harness.pipeline import PipelineHarness
from benchmark.harness.workload import WorkloadDriver


class BenchmarkOrchestrator:
  """Coordina el reseteo, ingesta streaming, generación de carga y evaluación analítica."""

  def __init__(self):
    self.lifecycle = LakehouseLifecycleManager()
    self.pipeline = PipelineHarness(drain_timeout_sec=15.0)
    self.workload = WorkloadDriver(api_base_url="http://localhost:8000")
    self.evaluator = ReadEvaluator()

  def run_treatment(self, treatment: TreatmentConfig) -> dict:
    print(f"\n{'='*80}")
    print(f"EJECUTANDO TRATAMIENTO: {treatment.run_id}")
    print(
        f"Parámetros: K={treatment.k_factor} | Buffer={treatment.buffer_size} |"
        f" Pasos={treatment.sim_steps}"
    )
    print(f"{'='*80}")

    # 1. Reseteo en frío del Lakehouse y verificación de servicios
    self.lifecycle.reset_table(treatment.namespace, treatment.table_name)

    # 2. Iniciar el pipeline de consumo en segundo plano
    self.pipeline.start(treatment)

    # 3. Disparar y esperar a que SUMO complete la simulación
    t0_sim = time.perf_counter()
    sim_stats = self.workload.trigger_and_wait(treatment)
    sim_duration_sec = time.perf_counter() - t0_sim

    # 4. Esperar el drenado de Kafka y apagar el consumidor de forma limpia
    self.pipeline.wait_until_drained()
    self.pipeline.stop()

    # 5. Auditoría de almacenamiento y evaluación de Data Skipping (Read-Path)
    evaluation_report = self.evaluator.evaluate_and_persist(treatment)

    treatment_summary = {
        "run_id": treatment.run_id,
        "sim_duration_sec": round(sim_duration_sec, 2),
        "simulation_report": sim_stats,
        "evaluation_report": evaluation_report,
    }
    return treatment_summary

  def run_suite(self, treatments: list[TreatmentConfig]) -> list[dict]:
    if not self.lifecycle.verify_environment():
      raise RuntimeError(
          "Infraestructura no disponible. Verifica contenedores Docker."
      )

    suite_results = []
    for treatment in treatments:
      result = self.run_treatment(treatment)
      suite_results.append(result)

    # Guardar resumen global de la suite
    summary_path = "results/suite_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
      json.dump(suite_results, f, indent=2)

    print(f"\n[✓] Suite completada exitosamente. Resumen en: {summary_path}")
    return suite_results


if __name__ == "__main__":
  # Matriz de prueba rápida preliminar
  matriz_experimental = [
      TreatmentConfig(
          k_factor=5,
          buffer_size=500,
          flush_interval_sec=5.0,
          sim_steps=1200,
          replica_id=1,
      ),
      TreatmentConfig(
          k_factor=10,
          buffer_size=1000,
          flush_interval_sec=10.0,
          sim_steps=1200,
          replica_id=1,
      ),
  ]

  orchestrator = BenchmarkOrchestrator()
  orchestrator.run_suite(matriz_experimental)