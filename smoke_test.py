"""Script de validación rápida (Smoke Test) de punta a punta.

Ejecuta una corrida corta de 300 pasos con K=1 y buffer de 200 registros.
"""

from benchmarking.config.treatment import TreatmentConfig
from benchmarking.core.lifecycle import LakehouseLifecycleManager
from benchmarking.evaluation.evaluator import ReadEvaluator
from benchmarking.harness.pipeline import PipelineHarness
from benchmarking.harness.workload import WorkloadDriver


def run_smoke_test():
    # Configuración del tratamiento de prueba (rápido y controlado)
    treatment = TreatmentConfig(
        k_factor=1,
        buffer_size=200,
        flush_interval_sec=5.0,
        sim_steps=300,
        replica_id=99,
        namespace="zacatenco",
        table_name="telemetria_trolebuses",
    )

    print("==================================================")
    print(f"[*] INICIANDO SMOKE TEST: {treatment.run_id}")
    print("==================================================")

    # 1. Reset en frío del Lakehouse
    print("\n[Fase 1] Verificación y reseteo del Lakehouse...")
    lifecycle = LakehouseLifecycleManager()
    if not lifecycle.verify_environment():
        print("[!] ERROR: Servicios base (MinIO/Catálogo) no disponibles.")
        return

    lifecycle.reset_table(treatment.namespace, treatment.table_name)
    print("[✓] Tabla Iceberg inicializada en estado limpio.")

    # 2. Levantar el pipeline de streaming en segundo plano
    print("\n[Fase 2] Desplegando arnés de streaming...")
    pipeline = PipelineHarness(drain_timeout_sec=8.0)
    pipeline.start(treatment)
    print("[✓] Consumidor Kafka + PyIceberg activo.")

    # 3. Disparar carga de trabajo vía HTTP a sim-zacatenco:v5
    print("\n[Fase 3] Disparando simulación SUMO vía FastAPI...")
    workload = WorkloadDriver(api_base_url="https://simulacion.viccoding.dev")

    # Nota: si tu broker es accesible por otra IP/nombre, indícalo aquí
    sim_stats = workload.trigger_and_wait(
        treatment
    )
    print(
        f"[✓] Simulación terminada: {sim_stats.get('progreso', {}).get('paso_actual', 0)} pasos procesados."
    )

    # 4. Drenar mensajes pendientes y detener el consumidor
    print("\n[Fase 4] Drenando cola y cerrando pipeline...")
    pipeline.wait_until_drained()
    pipeline.stop()
    print("[✓] Pipeline detenido y archivos Parquet consolidados.")

    # 5. Evaluación y auditoría analítica con DuckDB
    print("\n[Fase 5] Ejecutando consultas analíticas de auditoría...")
    evaluator = ReadEvaluator()
    report = evaluator.evaluate_and_persist(treatment)
    storage = report.get("storage_audit", {})
    parquet_files = storage.get("total_parquet_files", "N/A")
    total_registros = storage.get("total_records", "N/A")
    print("\n==================================================")
    print("[✓] SMOKE TEST COMPLETADO EXITOSAMENTE")
    print("==================================================")
    print(f"Archivo de resultados generado en: {treatment.evaluation_json_path}")
    print(f"Registros auditados en Iceberg: {total_registros}")
    print(f"Archivos Parquet creados: {parquet_files}")


if __name__ == "__main__":
  run_smoke_test()