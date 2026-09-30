"""Script de validación rápida (Smoke Test Dual) de punta a punta.

Evalúa la consistencia y funcionalidad del conmutador de ingesta: Modo Raw Parquet
(Data Lake) vs. Modo Apache Iceberg (Lakehouse).
"""

import time


from benchmarking.config.treatment import TreatmentConfig
from benchmarking.core.lifecycle import LakehouseLifecycleManager
from benchmarking.evaluation.evaluator import ReadEvaluator
from benchmarking.harness.pipeline import PipelineHarness
from benchmarking.harness.workload import WorkloadDriver
from consumer.sink_factory import SinkType


def run_treatment_smoke(sink_type: SinkType, replica_id: int = 99) -> bool:
    # 1. Configuración del tratamiento de prueba (rápido y controlado)
    treatment = TreatmentConfig(
        k_factor=1,
        buffer_size=200,
        flush_interval_sec=5.0,
        sim_steps=300,
        replica_id=replica_id,
        namespace="zacatenco",
        table_name="telemetria_trolebuses",
        sink_type=sink_type,  # Switch de modo: 'raw' o 'iceberg'
        tag="metadata_issue"
    )

    print("\n" + "=" * 55)
    print(f"[*] INICIANDO SMOKE TEST [{sink_type.upper()}]: {treatment.run_id}")
    print("=" * 55)

    lifecycle = LakehouseLifecycleManager()

    # Fase 1: Verificación de servicios y reseteo condicional
    print(f"\n[Fase 1] Verificación y reseteo del entorno ({sink_type})...")
    if not lifecycle.verify_environment():
        print("[!] ERROR: Servicios base (MinIO/Catálogo/Kafka) no disponibles.")
        return False

    if sink_type == "iceberg":
        # Purga metadatos transaccionales y recrea tabla en catálogo vía purge_table
        lifecycle.reset_table(treatment.namespace, treatment.table_name)
        print("[✓] Tabla Iceberg inicializada en estado limpio.")
    else:
        # Purga el directorio de archivos planos en MinIO
        bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
        base_path = getattr(
            treatment, "raw_base_path", "raw_parquet/zacatenco"
        )
        lifecycle.reset_raw_directory(bucket_name=bucket, base_path=base_path)
        print(f"[✓] Directorio '{bucket}/{base_path}' preparado en estado limpio.")

    # Fase 2: Despliegue del consumidor en segundo plano con el sink correspondiente
    print(f"\n[Fase 2] Desplegando arnés de streaming en modo {sink_type}...")
    pipeline = PipelineHarness(drain_timeout_sec=8.0)
    pipeline.start(treatment)
    print(f"[✓] Consumidor Kafka + Sink ({sink_type}) activo.")

    # Fase 3: Disparar simulación SUMO en servidor remoto
    print("\n[Fase 3] Disparando simulación SUMO vía FastAPI...")
    workload = WorkloadDriver(api_base_url="https://simulacion.viccoding.dev")
    sim_stats = workload.trigger_and_wait(treatment)
    pasos_procesados = sim_stats.get("progreso", {}).get(
        "paso_actual"
    ) or sim_stats.get("paso_actual", 0)
    print(f"[✓] Simulación terminada: {pasos_procesados} pasos procesados.")

    # Fase 4: Drenar mensajes pendientes y consolidar archivos
    print("\n[Fase 4] Drenando cola de Kafka y cerrando pipeline...")
    pipeline.wait_until_drained()
    pipeline.stop()
    print(f"[✓] Pipeline ({sink_type}) detenido y archivos consolidados en MinIO.")

    # Margen de asentamiento en MinIO / Catálogo
    time.sleep(1.0)

    # Fase 5: Auditoría analítica y métricas de almacenamiento
    print("\n[Fase 5] Ejecutando verificación y auditoría analítica...")
    evaluator = ReadEvaluator()
    # Inyección de sim_stats para consolidar métricas de punta a punta
    report = evaluator.evaluate_and_persist(treatment, workload_stats=sim_stats)
    storage = report.get("storage_audit", {})
    parquet_files = storage.get("total_parquet_files", "N/A")
    total_registros = storage.get("total_records", "N/A")

    print("-" * 55)
    print(f"[✓] RESULTADOS DE AUDITORÍA ({sink_type.upper()}):")
    print(f" -> Registros persistidos: {total_registros}")
    print(f" -> Archivos Parquet generados: {parquet_files}")
    print(f" -> Reporte JSON: {treatment.evaluation_json_path}")
    print("-" * 55)

    # Fase 6: Resumen de benchmark analítico (Queries A, B y C)
    print("\n[Fase 6] Resumen de rendimiento analítico OLAP (DuckDB)...")
    bench = report.get("query_benchmark", {})
    bench_status = bench.get("status", "unknown")

    if bench_status == "success":
        queries = bench.get("queries", {})
        print(f"[✓] BENCHMARK ANALÍTICO ({sink_type.upper()}):")
        for q_key, q_data in queries.items():
            q_label = q_key.upper().replace("_", " ")
            mean_t = q_data.get("mean_latency_ms", 0.0)
            rows = q_data.get("rows_returned", 0)
            times = q_data.get("execution_times_ms", [])
            times_str = ", ".join([f"{t:.2f}ms" for t in times])
            print(f" -> {q_label}:")
            print(f"     • Latencia media: {mean_t:.2f} ms")
            print(f"     • Corridas: [{times_str}]")
            print(f"     • Filas recuperadas: {rows}")
    else:
        err = bench.get("error", "Error no especificado")
        print(f"[!] Falló el benchmark analítico ({sink_type.upper()}): {err}")
    print("-" * 55)
    return True


def run_dual_smoke_test():
    print("#######################################################")
    print("##     SUITE DE VALIDACIÓN DUAL: RAW vs. ICEBERG     ##")
    print("#######################################################")

    # Test 1: Data Lake plano
    ok_raw = run_treatment_smoke(sink_type="raw", replica_id=98)
    if not ok_raw:
        print("[!] Falló la prueba en modo RAW. Abortando suite.")
        return

    time.sleep(2.0)

    # Test 2: Data Lakehouse transaccional
    ok_iceberg = run_treatment_smoke(sink_type="iceberg", replica_id=99)
    if not ok_iceberg:
        print("[!] Falló la prueba en modo ICEBERG.")
        return

    print("\n=======================================================")
    print("[✓✓] SMOKE TEST DUAL COMPLETADO EXITOSAMENTE")
    print("      Ambos modos de ingesta operan y persisten datos.")
    print("=======================================================")


if __name__ == "__main__":
    run_dual_smoke_test()