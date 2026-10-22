#!/usr/bin/env python3
"""
Módulo de Consolidación de Resultados de Ablación (DL-4.1)
Unifica los JSONs analíticos de 'results/eval' y las trazas CSV de 'results/ingest'.
"""

from pathlib import Path
import json
import polars as pl


def parse_ingest_metrics(csv_path: Path) -> dict:
    """Extrae métricas agregadas de streaming de un archivo CSV individual."""
    empty = {
        "batches_total": None,
        "records_total": None,
        "commit_time_mean_ms": None,
        "commit_time_std_ms": None,
        "commit_time_p50_ms": None,
        "commit_time_p95_ms": None,
        "commit_time_p99_ms": None,
        "transform_time_mean_ms": None,
        "total_cycle_mean_ms": None,
        "compression_ratio_mean": None,
        "raw_bytes_total": None,
        "arrow_bytes_total": None,
    }
    if not csv_path or not csv_path.exists():
        return empty

    try:
        df = pl.read_csv(csv_path)
        if df.height == 0:
            return empty

        return {
            "batches_total": df.height,
            "records_total": int(df["records_count"].sum()) if "records_count" in df.columns else None,
            "commit_time_mean_ms": float(df["commit_time_ms"].mean()) if "commit_time_ms" in df.columns else None,
            "commit_time_std_ms": float(df["commit_time_ms"].std()) if "commit_time_ms" in df.columns and df.height > 1 else 0.0,
            "commit_time_p50_ms": float(df["commit_time_ms"].quantile(0.50)) if "commit_time_ms" in df.columns else None,
            "commit_time_p95_ms": float(df["commit_time_ms"].quantile(0.95)) if "commit_time_ms" in df.columns else None,
            "commit_time_p99_ms": float(df["commit_time_ms"].quantile(0.99)) if "commit_time_ms" in df.columns else None,
            "transform_time_mean_ms": float(df["transform_time_ms"].mean()) if "transform_time_ms" in df.columns else None,
            "total_cycle_mean_ms": float(df["total_cycle_ms"].mean()) if "total_cycle_ms" in df.columns else None,
            "compression_ratio_mean": float(df["compression_ratio"].mean()) if "compression_ratio" in df.columns else None,
            "raw_bytes_total": int(df["raw_bytes_size"].sum()) if "raw_bytes_size" in df.columns else None,
            "arrow_bytes_total": int(df["arrow_bytes_size"].sum()) if "arrow_bytes_size" in df.columns else None,
        }
    except Exception as err:
        print(f"[!] Error leyendo {csv_path.name}: {err}")
        return empty


def extract_evaluation_record(json_path: Path, ingest_dir: Path) -> dict:
    """Extrae las variables del JSON y las enlaza con los CSV de trolebuses y estaciones."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    run_id = data.get("run_id", json_path.stem.replace("eval_", ""))
    treatment = data.get("treatment", {})
    workload = data.get("workload_producer", {}).get("detalles", {}).get("rendimiento", {})
    storage = data.get("storage_audit", {})
    read_bench = data.get("read_benchmark", {})
    queries = data.get("query_benchmark", {}).get("queries", {})

    b_size = treatment.get("buffer_size")
    flush_interval = treatment.get("flush_interval_sec")
    strategy_label = f"b={b_size} (t={flush_interval}s)" if flush_interval else f"b={b_size}"

    record = {
        "run_id": run_id,
        "sink_type": treatment.get("sink_type"),
        "k_factor": treatment.get("k_factor"),
        "buffer_size": b_size,
        "flush_interval_sec": treatment.get("flush_interval_sec"),
        "replica_id": treatment.get("replica_id"),
        "sim_steps": treatment.get("sim_steps"),
        "buffer_strategy": strategy_label,
        # Rendimiento SUMO
        "sumo_duration_sec": workload.get("segundos_transcurridos"),
        "sumo_steps_per_sec": workload.get("steps_por_segundo"),
        # Almacenamiento
        "total_parquet_files": storage.get("total_parquet_files"),
        "total_storage_bytes": storage.get("total_size_bytes"),
        "total_storage_mb": storage.get("total_size_mb"),
        "avg_file_size_kb": storage.get("avg_file_size_kb"),
        "total_records_stored": storage.get("total_records"),
        # Poda y lectura analítica
        "skipping_table_files": read_bench.get("total_files_in_table"),
        "skipping_files_scanned": read_bench.get("files_scanned"),
        "data_skipping_ratio_pct": read_bench.get("data_skipping_ratio_pct"),
        "httpfs_get_requests": read_bench.get("httpfs_get_requests"),
        "read_bench_latency_ms": read_bench.get("execution_time_ms"),
    }

    # Latencias analíticas DuckDB
    for q_name, q_data in queries.items():
        record[f"{q_name}_mean_ms"] = q_data.get("mean_latency_ms")
        record[f"{q_name}_cold_ms"] = (
            q_data.get("execution_times_ms", [None])[0]
            if q_data.get("execution_times_ms")
            else None
        )

    # Ingesta Trolebuses
    csv_trolebuses = ingest_dir / f"ingest_{run_id}_trolebuses.csv"
    metrics_trolebuses = parse_ingest_metrics(csv_trolebuses)
    for k, v in metrics_trolebuses.items():
        record[f"ingest_{k}"] = v  # Llave principal esperada por el plot: ingest_commit_time_mean_ms

    # Ingesta Estaciones
    csv_estaciones = ingest_dir / f"ingest_{run_id}_estaciones.csv"
    metrics_estaciones = parse_ingest_metrics(csv_estaciones)
    for k, v in metrics_estaciones.items():
        record[f"estaciones_{k}"] = v

    return record


def consolidate_suite(eval_dir: Path, ingest_dir: Path, suffix: str = ""):
    """Ejecuta la consolidación sobre un par específico de directorios eval/ingest."""
    out_dir = Path("ablation_study/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = sorted(eval_dir.glob("eval_*.json"))
    if not json_files:
        print(f"[!] No se encontraron JSONs en {eval_dir.resolve()}")
        return

    print(f"[*] Consolidando: {eval_dir.name} ({len(json_files)} reportes) con {ingest_dir.name}...")

    records = [extract_evaluation_record(jf, ingest_dir) for jf in json_files]
    df_runs = pl.DataFrame(records)

    # 1. Guardar runs crudas
    raw_name_parquet = f"ablation_runs_raw_{suffix}.parquet"
    raw_name_csv = f"ablation_runs_raw_{suffix}.csv"
    df_runs.write_parquet(out_dir / raw_name_parquet)
    df_runs.write_csv(out_dir / raw_name_csv)
    print(f"[✓] {raw_name_parquet} exportado con {df_runs.height} filas y {df_runs.width} columnas.")

    # 2. Resumen estadístico agrupado
    group_cols = ["sink_type", "k_factor", "buffer_size", "flush_interval_sec", "buffer_strategy"]
    numeric_cols = [
        col for col in df_runs.columns
        if col not in group_cols + ["run_id", "replica_id"]
        and df_runs[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
    ]

    aggregations = []
    for col in numeric_cols:
        aggregations.append(pl.col(col).mean().alias(f"{col}__mean"))
        aggregations.append(pl.col(col).std().alias(f"{col}__std"))

    df_summary = (
        df_runs.group_by(group_cols)
        .agg(pl.len().alias("replicas_count"), *aggregations)
        .sort(["sink_type", "k_factor", "buffer_size"])
    )

    summary_name_parquet = f"ablation_summary_{suffix}.parquet"
    summary_name_csv = f"ablation_summary_{suffix}.csv"
    df_summary.write_parquet(out_dir / summary_name_parquet)
    df_summary.write_csv(out_dir / summary_name_csv)
    print(f"[✓] {summary_name_parquet} exportado exitosamente.\n")


def main():
    root_results = Path("results")

    # Corrida actual (con monitor activo)
    consolidate_suite(
        eval_dir=root_results / "eval_full_run",
        ingest_dir=root_results / "ingest_full_run",
        suffix="full_run",  # Genera ablation_runs_raw.parquet y ablation_summary.parquet
    )

    # Primera corrida de referencia (sin monitor), si existe
    # eval_first = root_results / "eval_first_run"
    # ingest_first = root_results / "ingest_first_run"
    # if eval_first.exists() and ingest_first.exists():
    #     consolidate_suite(
    #         eval_dir=eval_first,
    #         ingest_dir=ingest_first,
    #         suffix="_first_run",  # Genera ablation_runs_raw_first_run.parquet
    #     )


if __name__ == "__main__":
    main()