"""
Módulo de evaluación analítica post-ingesta (Read-Path) y auditoría física del Lakehouse.
Soporte dual de entidades: Trolebuses y Estaciones.
"""

import json
import re
import time
from typing import Any
import boto3
import duckdb
from pyiceberg.catalog import load_catalog, Catalog

from benchmarking.config.treatment import TreatmentConfig
from benchmarking.evaluation.duckdb_runner import DuckDBQueryRunner


class ReadEvaluator:
    """Audita el footprint de almacenamiento y mide la eficiencia analítica de Data Skipping."""

    def __init__(
        self,
        catalog_uri: str = "http://localhost:8181",
        s3_endpoint: str = "localhost:9000",
        s3_access_key: str = "admin",
        s3_secret_key: str = "password123",
        warehouse_name: str = "lakehouse",
        catalog_name: str = "rest",
    ):
        self.catalog_uri = catalog_uri
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.warehouse_name = warehouse_name

        self.catalog_properties = {
            "type": "rest",
            "uri": self.catalog_uri,
            "s3.endpoint": f"http://{self.s3_endpoint}",
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        }
        self.catalog: Catalog | None = None
        self.catalog_name = catalog_name

    def _ensure_catalog(self) -> Catalog:
        """Carga o retorna la instancia del catálogo garantizando que no sea None."""
        if self.catalog is None:
            self.catalog = load_catalog(self.catalog_name, **self.catalog_properties)

        if self.catalog is None:
            raise RuntimeError(
                f"No se pudo inicializar el catálogo '{self.catalog_name}'."
            )

        return self.catalog

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Configura y retorna una sesión de DuckDB en memoria lista para MinIO e Iceberg."""
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute(f"""
            SET s3_endpoint='{self.s3_endpoint}';
            SET s3_access_key_id='{self.s3_access_key}';
            SET s3_secret_access_key='{self.s3_secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET unsafe_enable_version_guessing=true;
        """)
        return con

    def _init_duckdb_s3_connection(self) -> duckdb.DuckDBPyConnection:
        """Inicializa una conexión DuckDB configurada con la extensión httpfs para MinIO."""
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        s3_ep = (
            self.catalog_properties.get("s3.endpoint", "http://localhost:9000")
            .replace("http://", "")
            .replace("https://", "")
        )
        use_ssl = (
            "true"
            if self.catalog_properties.get("s3.endpoint", "").startswith("https://")
            else "false"
        )

        con.execute(f"SET s3_endpoint='{s3_ep}';")
        con.execute(f"SET s3_access_key_id='{self.s3_access_key}';")
        con.execute(f"SET s3_secret_access_key='{self.s3_secret_key}';")
        con.execute(f"SET s3_use_ssl={use_ssl};")
        con.execute("SET s3_url_style='path';")
        return con

    def audit_storage_footprint(self, treatment: TreatmentConfig) -> dict[str, Any]:
        """
        Audita los archivos físicos de ambas tablas (trolebuses y estaciones) usando PyIceberg.
        Cuantifica snapshots, total de archivos Parquet y tamaño acumulado.
        """
        catalog = self._ensure_catalog()
        ns = treatment.namespace
        tbl_t_name = getattr(treatment, "table_trolebuses", "telemetria_trolebuses")
        tbl_e_name = getattr(treatment, "table_estaciones", "telemetria_estaciones")

        tables_to_audit = [("trolebuses", tbl_t_name), ("estaciones", tbl_e_name)]
        
        entities_stats: dict[str, Any] = {}
        total_files_combined = 0
        total_bytes_combined = 0
        total_records_combined = 0
        metadata_locations: dict[str, str] = {}

        for entity_label, tbl_name in tables_to_audit:
            try:
                table = catalog.load_table(f"{ns}.{tbl_name}")
                scan = table.scan()
                data_files = list(scan.plan_files())

                f_count = len(data_files)
                b_count = sum(f.file.file_size_in_bytes for f in data_files)
                snapshot = table.current_snapshot()
                r_count = int(
                    snapshot.summary.get("total-records", 0)
                    if snapshot and snapshot.summary
                    else 0
                )

                metadata_locations[entity_label] = table.metadata_location
                total_files_combined += f_count
                total_bytes_combined += b_count
                total_records_combined += r_count

                entities_stats[entity_label] = {
                    "table_name": tbl_name,
                    "metadata_location": table.metadata_location,
                    "current_snapshot_id": snapshot.snapshot_id if snapshot else None,
                    "total_snapshots": len(table.metadata.snapshots),
                    "total_parquet_files": f_count,
                    "total_size_bytes": b_count,
                    "total_size_mb": round(b_count / (1024 * 1024), 2),
                    "total_records": r_count,
                }
            except Exception as e:
                print(f"[ReadEvaluator Warning] No se pudo auditar tabla '{tbl_name}': {e}")

        avg_file_size_kb = (
            round((total_bytes_combined / total_files_combined) / 1024, 2)
            if total_files_combined > 0
            else 0.0
        )

        stats = {
            "metadata_location": metadata_locations.get("trolebuses"),
            "metadata_location_trolebuses": metadata_locations.get("trolebuses"),
            "metadata_location_estaciones": metadata_locations.get("estaciones"),
            "total_parquet_files": total_files_combined,
            "total_size_bytes": total_bytes_combined,
            "total_size_mb": round(total_bytes_combined / (1024 * 1024), 2),
            "avg_file_size_kb": avg_file_size_kb,
            "total_records": total_records_combined,
            "entities": entities_stats,
        }

        print(
            f"[ReadEvaluator] Auditoría física Iceberg: {total_files_combined} archivos Parquet "
            f"({stats['total_size_mb']} MB totales, {total_records_combined} registros consolidando trolebuses y estaciones)."
        )
        return stats

    def benchmark_data_skipping(
        self,
        treatment: TreatmentConfig,
        step_min: int = 50,
        step_max: int = 250,
        speed_threshold: float = 10.0,
    ) -> dict[str, Any]:
        """
        Ejecuta EXPLAIN ANALYZE con DuckDB para cuantificar la tasa de poda de archivos (Data Skipping).
        """
        catalog = self._ensure_catalog()
        tbl_t_name = getattr(treatment, "table_trolebuses", "telemetria_trolebuses")
        table = catalog.load_table(f"{treatment.namespace}.{tbl_t_name}")
        metadata_path = table.metadata_location

        total_files = len(list(table.scan().plan_files()))

        query = f"""
            SELECT 
                trolebus_id,
                COUNT(*) AS total_muestras,
                ROUND(AVG(velocidad_kmh), 2) AS vel_promedio,
                MAX(pasajeros_a_bordo) AS max_pasajeros
            FROM iceberg_scan('{metadata_path}')
            WHERE step BETWEEN {step_min} AND {step_max}
              AND velocidad_kmh > {speed_threshold}
            GROUP BY trolebus_id
            ORDER BY total_muestras DESC;
        """

        con = self._get_duckdb_connection()

        t0 = time.perf_counter()
        explain_result = con.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        total_query_time_ms = (time.perf_counter() - t0) * 1000

        plan_text = "\n".join([str(fila[1]) for fila in explain_result])

        files_read_match = re.search(r"Total Files Read:\s*(\d+)", plan_text)
        files_read = int(files_read_match.group(1)) if files_read_match else None

        network_in_match = re.search(r"in:\s*([\d\.]+\s*[A-Za-z]+)", plan_text)
        network_in = network_in_match.group(1) if network_in_match else "Desconocido"

        http_gets_match = re.search(r"#GET:\s*(\d+)", plan_text)
        http_gets = int(http_gets_match.group(1)) if http_gets_match else None

        skipping_ratio_pct = 0.0
        if total_files > 0 and files_read is not None:
            skipping_ratio_pct = round((1.0 - (files_read / total_files)) * 100.0, 2)

        results = {
            "query_step_window": [step_min, step_max],
            "total_files_in_table": total_files,
            "files_scanned": files_read,
            "data_skipping_ratio_pct": skipping_ratio_pct,
            "httpfs_bytes_transferred": network_in,
            "httpfs_get_requests": http_gets,
            "execution_time_ms": round(total_query_time_ms, 2),
        }

        print(
            f"[ReadEvaluator] Data Skipping: {files_read}/{total_files} archivos leídos "
            f"({skipping_ratio_pct}% podados) en {results['execution_time_ms']} ms."
        )
        return results

    def audit_raw_storage(self, treatment: TreatmentConfig) -> dict[str, Any]:
        """Audita físicamente los archivos Parquet en MinIO bajo el prefijo raíz (trolebuses + estaciones)."""
        bucket_name = getattr(treatment, "raw_bucket", "telemetria-lake")
        base_prefix = getattr(treatment, "raw_base_prefix", "raw_parquet/zacatenco")

        s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{self.s3_endpoint}",
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=base_prefix)

        total_files = 0
        total_bytes = 0

        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    total_files += 1
                    total_bytes += obj["Size"]

        # Conteo exacto de registros en Raw consolidando ambos subdirectorios particionados
        total_records = 0
        if total_files > 0:
            con = self._init_duckdb_s3_connection()
            s3_glob = f"s3://{bucket_name}/{base_prefix}/**/*.parquet"
            try:
                res = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{s3_glob}', hive_partitioning=1)"
                ).fetchone()
                total_records = res[0] if res else 0
            except Exception as e:
                print(f"[ReadEvaluator] Advertencia al contar registros en Raw: {e}")
            finally:
                con.close()

        total_size_mb = round(total_bytes / (1024 * 1024), 4)
        avg_file_size_kb = (
            round((total_bytes / total_files) / 1024, 2)
            if total_files > 0
            else 0.0
        )

        return {
            "metadata_location": None,
            "metadata_location_trolebuses": None,
            "metadata_location_estaciones": None,
            "current_snapshot_id": None,
            "total_snapshots": 0,
            "total_parquet_files": total_files,
            "total_size_bytes": total_bytes,
            "total_size_mb": total_size_mb,
            "avg_file_size_kb": avg_file_size_kb,
            "total_records": total_records,
        }

    def benchmark_raw_reads(
        self,
        treatment: TreatmentConfig,
        window_start: int = 50,
        window_end: int = 250,
    ) -> dict[str, Any]:
        """Mide latencia y data skipping consultando archivos Parquet planos en MinIO vía DuckDB."""
        bucket_name = getattr(treatment, "raw_bucket", "telemetria-lake")
        base_prefix = getattr(treatment, "raw_base_prefix", "raw_parquet/zacatenco")
        
        # Enfocado en la entidad de hechos principal: trolebuses
        s3_glob_trolebuses = f"s3://{bucket_name}/{base_prefix}/trolebuses/**/*.parquet"

        con = self._init_duckdb_s3_connection()
        t0 = time.perf_counter()
        try:
            query = f"""
                SELECT COUNT(*) 
                FROM read_parquet('{s3_glob_trolebuses}', hive_partitioning=1) 
                WHERE step BETWEEN {window_start} AND {window_end}
            """
            con.execute(query).fetchall()
            t_execution = (time.perf_counter() - t0) * 1000.0
        except Exception as e:
            print(f"[ReadEvaluator] Error durante benchmark_raw_reads: {e}")
            t_execution = 0.0
        finally:
            con.close()

        audit = self.audit_raw_storage(treatment)
        total_files = audit["total_parquet_files"]

        return {
            "query_step_window": [window_start, window_end],
            "total_files_in_table": total_files,
            "files_scanned": total_files,
            "data_skipping_ratio_pct": 0.0,
            "httpfs_bytes_transferred": "N/A",
            "httpfs_get_requests": total_files,
            "execution_time_ms": round(t_execution, 2),
        }

    def evaluate_and_persist(
        self,
        treatment: TreatmentConfig,
        workload_stats: dict[str, Any] | None = None,
        gtfs_parquet_path: str = "data/gtfs/dim_estaciones_gtfs.parquet",
    ) -> dict[str, Any]:
        """Ejecuta la auditoría física y el benchmark analítico, persistiendo el JSON final."""
        sink_type = getattr(treatment, "sink_type", "iceberg")

        if sink_type == "iceberg":
            storage_stats = self.audit_storage_footprint(treatment)
            read_stats = self.benchmark_data_skipping(treatment)
            meta_trolebuses = storage_stats.get("metadata_location_trolebuses")
            meta_estaciones = storage_stats.get("metadata_location_estaciones")
        else:
            storage_stats = self.audit_raw_storage(treatment)
            read_stats = self.benchmark_raw_reads(treatment)
            meta_trolebuses = None
            meta_estaciones = None

        runner = DuckDBQueryRunner(
            s3_endpoint=f"http://{self.s3_endpoint}",
            s3_access_key=self.s3_access_key,
            s3_secret_key=self.s3_secret_key,
        )

        query_report = runner.run_all_queries(
            treatment=treatment,
            metadata_trolebuses=meta_trolebuses,
            metadata_estaciones=meta_estaciones,
            gtfs_parquet_path=gtfs_parquet_path,
            runs=3,
        )

        combined_report: dict[str, Any] = {
            "run_id": treatment.run_id,
            "treatment": {
                "k_factor": treatment.k_factor,
                "buffer_size": treatment.buffer_size,
                "flush_interval_sec": treatment.flush_interval_sec,
                "sim_steps": treatment.sim_steps,
                "replica_id": treatment.replica_id,
                "sink_type": getattr(treatment, "sink_type", "iceberg"),
            },
            "workload_producer": (
                {
                    "pasos_solicitados": treatment.sim_steps,
                    "pasos_reales": (
                        workload_stats.get("progreso", {}).get("paso_actual")
                        if "progreso" in workload_stats
                        else workload_stats.get("paso_actual", 0)
                    ),
                    "tasa_steps_segundo": workload_stats.get("tasa_steps_segundo", 0.0),
                    "inicio_timestamp": workload_stats.get("inicio_timestamp"),
                    "completado": workload_stats.get("completado", False),
                    "detalles": workload_stats,
                }
                if workload_stats
                else {}
            ),
            "storage_audit": storage_stats,
            "read_benchmark": read_stats,
            "query_benchmark": query_report,
        }

        with open(treatment.evaluation_json_path, "w", encoding="utf-8") as f:
            json.dump(combined_report, f, indent=2)

        print(
            f"[ReadEvaluator] ✓ Reporte de evaluación guardado en: {treatment.evaluation_json_path}"
        )
        return combined_report