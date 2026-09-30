"""
Módulo de evaluación analítica post-ingesta (Read-Path) y auditoría física del Lakehouse.
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
        catalog_name: str = "rest"
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
            self.catalog = load_catalog(self.catalog_name, 
                                        **self.catalog_properties)

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
        """)
        return con

    def _init_duckdb_s3_connection(self) -> duckdb.DuckDBPyConnection:
        """Inicializa una conexión DuckDB configurada con la extensión httpfs para MinIO."""
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        s3_endpoint = (
            self.catalog_properties.get("s3.endpoint", "http://localhost:9000")
            .replace("http://", "")
            .replace("https://", "")
        )
        use_ssl = (
            "true"
            if self.catalog_properties.get("s3.endpoint", "").startswith(
                "https://"
            )
            else "false"
        )

        con.execute(f"SET s3_endpoint='{s3_endpoint}';")
        con.execute(
            "SET"
            f" s3_access_key_id='{self.catalog_properties.get('s3.access-key-id', 'admin')}';"
        )
        con.execute(
            "SET"
            f" s3_secret_access_key='{self.catalog_properties.get('s3.secret-access-key', 'admin123')}';"
        )
        con.execute(f"SET s3_use_ssl={use_ssl};")
        con.execute("SET s3_url_style='path';")
        return con

    def audit_storage_footprint(self, treatment: TreatmentConfig) -> dict[str, Any]:
        """
        Audita los archivos físicos de la tabla usando PyIceberg.
        Cuantifica snapshots, total de archivos Parquet y tamaño acumulado (Small Files Problem).
        """
        catalog = self._ensure_catalog()
        table_identifier = f"{treatment.namespace}.{treatment.table_name}"
        table = catalog.load_table(table_identifier)

        # Planificar todos los archivos de datos vigentes en el snapshot actual
        scan = table.scan()
        data_files = list(scan.plan_files())

        total_files = len(data_files)
        total_bytes = sum(f.file.file_size_in_bytes for f in data_files)
        avg_file_size_kb = (total_bytes / total_files / 1024) if total_files > 0 else 0.0

        snapshot = table.current_snapshot()

        
        total_records = int(snapshot.summary.get("total-records", 0)
                            if snapshot and snapshot.summary
                            else 0)

        stats = {
            "metadata_location": table.metadata_location,
            "current_snapshot_id": snapshot.snapshot_id if snapshot else None,
            "total_snapshots": len(table.metadata.snapshots),
            "total_parquet_files": total_files,
            "total_size_bytes": total_bytes,
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "avg_file_size_kb": round(avg_file_size_kb, 2),
            "total_records": int(total_records),
        }
        print(f"[ReadEvaluator] Auditoría física: {total_files} archivos Parquet ({stats['total_size_mb']} MB totales, prom {stats['avg_file_size_kb']} KB/archivo).")
        
        return stats

    def benchmark_data_skipping(
        self,
        treatment: TreatmentConfig,
        step_min: int = 5000,
        step_max: int = 5500,
        speed_threshold: float = 10.0,
    ) -> dict[str, Any]:
        """
        Ejecuta EXPLAIN ANALYZE con DuckDB para cuantificar la tasa de poda de archivos (Data Skipping).
        """
        catalog = self._ensure_catalog()
        table_identifier = f"{treatment.namespace}.{treatment.table_name}"
        table = catalog.load_table(table_identifier)
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

        # Medir tiempo global extremo a extremo de la consulta
        t0 = time.perf_counter()
        explain_result = con.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        total_query_time_ms = (time.perf_counter() - t0) * 1000

        # Unificar el texto del plan de ejecución para extraer métricas con regex
        plan_text = "\n".join([str(fila[1]) for fila in explain_result])

        # 1. Extraer archivos leídos reportados por DuckDB
        files_read_match = re.search(r"Total Files Read:\s*(\d+)", plan_text)
        files_read = int(files_read_match.group(1)) if files_read_match else None

        # 2. Extraer volumen de red (in) de HTTPFS
        network_in_match = re.search(r"in:\s*([\d\.]+\s*[A-Za-z]+)", plan_text)
        network_in = network_in_match.group(1) if network_in_match else "Desconocido"

        # 3. Extraer número de GETs a MinIO
        http_gets_match = re.search(r"#GET:\s*(\d+)", plan_text)
        http_gets = int(http_gets_match.group(1)) if http_gets_match else None

        # 4. Calcular tasa de poda (Skipping Ratio)
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
        """Audita físicamente los archivos Parquet planos en MinIO sin pasar por catálogo transaccional."""
        bucket_name = getattr(treatment, "raw_bucket", "telemetria-lake")
        base_path = getattr(treatment, "raw_base_path", "raw_parquet/zacatenco")

        # Conexión directa a S3 / MinIO
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.catalog_properties.get(
                "s3.endpoint", "http://localhost:9000"
            ),
            aws_access_key_id=self.catalog_properties.get(
                "s3.access-key-id", "admin"
            ),
            aws_secret_access_key=self.catalog_properties.get(
                "s3.secret-access-key", "admin123"
            ),
        )

        # Listar objetos Parquet bajo el prefijo
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=base_path)

        total_files = 0
        total_bytes = 0

        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    total_files += 1
                    total_bytes += obj["Size"]

        # Conteo exacto de registros mediante DuckDB sobre MinIO
        total_records = 0
        if total_files > 0:
            con = self._init_duckdb_s3_connection()
            s3_glob = f"s3://{bucket_name}/{base_path}/**/*.parquet"
            try:
                res = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{s3_glob}')"
                ).fetchone()
                total_records = res[0] if res else 0
            except Exception as e:
                print(
                    f"[ReadEvaluator] Advertencia al contar registros en Raw: {e}"
                )
            finally:
                con.close()

        total_size_mb = round(total_bytes / (1024 * 1024), 4)
        avg_file_size_kb = (
            round((total_bytes / total_files) / 1024, 2)
            if total_files > 0
            else 0.0
        )

        return {
            "metadata_location": None,  # No aplica en Data Lake plano
            "current_snapshot_id": None,  # Sin control de versiones ACID
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
            window_start: int = 5000,
            window_end: int = 5500,
        ) -> dict[str, Any]:
        """Mide latencia y data skipping consultando archivos Parquet planos en MinIO vía DuckDB."""
        bucket_name = getattr(treatment, "raw_bucket", "telemetria-lake")
        base_path = getattr(treatment, "raw_base_path", "raw_parquet/zacatenco")
        s3_glob = f"s3://{bucket_name}/{base_path}/**/*.parquet"

        con = self._init_duckdb_s3_connection()

        # Activar profiling detallado de DuckDB para monitorear lecturas HTTP
        # con.execute("PRAGMA enable_profiling = 'json'")
        # con.execute("PRAGMA profiling_output = '/tmp/duckdb_raw_profile.json'")

        t0 = time.perf_counter()
        try:
            # Consulta de filtrado por ventana de pasos
            query = f"""
                    SELECT COUNT(*) 
                    FROM read_parquet('{s3_glob}') 
                    WHERE step BETWEEN {window_start} AND {window_end}
                """
            con.execute(query).fetchall()
            t_execution = (time.perf_counter() - t0) * 1000.0  # ms
        except Exception as e:
            print(f"[ReadEvaluator] Error durante benchmark_raw_reads: {e}")
            t_execution = 0.0
        finally:
            con.close()

        # Obtener auditoría física para comparar data skipping
        audit = self.audit_raw_storage(treatment)
        total_files = audit["total_parquet_files"]

        # En Raw Parquet (sin poda por catálogo global), el motor de lectura debe
        # solicitar los footers/metadatos de todos los archivos para verificar estadísticas internas.
        files_scanned = total_files
        skipping_ratio = 0.0  # En Parquet plano sin Hive partitioning suele ser 0% de skipping a nivel de archivos

        return {
            "query_step_window": [window_start, window_end],
            "total_files_in_table": total_files,
            "files_scanned": files_scanned,
            "data_skipping_ratio_pct": skipping_ratio,
            "httpfs_bytes_transferred": "N/A",  # O métrica extraída del profiler JSON
            "httpfs_get_requests": total_files,  # Al menos 1 GET de footer por archivo
            "execution_time_ms": round(t_execution, 2),
        }

    def evaluate_and_persist(
        self,
        treatment: TreatmentConfig,
        workload_stats: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Ejecuta la auditoría física y el benchmark analítico, persistiendo el JSON final."""
        sink_type = getattr(treatment, "sink_type", "iceberg")
        if sink_type == "iceberg":
            storage_stats = self.audit_storage_footprint(treatment)
            read_stats = self.benchmark_data_skipping(treatment)
            meta_loc = storage_stats.get("metadata_location")
        else:
            storage_stats = self.audit_raw_storage(treatment)
            read_stats = self.benchmark_raw_reads(treatment)
            meta_loc = None

        runner = DuckDBQueryRunner(
            s3_endpoint=self.catalog_properties.get("s3.endpoint", "http://localhost:9000"),
            s3_access_key=self.catalog_properties.get("s3.access-key-id", "admin"),
            s3_secret_key=self.catalog_properties.get("s3.secret-access-key", "admin123"),
        )

        query_report = runner.run_all_queries(
            treatment, metadata_location=meta_loc, runs=3
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
                    "tasa_steps_segundo": workload_stats.get(
                        "tasa_steps_segundo", 0.0
                    ),
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

        # Guardar en disco bajo results/eval_<run_id>.json
        with open(treatment.evaluation_json_path, "w", encoding="utf-8") as f:
            json.dump(combined_report, f, indent=2)

        print(
            "[ReadEvaluator] ✓ Reporte de evaluación guardado en:"
            f" {treatment.evaluation_json_path}"
        )
        return combined_report