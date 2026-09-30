"""
Runner analítico parametrizado usando DuckDB (SQL OLAP).
Ejecuta las Queries canónicas A, B y C sobre MinIO S3 tanto para
Data Lake crudo (read_parquet) como para Lakehouse (iceberg_scan).
"""
import time
import statistics
from typing import Any, Dict, List
import duckdb
from benchmarking.config.treatment import TreatmentConfig

class DuckDBQueryRunner:
    def __init__(
        self,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "admin",
        s3_secret_key: str = "admin123",
    ):
        self.endpoint = s3_endpoint.replace("http://", "").replace("https://", "")
        self.access_key = s3_access_key
        self.secret_key = s3_secret_key
        self.use_ssl = "true" if s3_endpoint.startswith("https") else "false"

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Crea una sesión en memoria de DuckDB con las extensiones requeridas."""
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        
        con.execute(f"SET s3_endpoint='{self.endpoint}';")
        con.execute(f"SET s3_access_key_id='{self.access_key}';")
        con.execute(f"SET s3_secret_access_key='{self.secret_key}';")
        con.execute(f"SET s3_use_ssl={self.use_ssl};")
        con.execute("SET s3_url_style='path';")

        con.execute("SET unsafe_enable_version_guessing = true;")
        return con

    def _resolve_source_expression(
        self, treatment: TreatmentConfig, metadata_location: str | None = None
    ) -> str:
        """Determina la tabla o función de origen según el tratamiento."""
        sink_type = getattr(treatment, "sink_type", "iceberg")
        if sink_type == "raw":
            bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
            base_path = getattr(treatment, "raw_base_path", "raw_parquet/zacatenco")
            return f"read_parquet('s3://{bucket}/{base_path}/**/*.parquet')"
        else:
            # Si tenemos la ubicación exacta del JSON de metadatos, la usamos directamente
            if metadata_location:
                return f"iceberg_scan('{metadata_location}')"

            # Fallback al directorio base si no se proveyó
            bucket = getattr(treatment, "s3_bucket", "telemetria-lake")
            namespace = getattr(treatment, "namespace", "zacatenco")
            table = getattr(treatment, "table_name", "telemetria_trolebuses")
            return f"iceberg_scan('s3://{bucket}/{namespace}/{table}')"

    def _benchmark_sql(
        self, con: duckdb.DuckDBPyConnection, query: str, runs: int = 3
    ) -> Dict[str, Any]:
        """Ejecuta una consulta SQL n veces y captura latencia y conteo de filas."""
        latencies: List[float] = []
        rows_count = 0

        for _ in range(runs):
            t0 = time.perf_counter()
            result = con.execute(query).fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            latencies.append(round(elapsed_ms, 2))
            rows_count = len(result)

        return {
            "execution_times_ms": latencies,
            "mean_latency_ms": round(statistics.mean(latencies), 2),
            "rows_returned": rows_count,
        }

    def run_all_queries(
        self,
        treatment: TreatmentConfig,
        metadata_location: str | None = None,
        target_trolebus: str = "trolebus_0",
        step_window: tuple[int, int] = (50, 250),
        runs: int = 3
    ) -> Dict[str, Any]:
        """Ejecuta la suite analítica completa (A, B y C)."""
        source = self._resolve_source_expression(
            treatment, metadata_location=metadata_location
        )
        con = self._get_connection()

        # Plantillas SQL con sustitución de la fuente
        query_a = f"""
            SELECT step, trolebus_id, velocidad_kmh, distancia_recorrida
            FROM {source}
            WHERE trolebus_id = '{target_trolebus}'
              AND step BETWEEN {step_window[0]} AND {step_window[1]};
        """

        query_b = f"""
            SELECT 
                proxima_estacion,
                ROUND(AVG(velocidad_kmh), 2) AS velocidad_promedio,
                MAX(pasajeros_a_bordo) AS pico_pasajeros,
                COUNT(*) AS total_reportes
            FROM {source}
            WHERE proxima_estacion IS NOT NULL AND proxima_estacion <> ''
            GROUP BY proxima_estacion
            ORDER BY pico_pasajeros DESC;
        """

        query_c = f"""
            SELECT 
                COUNT(*) AS total_eventos,
                MAX(step) AS paso_final,
                ROUND(AVG(velocidad_kmh), 2) AS velocidad_media_flota,
                ROUND(MAX(distancia_recorrida), 2) AS distancia_maxima_alcanzada
            FROM {source};
        """

        try:
            return {
                "status": "success",
                "source_mode": getattr(treatment, "sink_type", "iceberg"),
                "runs_per_query": runs,
                "queries": {
                    "query_a": self._benchmark_sql(con, query_a, runs=runs),
                    "query_b": self._benchmark_sql(con, query_b, runs=runs),
                    "query_c": self._benchmark_sql(con, query_c, runs=runs),
                }
            }
        except Exception as e:
            print(f"[DuckDBQueryRunner] Error ejecutando suite SQL: {e}")
            return {"status": "error", "error": str(e), "queries": {}}
        finally:
            con.close()