"""
Runner analítico parametrizado usando DuckDB (SQL OLAP).
Ejecuta las Queries canónicas (A, B, C, D, E y F) sobre MinIO S3 tanto para
Data Lake crudo (read_parquet) como para Lakehouse (iceberg_scan),
incorporando la dimensión estática GTFS.
"""
import time
import statistics
from typing import Any, Dict, List, Optional
import duckdb


from benchmarking.config.treatment import TreatmentConfig


class DuckDBQueryRunner:
    def __init__(
        self,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "admin",
        s3_secret_key: str = "password123",
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

    def _resolve_source_expressions(
        self,
        treatment: TreatmentConfig,
        meta_trolebuses: Optional[str] = None,
        meta_estaciones: Optional[str] = None,
    ) -> tuple[str, str]:
        """
        Determina las expresiones SQL de origen para trolebuses y estaciones
        según el modo del tratamiento (Raw vs Iceberg).
        """
        sink_type = getattr(treatment, "sink_type", "iceberg")

        if sink_type == "raw":
            bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
            prefix = getattr(treatment, "raw_base_prefix", "raw_parquet/zacatenco")
            src_trolebuses = (
                f"read_parquet('s3://{bucket}/{prefix}/trolebuses/**/*.parquet', hive_partitioning=1)"
            )
            src_estaciones = (
                f"read_parquet('s3://{bucket}/{prefix}/estaciones/**/*.parquet', hive_partitioning=1)"
            )
            return src_trolebuses, src_estaciones

        # Modo Iceberg
        if meta_trolebuses:
            src_trolebuses = f"iceberg_scan('{meta_trolebuses}')"
        else:
            bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
            ns = getattr(treatment, "namespace", "zacatenco")
            tbl_t = getattr(treatment, "table_trolebuses", "telemetria_trolebuses")
            src_trolebuses = f"iceberg_scan('s3://{bucket}/warehouse/{ns}/{tbl_t}')"

        if meta_estaciones:
            src_estaciones = f"iceberg_scan('{meta_estaciones}')"
        else:
            bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
            ns = getattr(treatment, "namespace", "zacatenco")
            tbl_e = getattr(treatment, "table_estaciones", "telemetria_estaciones")
            src_estaciones = f"iceberg_scan('s3://{bucket}/warehouse/{ns}/{tbl_e}')"

        return src_trolebuses, src_estaciones

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
        metadata_trolebuses: Optional[str] = None,
        metadata_estaciones: Optional[str] = None,
        gtfs_parquet_path: str = "data/gtfs/dim_estaciones_gtfs.parquet",
        target_trolebus: str = "f_trolebus_zacatenco.0",
        step_window: tuple[int, int] = (50, 250),
        runs: int = 3,
    ) -> Dict[str, Any]:
        """Ejecuta la suite analítica completa (A a F)."""
        src_trolebuses, src_estaciones = self._resolve_source_expressions(
            treatment,
            meta_trolebuses=metadata_trolebuses,
            meta_estaciones=metadata_estaciones,
        )
        con = self._get_connection()

        # Query A: Subconsulta dinámica para seleccionar un vehículo con datos en la ventana
        query_a = f"""
            SELECT step, trolebus_id, velocidad_kmh, distancia_recorrida
            FROM {src_trolebuses}
            WHERE trolebus_id = '{target_trolebus}'
              AND step BETWEEN {step_window[0]} AND {step_window[1]};
        """

        # Query B: Agregación grupal de telemetría vehicular
        query_b = f"""
            SELECT 
                proxima_estacion,
                ROUND(AVG(velocidad_kmh), 2) AS velocidad_promedio,
                MAX(pasajeros_a_bordo) AS pico_pasajeros,
                COUNT(*) AS total_reportes
            FROM {src_trolebuses}
            WHERE proxima_estacion IS NOT NULL AND TRIM(proxima_estacion) <> ''
            GROUP BY proxima_estacion
            ORDER BY pico_pasajeros DESC;
        """

        # Query C: Agregación global de flota
        query_c = f"""
            SELECT 
                COUNT(*) AS total_eventos,
                MAX(step) AS paso_final,
                ROUND(AVG(velocidad_kmh), 2) AS velocidad_media_flota,
                ROUND(MAX(distancia_recorrida), 2) AS distancia_maxima_alcanzada
            FROM {src_trolebuses};
        """

        # Query D: Demanda en andenes
        query_d = f"""
            SELECT 
                parada_id,
                nombre_parada,
                COUNT(*) AS total_muestreos,
                ROUND(AVG(personas_esperando), 2) AS personas_promedio,
                MAX(personas_esperando) AS max_personas_esperando
            FROM {src_estaciones}
            GROUP BY parada_id, nombre_parada
            ORDER BY max_personas_esperando DESC;
        """

        # Query E: JOIN de 2 vías temporal exacto (Aproximación vs Demanda en andén)
        query_e = f"""
            SELECT 
                t.step,
                t.trolebus_id,
                t.pasajeros_a_bordo,
                t.velocidad_kmh,
                e.parada_id,
                e.nombre_parada,
                e.personas_esperando
            FROM {src_trolebuses} t
            INNER JOIN {src_estaciones} e 
                ON t.step = e.step 
                AND TRIM(t.proxima_estacion) = TRIM(e.nombre_parada)
            WHERE t.es_sintetico = false
            ORDER BY t.step ASC, t.trolebus_id ASC;
        """

        # Query F: JOIN de 3 vías enriquecido con dimensión oficial GTFS
        query_f = f"""
            SELECT 
                t.step,
                t.trolebus_id,
                t.latitud AS lat_trolebus,
                t.longitud AS lon_trolebus,
                t.pasajeros_a_bordo,
                e.personas_esperando,
                g.stop_id AS gtfs_stop_id,
                g.stop_name,
                g.stop_lat AS lat_oficial_gtfs,
                g.stop_lon AS lon_oficial_gtfs
            FROM {src_trolebuses} t
            INNER JOIN {src_estaciones} e 
                ON t.step = e.step 
                AND TRIM(t.proxima_estacion) = TRIM(e.nombre_parada)
            LEFT JOIN read_parquet('{gtfs_parquet_path}') g
                ON TRIM(e.parada_id) = TRIM(g.parada_id)
            WHERE t.step % 10 = 0
            ORDER BY t.step ASC;
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
                    "query_d": self._benchmark_sql(con, query_d, runs=runs),
                    "query_e": self._benchmark_sql(con, query_e, runs=runs),
                    "query_f": self._benchmark_sql(con, query_f, runs=runs),
                },
            }
        except Exception as e:
            print(f"[DuckDBQueryRunner] Error ejecutando suite SQL: {e}")
            return {"status": "error", "error": str(e), "queries": {}}
        finally:
            con.close()