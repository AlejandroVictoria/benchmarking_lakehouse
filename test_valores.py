import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='password123';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET unsafe_enable_version_guessing=true;
""")

# 1. ¿Cómo vienen exactamente los steps y nombres en trolebuses?
print("=== VALORES DE PROXIMA_ESTACION EN TROLEBUSES ===")
print(
    con.execute("""
    SELECT DISTINCT proxima_estacion 
    FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/trolebuses/**/*.parquet', hive_partitioning=1)
    WHERE proxima_estacion IS NOT NULL
    LIMIT 15;
""").df()
)

# 2. ¿Cómo vienen en estaciones?
print("\n=== VALORES EN ESTACIONES (parada_id y nombre_parada) ===")
print(
    con.execute("""
    SELECT DISTINCT parada_id, nombre_parada 
    FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/estaciones/**/*.parquet', hive_partitioning=1)
    LIMIT 15;
""").df()
)

# 3. ¿Coinciden los steps entre ambas tablas?
print("\n=== INTERSECCIÓN TEMPORAL DE STEPS ===")
print(
    con.execute("""
    SELECT 
        (SELECT MIN(step) FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/trolebuses/**/*.parquet', hive_partitioning=1)) AS min_step_t,
        (SELECT MAX(step) FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/trolebuses/**/*.parquet', hive_partitioning=1)) AS max_step_t,
        (SELECT MIN(step) FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/estaciones/**/*.parquet', hive_partitioning=1)) AS min_step_e,
        (SELECT MAX(step) FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/estaciones/**/*.parquet', hive_partitioning=1)) AS max_step_e
""").df()
)

# 4. Probar los dos posibles JOINs para ver cuál hace match
print("\n=== MATCH POR NOMBRE vs MATCH POR ID ===")
match_nombre = con.execute("""
    SELECT COUNT(*) 
    FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/trolebuses/**/*.parquet', hive_partitioning=1) t
    INNER JOIN read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/estaciones/**/*.parquet', hive_partitioning=1) e
        ON t.step = e.step AND t.proxima_estacion = e.nombre_parada
""").fetchone()[0]

match_id = con.execute("""
    SELECT COUNT(*) 
    FROM read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/trolebuses/**/*.parquet', hive_partitioning=1) t
    INNER JOIN read_parquet('s3://telemetria-lake/raw_parquet/zacatenco/estaciones/**/*.parquet', hive_partitioning=1) e
        ON t.step = e.step AND t.proxima_estacion = e.parada_id
""").fetchone()[0]

print(f"Coincidencias con nombre_parada: {match_nombre}")
print(f"Coincidencias con parada_id: {match_id}")