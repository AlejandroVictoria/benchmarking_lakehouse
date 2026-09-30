# iceberg_sink.py
import os
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType
)


class IcebergLakehouseSink:
    def __init__(
        self,
        catalog_uri: str = "http://localhost:8181",
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "admin",
        s3_secret_key: str = "password123",
        namespace: str = "zacatenco",
        table_name: str = "telemetria_trolebuses",
    ):
        self.namespace = namespace
        self.table_identifier = f"{namespace}.{table_name}"

        uri = catalog_uri or os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        s3_ep = s3_endpoint or os.getenv("S3_ENDPOINT", "http://localhost:9000")
        s3_ak = s3_access_key or os.getenv("AWS_ACCESS_KEY_ID", "admin")
        s3_sk = s3_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

        self.catalog = load_catalog(
            "lakehouse",
            **{
                "type": "rest",
                "uri": uri,
                "s3.endpoint": s3_ep,
                "s3.access-key-id": s3_ak,
                "s3.secret-access-key": s3_sk,
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )

        self._ensure_table()

    def _ensure_table(self):
        self.catalog.create_namespace_if_not_exists(self.namespace)

        # Esquema alineado estrictamente con ArrowTransformer y Lifecycle
        schema_iceberg = Schema(
            NestedField(1, "trolebus_id", StringType(), required=True),
            NestedField(2, "step", IntegerType(), required=True),
            NestedField(3, "latitud", DoubleType(), required=False),
            NestedField(4, "longitud", DoubleType(), required=False),
            NestedField(5, "velocidad_kmh", FloatType(), required=False),
            NestedField(6, "distancia_recorrida", DoubleType(), required=False),
            NestedField(7, "pasajeros_a_bordo", IntegerType(), required=False),
            NestedField(8, "proxima_estacion", StringType(), required=False),
            NestedField(9, "event_time", TimestampType(), required=False),
            NestedField(10, "ingest_time", TimestampType(), required=True),
            NestedField(11, "es_sintetico", BooleanType(), required=True),
        )

        try:
            self.table = self.catalog.load_table(self.table_identifier)
        except Exception:
            self.table = self.catalog.create_table(
                self.table_identifier, schema=schema_iceberg
            )

    def append_batch(self, table_arrow: pa.Table) -> int:
        """Escribe archivos Parquet en MinIO y realiza el commit del snapshot vía REST Catalog."""
        if table_arrow.num_rows == 0:
            return 0

        try:
            # Append síncrono al catálogo Iceberg
            self.table.append(table_arrow)
            return table_arrow.nbytes
        except Exception as e:
            print(f"[IcebergSink ERROR] Fallo al comitear lote a Iceberg: {e}")
            raise e