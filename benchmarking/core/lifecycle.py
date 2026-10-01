"""Gestor del ciclo de vida e inicialización de estado para Apache Iceberg y MinIO."""

import socket
import time
from typing import Any, Optional
import pyarrow.fs as pafs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


class LakehouseLifecycleManager:
    """Administra la verificación de salud, purga y recreación de tablas en el Lakehouse."""

    def __init__(
        self,
        catalog_uri: str = "http://localhost:8181",
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "admin",
        s3_secret_key: str = "password123",
        warehouse_name: str = "lakehouse",
    ):
        self.catalog_uri = catalog_uri
        self.s3_endpoint = s3_endpoint
        self.warehouse_name = warehouse_name

        self.catalog_properties: dict[str, Any] = {
            "type": "rest",
            "uri": self.catalog_uri,
            "s3.endpoint": self.s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        }
        self.catalog = None

    def connect(self) -> None:
        """Inicializa la conexión con el catálogo REST."""
        self.catalog = load_catalog(
            self.warehouse_name, **self.catalog_properties
        )

    def verify_service_health(
        self, host: str, port: int, timeout_sec: float = 3.0
    ) -> bool:
        """Verifica si un socket TCP responde en el host y puerto especificados."""
        try:
            with socket.create_connection((host, port), timeout=timeout_sec):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    def verify_environment(self) -> bool:
        """Comprueba la disponibilidad de MinIO S3, REST Catalog y Kafka Broker."""
        services = [
            ("MinIO S3", "localhost", 9000),
            ("Iceberg REST Catalog", "localhost", 8181),
            ("Kafka Broker", "100.111.214.11", 9092),
        ]
        all_ok = True
        for name, host, port in services:
            status = self.verify_service_health(host, port)
            symbol = "✓" if status else "✗"
            print(f"[{symbol}] Servicio {name} ({host}:{port})")
            if not status:
                all_ok = False
        return all_ok

    @staticmethod
    def get_trolebuses_schema() -> Schema:
        """Retorna la especificación formal del esquema tipado para telemetría vehicular."""
        return Schema(
            NestedField(
                field_id=1,
                name="trolebus_id",
                field_type=StringType(),
                required=True,
            ),
            NestedField(
                field_id=2, name="step", field_type=LongType(), required=True
            ),
            NestedField(
                field_id=3,
                name="latitud",
                field_type=DoubleType(),
                required=False,
            ),
            NestedField(
                field_id=4,
                name="longitud",
                field_type=DoubleType(),
                required=False,
            ),
            NestedField(
                field_id=5,
                name="velocidad_kmh",
                field_type=FloatType(),
                required=False,
            ),
            NestedField(
                field_id=6,
                name="distancia_recorrida",
                field_type=DoubleType(),
                required=False,
            ),
            NestedField(
                field_id=7,
                name="pasajeros_a_bordo",
                field_type=IntegerType(),
                required=False,
            ),
            NestedField(
                field_id=8,
                name="proxima_estacion",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=9,
                name="event_time",
                field_type=TimestampType(),
                required=False,
            ),
            NestedField(
                field_id=10,
                name="ingest_time",
                field_type=TimestampType(),
                required=True,
            ),
            NestedField(
                field_id=11,
                name="es_sintetico",
                field_type=BooleanType(),
                required=True,
            ),
        )

    # Alias de compatibilidad hacia atrás
    get_silver_schema = get_trolebuses_schema

    @staticmethod
    def get_estaciones_schema() -> Schema:
        """Retorna la especificación formal del esquema tipado para infraestructura (estaciones)."""
        return Schema(
            NestedField(
                field_id=1, name="step", field_type=LongType(), required=True
            ),
            NestedField(
                field_id=2, name="parada_id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=3,
                name="nombre_parada",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=4,
                name="personas_esperando",
                field_type=IntegerType(),
                required=True,
            ),
            NestedField(
                field_id=5,
                name="event_time",
                field_type=TimestampType(),
                required=False,
            ),
            NestedField(
                field_id=6,
                name="ingest_time",
                field_type=TimestampType(),
                required=True,
            ),
            NestedField(
                field_id=7,
                name="es_sintetico",
                field_type=BooleanType(),
                required=True,
            ),
        )

    def reset_table(
        self,
        namespace: str,
        table_name: str,
        schema: Optional[Schema] = None,
    ) -> None:
        """Ejecuta un DROP TABLE PURGE para borrar datos y metadatos, y recrea la tabla vacía."""
        if self.catalog is None:
            self.connect()
        assert self.catalog is not None

        # 1. Asegurar namespace
        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        table_identifier = f"{namespace}.{table_name}"

        # 2. Purgar tabla previa si existe
        try:
            try:
                self.catalog.purge_table(table_identifier)
            except TypeError:
                self.catalog.drop_table(table_identifier)

            print(f"[Lifecycle] Tabla '{table_identifier}' purgada exitosamente.")
        except NoSuchTableError:
            print(f"[Lifecycle] Tabla '{table_identifier}' no existía previamente.")

        time.sleep(0.5)

        # 3. Determinar esquema por defecto según el nombre de la tabla
        target_schema = schema
        if target_schema is None:
            if "estacion" in table_name:
                target_schema = self.get_estaciones_schema()
            else:
                target_schema = self.get_trolebuses_schema()

        self.catalog.create_table(
            identifier=table_identifier,
            schema=target_schema,
            properties=self.catalog_properties,
        )
        print(f"[Lifecycle] Tabla '{table_identifier}' recreada en estado limpio.")

    def reset_raw_directory(
        self,
        bucket_name: str = "telemetria-lake",
        base_path: str = "raw_parquet/zacatenco",
    ) -> None:
        """Purga los archivos Parquet del modo crudo para garantizar aislamiento en ablación."""
        s3_ep = self.catalog_properties.get("s3.endpoint", "http://localhost:9000")
        endpoint_override = (
            s3_ep.replace("http://", "").replace("https://", "").rstrip("/")
        )
        scheme = "http" if not s3_ep.startswith("https://") else "https"

        fs = pafs.S3FileSystem(
            access_key=self.catalog_properties.get("s3.access-key-id", "admin"),
            secret_key=self.catalog_properties.get("s3.secret-access-key", "password123"),
            endpoint_override=endpoint_override,
            scheme=scheme,
            region="us-east-1",
            force_virtual_addressing=False,
        )

        clean_path = base_path.strip("/")
        target = f"{bucket_name}/{clean_path}"

        # 1. Asegurar que el bucket exista
        try:
            fs.create_dir(bucket_name)
        except Exception:
            pass

        # 2. Purgar contenido recursivamente
        try:
            file_info = fs.get_file_info(target)
            if file_info.type != pafs.FileType.NotFound:
                fs.delete_dir_contents(target)
                print(f"[Lifecycle] Directorio crudo '{target}' purgado exitosamente.")
            else:
                fs.create_dir(target)
                print(f"[Lifecycle] Directorio crudo '{target}' inicializado en estado limpio.")
        except Exception as e:
            try:
                fs.create_dir(target)
                print(f"[Lifecycle] Directorio crudo '{target}' preparado.")
            except Exception:
                print(f"[Lifecycle Warning] No se pudo purgar '{target}': {e}")

    def reset_for_treatment(self, treatment: Any) -> None:
        """Punto de entrada orquestador para resetear el entorno según el tratamiento."""
        if treatment.sink_type == "raw":
            # Purgar el prefijo raíz limpia tanto trolebuses como estaciones y subdirectorios date=*
            raw_base = getattr(treatment, "raw_base_prefix", "raw_parquet/zacatenco")
            raw_bucket = getattr(treatment, "raw_bucket", "telemetria-lake")
            self.reset_raw_directory(bucket_name=raw_bucket, base_path=raw_base)
        else:
            # En Iceberg recreamos ambas tablas en estado vacío
            self.reset_table(
                namespace=treatment.namespace,
                table_name=treatment.table_trolebuses,
                schema=self.get_trolebuses_schema(),
            )
            self.reset_table(
                namespace=treatment.namespace,
                table_name=treatment.table_estaciones,
                schema=self.get_estaciones_schema(),
            )