# consumer/raw_parquet_sink.py
import os
import uuid
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from typing import Optional


class RawParquetSink: 
    def __init__(
        self,
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        bucket_name: str = "telemetria-lake",
        base_path: str = "raw_parquet/zacatenco",
        ):
        self.bucket_name = bucket_name
        self.base_path = base_path.strip("/")

        s3_ep = s3_endpoint or os.getenv("S3_ENDPOINT", "http://localhost:9000")
        s3_ak = s3_access_key or os.getenv("AWS_ACCESS_KEY_ID", "admin")
        s3_sk = s3_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

        # Configuración de S3FileSystem de PyArrow para MinIO
        # s3_ep puede venir con o sin http://
        endpoint_override = (
            s3_ep.replace("http://", "").replace("https://", "").rstrip("/")
        )
        scheme = "http" if not s3_ep.startswith("https://") else "https"

        self.fs = pafs.S3FileSystem(
            access_key=s3_ak,
            secret_key=s3_sk,
            endpoint_override=endpoint_override,
            scheme=scheme,
            region="us-east-1",
            force_virtual_addressing=False,  # Path-style para MinIO
        )

        self._ensure_bucket()

    def _ensure_bucket(self):
        """Crea el bucket si no existe."""
        try:
            self.fs.create_dir(self.bucket_name)
        except Exception:
            pass

    def append_batch(self, table_arrow: pa.Table) -> int:
        """Escribe directamente un archivo Parquet individual en el bucket de S3 sin catálogo transaccional."""
        if table_arrow.num_rows == 0:
            return 0

        file_id = uuid.uuid4().hex
        # Ruta: telemetria-lake/raw_parquet/zacatenco/batch_<id>.parquet
        file_path = f"{self.bucket_name}/{self.base_path}/batch_{file_id}.parquet"

        pq.write_table(
            table_arrow,
            file_path,
            filesystem=self.fs,
            compression="snappy",
        )

        return table_arrow.nbytes