# consumer/sink_factory.py
from typing import Optional

from consumer.iceberg_sink import IcebergLakehouseSink
from consumer.raw_parquet_sink import RawParquetSink
from benchmarking.config.treatment import SinkType


def get_lakehouse_sink(
    sink_type: SinkType = "iceberg",
    entity_type: str = "trolebuses",
    table_name: Optional[str] = None,
    raw_path: Optional[str] = None,
    namespace: str = "zacatenco",
):
  """Instancia el sink correspondiente según el modo del tratamiento y la entidad."""
  if sink_type == "iceberg":
    target_table = table_name or f"telemetria_{entity_type}"
    return IcebergLakehouseSink(namespace=namespace, table_name=target_table)
  elif sink_type == "raw":
    target_path = raw_path or f"raw_parquet/zacatenco/{entity_type}"
    return RawParquetSink(base_path=target_path)
  else:
    raise ValueError(
        f"Tipo de sink desconocido: {sink_type}. Opciones: 'iceberg', 'raw'"
    )