# consumer/sink_factory.py
from typing import Literal
from consumer.iceberg_sink import IcebergLakehouseSink
from consumer.raw_parquet_sink import RawParquetSink

SinkType = Literal["iceberg", "raw"]


def get_lakehouse_sink(sink_type: SinkType = "iceberg"):
    """Instancia el sink correspondiente según el modo del tratamiento."""
    if sink_type == "iceberg":
        return IcebergLakehouseSink()
    elif sink_type == "raw":
        return RawParquetSink()
    else:
        raise ValueError(
            f"Tipo de sink desconocido: {sink_type}. Opciones: 'iceberg', 'raw'"
        )