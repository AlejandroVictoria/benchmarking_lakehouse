# arrow_transformer.py
import json
import pyarrow as pa
from datetime import datetime, timezone


class ArrowTransformer:
    def __init__(self):
        # Contrato estricto para telemetria_trolebuses
        self.schema_trolebuses = pa.schema([
            pa.field("step", pa.int32(), nullable=False),
            pa.field("event_time", pa.timestamp("us"), nullable=False),
            pa.field("ingest_time", pa.timestamp("us"), nullable=False),
            pa.field("trolebus_id", pa.string(), nullable=False),
            pa.field("es_sintetico", pa.bool_(), nullable=False),
            pa.field("pasajeros_a_bordo", pa.int32(), nullable=False),
            pa.field("velocidad_kmh", pa.float32(), nullable=False),
            pa.field("proxima_estacion", pa.string(), nullable=True),
            pa.field("distancia_recorrida", pa.float64(), nullable=False),
            pa.field("latitud", pa.float64(), nullable=False),
            pa.field("longitud", pa.float64(), nullable=False),
        ])

    def _parse_timestamp(self, ts_val) -> datetime:
        """Convierte cadenas ISO-8601 o nulos a datetime UTC aware."""
        if isinstance(ts_val, datetime):
            return ts_val
        if isinstance(ts_val, str):
            # Reemplaza sufijo Z por UTC formal para fromisoformat
            ts_clean = ts_val.replace("Z", "+00:00")
            return datetime.fromisoformat(ts_clean)
        # Fallback si viene None o formato inválido
        return datetime.now(timezone.utc)

    def transform(self, raw_messages: list[bytes]) -> tuple[pa.Table, int]:
        """Deserializa y construye un RecordBatch columnar.

        Retorna la tabla Arrow y el total de bytes crudos procesados.
        """
        total_raw_bytes = 0
        records = []

        for msg in raw_messages:
            total_raw_bytes += len(msg)
            try:
                item = json.loads(msg.decode("utf-8"))
                records.append({
                    "step": int(item.get("step", 0)),
                    "event_time": self._parse_timestamp(item.get("event_time", "")),
                    "ingest_time": self._parse_timestamp(item.get("ingest_time", "")),
                    "trolebus_id": str(item.get("trolebus_id", "")),
                    "es_sintetico": bool(item.get("es_sintetico", False)),
                    "pasajeros_a_bordo": int(item.get("pasajeros_a_bordo", 0)),
                    "velocidad_kmh": float(item.get("velocidad_kmh", 0.0)),
                    "proxima_estacion": str(item.get("proxima_estacion", "")),
                    "distancia_recorrida": float(item.get("distancia_recorrida", 0.0)),
                    "latitud": float(item.get("latitud", 0.0)),
                    "longitud": float(item.get("longitud", 0.0)),
                })
            except Exception:
                # Aquí se integraría el Dead Letter Queue si un mensaje es corrupto
                continue

        arrow_table = pa.Table.from_pylist(records, schema=self.schema_trolebuses)
        return arrow_table, total_raw_bytes