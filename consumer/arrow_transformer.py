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

        self.schema_estaciones = pa.schema([
            pa.field("step", pa.int64(), nullable=False),
            pa.field("parada_id", pa.string(), nullable=False),
            pa.field("nombre_parada", pa.string(), nullable=True),
            pa.field("personas_esperando", pa.int32(), nullable=False),
            pa.field("event_time", pa.timestamp("us"), nullable=True),
            pa.field("ingest_time", pa.timestamp("us"), nullable=False),
            pa.field("es_sintetico", pa.bool_(), nullable=False),
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

    def _transform_trolebuses(
        self, raw_messages: list[bytes]
    ) -> tuple[pa.Table, int]:
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
                continue
        arrow_table = pa.Table.from_pylist(records, schema=self.schema_trolebuses)
        return arrow_table, total_raw_bytes

    def _transform_estaciones(
        self, raw_messages: list[bytes]
    ) -> tuple[pa.Table, int]:
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
                    "parada_id": str(item.get("parada_id", "")),
                    "nombre_parada": (
                        str(item.get("nombre_parada", ""))
                        if item.get("nombre_parada") is not None
                        else None
                    ),
                    "personas_esperando": int(item.get("personas_esperando", 0)),
                    "es_sintetico": bool(item.get("es_sintetico", False)),
                })
            except Exception:
                continue
        arrow_table = pa.Table.from_pylist(records, schema=self.schema_estaciones)
        return arrow_table, total_raw_bytes

    def transform(
        self, raw_messages: list[bytes], entity_type: str = "trolebuses"
    ) -> tuple[pa.Table, int]:
        """Deserializa y construye un RecordBatch columnar según la entidad."""
        if entity_type == "estaciones":
            return self._transform_estaciones(raw_messages)
        return self._transform_trolebuses(raw_messages)