# metrics_tracker.py
import csv
import os
import time
from dataclasses import asdict, dataclass


@dataclass
class BatchMetrics:
    batch_id: int
    records_count: int
    buffer_wait_ms: float
    transform_time_ms: float
    commit_time_ms: float
    total_cycle_ms: float
    raw_bytes_size: int
    arrow_bytes_size: int
    compression_ratio: float  # raw_bytes / arrow_bytes


class MetricsTracker:
    def __init__(self, output_path: str = "metricas_experimento.csv"):
        self.output_path = output_path
        self.batch_counter = 0
        self._init_csv()

    def _init_csv(self):
        if not os.path.exists(self.output_path):
            with open(self.output_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                "batch_id",
                "timestamp",
                "records_count",
                "buffer_wait_ms",
                "transform_time_ms",
                "commit_time_ms",
                "total_cycle_ms",
                "raw_bytes_size",
                "arrow_bytes_size",
                "compression_ratio",
                ])

    def log_batch(
        self,
        records_count: int,
        buffer_wait_ms: float,
        transform_time_ms: float,
        commit_time_ms: float,
        raw_bytes_size: int,
        arrow_bytes_size: int,
        ) -> BatchMetrics:
        self.batch_counter += 1
        total_cycle_ms = buffer_wait_ms + transform_time_ms + commit_time_ms
        ratio = (
            round(raw_bytes_size / arrow_bytes_size, 3)
            if arrow_bytes_size > 0
            else 1.0
            )

        metric = BatchMetrics(
            batch_id=self.batch_counter,
            records_count=records_count,
            buffer_wait_ms=round(buffer_wait_ms, 2),
            transform_time_ms=round(transform_time_ms, 2),
            commit_time_ms=round(commit_time_ms, 2),
            total_cycle_ms=round(total_cycle_ms, 2),
            raw_bytes_size=raw_bytes_size,
            arrow_bytes_size=arrow_bytes_size,
            compression_ratio=ratio,
            )

        with open(self.output_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
            metric.batch_id,
            time.time(),
            metric.records_count,
            metric.buffer_wait_ms,
            metric.transform_time_ms,
            metric.commit_time_ms,
            metric.total_cycle_ms,
            metric.raw_bytes_size,
            metric.arrow_bytes_size,
            metric.compression_ratio,
            ])

        return metric