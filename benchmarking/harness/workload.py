"""
Driver para la generación e inyección de carga de trabajo mediante FastAPI y SUMO.
"""

import time
import requests


from benchmarking.config.treatment import TreatmentConfig


class WorkloadDriver:
    """Controla el inicio y la supervisión de simulaciones en el contenedor de SUMO."""
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url.rstrip("/")
        self.start_endpoint = f"{self.api_base_url}/api/v1/experimentos/iniciar"
        self.status_endpoint = f"{self.api_base_url}/api/v1/experimentos/estado"

    def trigger_simulation(self, treatment: TreatmentConfig) -> dict:
        """Envía la solicitud POST para arrancar SUMO con los parámetros del tratamiento."""
        # Derivación de política de flush según buffer
        flush_policy = "immediate" if treatment.buffer_size <= 1 else "microbatch"
        storage_mode = getattr(treatment, "sink_type", "iceberg")

        payload = {
            "experiment_id": treatment.run_id,
            "steps": treatment.sim_steps,
            "scale_factor": treatment.k_factor,
            "storage_mode": storage_mode,
            "flush_policy": flush_policy,
            "fast_forward": True,
        }

        response = requests.post(self.start_endpoint, json=payload, timeout=10.0)
        response.raise_for_status()
        data = response.json()
        print(
            f"[WorkloadDriver] Simulación iniciada [{treatment.run_id}]:"
            f" {treatment.sim_steps} pasos, K={treatment.k_factor},"
            f" mode={storage_mode}, flush={flush_policy}."
        )
        return data

    def wait_until_completed(self, poll_interval_sec: float = 3.0) -> dict:
        """
        Sondea periódicamente el endpoint de estado hasta que la simulación finalice.
        Retorna las métricas y estado reportados por la API.
        """
        print("[WorkloadDriver] Esperando a que concluya la simulación...")
        while True:
            try:
                response = requests.get(self.status_endpoint, timeout=5.0)
                if response.status_code == 200:
                    estado = response.json()
                    # Si el servicio reporta que ya no está activo
                    if not estado.get("activo", False):
                        print("[WorkloadDriver] ✓ Simulación SUMO concluida.")
                        return estado
                    
                    paso_actual = estado.get("paso_actual", 0)
                    total_pasos = estado.get("duracion_pasos", 0)
                    print(f"  [Progreso] Paso {paso_actual}/{total_pasos}...", end="\r")
            except requests.RequestException as e:
                print(f"[WorkloadDriver Warning] Error sondeando estado: {e}")

            time.sleep(poll_interval_sec)

    def trigger_and_wait(self, treatment: TreatmentConfig, poll_interval_sec: float = 3.0) -> dict:
        """Secuencia combinada: dispara la carga y bloquea hasta su finalización."""
        self.trigger_simulation(treatment)
        # Margen breve para que el thread en FastAPI pase a estado activo
        time.sleep(1.0)
        return self.wait_until_completed(poll_interval_sec=poll_interval_sec)