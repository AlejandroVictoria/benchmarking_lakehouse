"""
Driver para la generación e inyección de carga de trabajo mediante FastAPI y SUMO.
"""

import time
from typing import Any, Dict, Optional
import requests

from benchmarking.config.treatment import TreatmentConfig


class SimulationExecutionError(Exception):
    """Excepción cuando el backend de SUMO/FastAPI reporta un error de ejecución."""
    pass


class SimulationTimeoutError(Exception):
    """Excepción cuando la simulación excede el tiempo límite de sondeo."""
    pass


class WorkloadDriver:
    """Controla el inicio y la supervisión de simulaciones en el contenedor de SUMO."""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url.rstrip("/")
        self.start_endpoint = f"{self.api_base_url}/api/v1/experimentos/iniciar"
        self.status_endpoint = f"{self.api_base_url}/api/v1/experimentos/estado"
        self.stop_endpoint = f"{self.api_base_url}/api/v1/simulacion/detener"

    def trigger_simulation(self, treatment: TreatmentConfig) -> dict:
        """Envía la solicitud POST para arrancar SUMO con los parámetros del tratamiento."""
        flush_policy = "immediate" if treatment.buffer_size <= 1 else "microbatch"
        storage_mode = getattr(treatment, "sink_type", "iceberg")
        seed = getattr(treatment, "seed", 42)

        payload = {
            "experiment_id": treatment.run_id,
            "steps": treatment.sim_steps,
            "scale_factor": treatment.k_factor,
            "storage_mode": storage_mode,
            "flush_policy": flush_policy,
            "fast_forward": True,
            "seed": seed,
        }

        response = requests.post(self.start_endpoint, json=payload, timeout=10.0)
        response.raise_for_status()
        data = response.json()
        print(
            f"[WorkloadDriver] Simulación iniciada [{treatment.run_id}]:"
            f" {treatment.sim_steps} pasos, K={treatment.k_factor}, seed={seed},"
            f" mode={storage_mode}, flush={flush_policy}."
        )
        return data

    def stop_simulation(self) -> dict:
        """Fuerza la interrupción controlada de la simulación en SUMO."""
        try:
            response = requests.post(self.stop_endpoint, timeout=1.0)
            if response.status_code == 200:
                print("[WorkloadDriver] ✓ Señal de parada enviada a SUMO exitosamente.")
                return response.json()
            else:
                print(
                    "[WorkloadDriver Warning] Respuesta al detener SUMO: HTTP"
                    f" {response.status_code}"
                )
        except requests.RequestException as e:
            print(f"[WorkloadDriver Warning] No se pudo contactar endpoint de stop: {e}")
        return {}

    def wait_until_completed(
        self,
        poll_interval_sec: float = 2.0,
        timeout_sec: float = 600.0,
        max_consecutive_errors: int = 5,
    ) -> dict:
        """
        Sondea periódicamente el endpoint de estado hasta que la simulación finalice (Handshake Fase 1).
        
        Garantiza que el productor remoto terminó de emitir antes de proceder al drenado.
        Maneja timeouts, errores del backend e intermitencias de red.
        """
        print("[WorkloadDriver] Esperando a que concluya la simulación en API remota...")
        t_inicio = time.perf_counter()
        errores_consecutivos = 0

        while True:
            # 1. Guarda de timeout global
            elapsed_total = time.perf_counter() - t_inicio
            if elapsed_total > timeout_sec:
                raise SimulationTimeoutError(
                    f"[WorkloadDriver Error] Timeout alcanzado ({timeout_sec}s) esperando fin de la simulación."
                )

            try:
                response = requests.get(self.status_endpoint, timeout=5.0)

                if response.status_code == 200:
                    errores_consecutivos = 0
                    estado = response.json()

                    # Si el servicio reporta un error interno explícito de SUMO/FastAPI
                    if estado.get("error"):
                        raise SimulationExecutionError(
                            f"[WorkloadDriver Error] Error reportado por el backend remoto: {estado['error']}"
                        )

                    # Extracción compatible con respuestas anidadas o planas
                    progreso = estado.get("progreso", {})
                    paso_actual = progreso.get("paso_actual", estado.get("paso_actual", 0))
                    total_pasos = progreso.get(
                        "total_pasos",
                        estado.get("duracion_pasos", estado.get("total_pasos", 0)),
                    )
                    pct = progreso.get("porcentaje", 0.0)

                    esta_activo = estado.get("activo", False)
                    esta_completado = estado.get("completado", False)

                    # Condición de corte Fase 1: completado explícito o inactivo habiendo procesado pasos
                    if esta_completado or (not esta_activo and paso_actual > 0):
                        rendimiento = estado.get("rendimiento", {})
                        tasa = rendimiento.get(
                            "steps_por_segundo", estado.get("tasa_steps_segundo", 0.0)
                        )
                        duracion = rendimiento.get("segundos_transcurridos", round(elapsed_total, 2))
                        print(
                            f"\n[WorkloadDriver] ✓ Simulación SUMO concluida: {paso_actual}/{total_pasos} steps "
                            f"en {duracion}s (~{tasa} steps/s)."
                        )
                        return estado

                    # Actualización de línea de progreso
                    print(
                        f"  [Progreso SUMO] Paso {paso_actual}/{total_pasos} ({pct}%) | "
                        f"Tiempo: {elapsed_total:.1f}s...",
                        end="\r",
                    )

                else:
                    errores_consecutivos += 1
                    print(
                        f"\n[WorkloadDriver Warning] HTTP {response.status_code} al consultar estado. "
                        f"Reintentos: {errores_consecutivos}/{max_consecutive_errors}"
                    )

            except requests.RequestException as e:
                errores_consecutivos += 1
                print(
                    f"\n[WorkloadDriver Warning] Error sondeando estado: {e}. "
                    f"Reintentos: {errores_consecutivos}/{max_consecutive_errors}"
                )

            if errores_consecutivos >= max_consecutive_errors:
                self.stop_simulation()
                
                raise ConnectionError(
                    f"[WorkloadDriver Error] Se perdió conectividad con {self.status_endpoint} tras "
                    f"{max_consecutive_errors} fallos consecutivos."
                )

            time.sleep(poll_interval_sec)
    
    def trigger_and_wait(
        self,
        treatment: TreatmentConfig,
        poll_interval_sec: float = 2.0,
        timeout_sec: float = 600.0,
    ) -> dict:
        """Secuencia combinada: dispara la carga y bloquea hasta su finalización."""
        self.trigger_simulation(treatment)
        # Margen breve para que el thread en FastAPI pase a estado activo
        time.sleep(1.0)
        return self.wait_until_completed(
            poll_interval_sec=poll_interval_sec,
            timeout_sec=timeout_sec,
        )