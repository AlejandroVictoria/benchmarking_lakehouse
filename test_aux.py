from config.treatment import TreatmentConfig
from core.lifecycle import LakehouseLifecycleManager

# 1. Instanciar configuración
treatment = TreatmentConfig(
    k_factor=10, buffer_size=1000, flush_interval_sec=10.0, sim_steps=600, replica_id=1
)
print(f"Run ID: {treatment.run_id}")
print(f"CSV de Ingesta: {treatment.metrics_csv_path}")

# 2. Validar Lifecycle Manager
mgr = LakehouseLifecycleManager()
if mgr.verify_environment():
  mgr.reset_table(treatment.namespace, treatment.table_name)
  print("✓ Entorno verificado y tabla lista para ingesta limpia.")
else:
  print("✗ Algunos servicios de infraestructura no están respondiendo.")