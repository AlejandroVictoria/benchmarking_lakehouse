#!/usr/bin/env python3
"""
Módulo de Visualización Empírica de Ablación (DL-4.2)
Genera interactivamente las figuras de comportamiento físico del Lakehouse.
"""

from pathlib import Path
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

STRATEGY_ORDER = [
    "Streaming (b=1)",
    "b=50 (t=2s)",
    "b=~ (t=2s)",
    "b=200 (t=5s)",
    "b=~ (t=5s)"
]

PALLET = {
    "iceberg": "#4e5598",
    "raw": "#b73a31"
}

def setup_plot_style():
    """Configura el estilo visual académico estandarizado."""
    sns.set_theme(style="whitegrid", font_scale=1.05)
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
            "axes.edgecolor": "#333333",
            "axes.linewidth": 0.8,
            "grid.color": "#e0e0e0",
            "grid.linestyle": "--",
            "grid.alpha": 0.7,
        }
    )

def load_dataset() -> pl.DataFrame:
    """Carga y consolida todos los datasets de corridas experimentales."""
    root_path = Path.cwd()
    data_dir = root_path / "ablation_study" / "processed"
    
    files = list(data_dir.glob("ablation_runs_raw_full_run.parquet"))
    if not files:
        files = list(root_path.glob("ablation_runs_raw*.parquet")) + list(root_path.glob("ablation_runs_raw*.csv"))
    
    if not files:
        raise FileNotFoundError(
            "No se encontraron datasets. Verifica la ruta o ejecuta primero la consolidación."
        )

    print(f"[*] Archivos encontrados para consolidar: {[f.name for f in files]}")
    
    dfs = []
    for f in files:
        if f.suffix == ".parquet":
            dfs.append(pl.read_parquet(f))
        elif f.suffix == ".csv":
            dfs.append(pl.read_csv(f))
            
    df = pl.concat(dfs, how="diagonal")

    df = df.with_columns(
        pl.when(pl.col("buffer_size") == 1).then(pl.lit("Streaming (b=1)"))
        .when((pl.col("buffer_size") >= 1000000) & (pl.col("flush_interval_sec") == 2.0))
        .then(pl.lit("b=~ (t=2s)"))
        .when((pl.col("buffer_size") >= 1000000) & (pl.col("flush_interval_sec") == 5.0))
        .then(pl.lit("b=~ (t=5s)"))
        .when(pl.col("buffer_size") == 50).then(pl.lit("b=50 (t=2s)"))
        .when(pl.col("buffer_size") == 200).then(pl.lit("b=200 (t=5s)"))
        .otherwise(pl.lit("Other"))
        .alias("buffer_strategy")
    )

    return df

def plot_commit_overhead_bar(df: pl.DataFrame, output_dir: Path):
    """Panel 1A: Costo de transaccionalidad ACID en la ingesta (Barras)."""
    df_pd = df.to_pandas()
    
    plt.figure(figsize=(8, 5.5))
    sns.barplot(
        data=df_pd, x="buffer_strategy", y="ingest_commit_time_mean_ms",
        hue="sink_type", palette=PALLET, errorbar="sd", capsize=0.1,
        order=STRATEGY_ORDER
    )
    plt.title("Average Commit Latency (ms)", fontweight="bold")
    plt.xlabel("Buffering Strategy")
    plt.ylabel("Commit Time (ms)")
    plt.yscale("log")
    plt.xticks(rotation=15)
    plt.legend(title="Formato Sink", loc="upper left")
    
    plt.tight_layout()
    plt.savefig(output_dir / "panel1a_commit_overhead_bar.pdf", format="pdf", bbox_inches="tight")

def plot_commit_overhead_line(df: pl.DataFrame, output_dir: Path):
    """Panel 1B: Latencia de commit desagregada por factor de demanda K."""
    df_pd = df.to_pandas()
    k_values = sorted(df_pd["k_factor"].unique())
    
    plt.figure(figsize=(8, 5.5))
    sns.lineplot(
        data=df_pd, x="k_factor", y="ingest_commit_time_mean_ms",
        hue="sink_type", style="buffer_strategy", markers=True, dashes=False,
        markersize=9, palette=PALLET, errorbar="sd"
    )
    plt.title("Scalability of Commit vs Vehicle Demand ($K$)", fontweight="bold")
    plt.xlabel("Vehicle Demand Factor ($K$)")
    plt.ylabel("Commit Time (ms)")
    plt.yscale("log")
    plt.xticks(k_values)
    plt.legend(title="Configuration", bbox_to_anchor=(1.02, 1), loc="upper left")
    
    plt.tight_layout()
    plt.savefig(output_dir / "panel1b_commit_overhead_line.pdf", format="pdf", bbox_inches="tight")

def plot_small_files_count(df: pl.DataFrame, output_dir: Path):
    """Panel 2A: Cantidad total de archivos Parquet creados."""
    df_pd = df.to_pandas()
    
    plt.figure(figsize=(8, 5.5))
    sns.barplot(
        data=df_pd, x="buffer_strategy", y="total_parquet_files",
        hue="sink_type", palette=PALLET, errorbar="sd", capsize=0.1,
        order=STRATEGY_ORDER
    )
    plt.title("Total Number of Parquet Files", fontweight="bold")
    plt.xlabel("Buffering Strategy")
    plt.ylabel("Number of Parquet Files")
    plt.xticks(rotation=15)
    plt.legend(title="Formato Sink", loc="upper right")
    
    plt.tight_layout()
    plt.savefig(output_dir / "panel2a_parquet_files_count.pdf", format="pdf", bbox_inches="tight")

def plot_small_files_size(df: pl.DataFrame, output_dir: Path):
    """Panel 2B: Tamaño promedio por archivo."""
    df_pd = df.to_pandas()
    
    plt.figure(figsize=(8, 5.5))
    sns.barplot(
        data=df_pd, x="buffer_strategy", y="avg_file_size_kb",
        hue="sink_type", palette=PALLET, errorbar="sd", capsize=0.1,
        order=STRATEGY_ORDER
    )
    plt.title("Average File Size (KB)", fontweight="bold")
    plt.xlabel("Buffering Strategy")
    plt.ylabel("Average Size (KB)")
    plt.xticks(rotation=15)
    plt.legend(title="Formato Sink", loc="upper left")
    
    plt.tight_layout()
    plt.savefig(output_dir / "panel2b_avg_file_size.pdf", format="pdf", bbox_inches="tight")

def plot_data_skipping(df: pl.DataFrame, output_dir: Path):
    """Panel 3A: Beneficio del Data Skipping en Iceberg."""
    df_pd = df.to_pandas()
    df_iceberg = df_pd[df_pd["sink_type"] == "iceberg"]
    
    if df_iceberg.empty:
        return

    k_values = sorted(df_iceberg["k_factor"].unique())
    num_k = len(k_values)
    # Se genera una paleta con colores extra y se omiten los dos primeros más claros
    custom_palette = sns.color_palette("Blues", n_colors=num_k + 2)[2:]
    
    plt.figure(figsize=(8, 5.5))
    sns.barplot(
        data=df_iceberg, x="buffer_strategy", y="data_skipping_ratio_pct",
        hue="k_factor", palette=custom_palette,
        order=STRATEGY_ORDER, errorbar="sd", capsize=0.1
    )
    plt.title("Data Skipping Rate in Iceberg (%)", fontweight="bold")
    plt.xlabel("Buffering Strategy")
    plt.ylabel("% Pruned Files (Data Skipping)")
    plt.ylim(0, 105)
    plt.xticks(rotation=15)
    plt.legend(title="Demand $K$", loc="upper right")
    
    plt.tight_layout()
    plt.savefig(output_dir / "panel3a_data_skipping.pdf", format="pdf", bbox_inches="tight")

def plot_olap_efficiency(df: pl.DataFrame, output_dir: Path):
    """Panel 3B: Latencia media en consultas OLAP representativas (A a F)."""
    df_pd = df.to_pandas()
    queries = ["a", "b", "c", "d", "e", "f"]
    
    for q in queries:
        target_column = f"query_{q}_mean_ms"
        if target_column not in df_pd.columns:
            continue
            
        plt.figure(figsize=(8, 5.5))
        sns.barplot(
            data=df_pd, x="buffer_strategy", y=target_column,
            hue="sink_type", palette=PALLET, errorbar="sd", capsize=0.1,
            order=STRATEGY_ORDER
        )
        plt.title(f"Average OLAP Read Latency (Query {q.upper()})", fontweight="bold")
        plt.xlabel("Buffering Strategy")
        plt.ylabel("Execution Time (ms)")
        plt.xticks(rotation=15)
        plt.legend(title="Sink Format", loc="upper right")
        
        plt.tight_layout()
        plt.savefig(output_dir / f"panel3b_olap_efficiency_query_{q}.pdf", format="pdf", bbox_inches="tight")
        
        # Se detiene la ejecución para mostrar cada gráfica iterativamente como se solicitó
        plt.show()

def plot_pareto_tradeoff(df: pl.DataFrame, output_dir: Path):
    """Panel 4: Frontera de compromiso entre tiempo de ingesta y lectura OLAP (Consultas A a F)."""
    df_pd = df.to_pandas()
    queries = ["a", "b", "c", "d", "e", "f"]
    
    for q in queries:
        target_column = f"query_{q}_mean_ms"
        
        # Verificamos si existe la columna para la consulta actual
        if target_column not in df_pd.columns:
            continue

        plt.figure(figsize=(9, 6.5))
        sns.scatterplot(
            data=df_pd, x="ingest_commit_time_mean_ms", y=target_column,
            hue="sink_type", style="buffer_strategy", size="k_factor",
            sizes=(60, 250), palette=PALLET, alpha=0.85
        )

        plt.title(f"Pareto Trade-off (Ingestion vs OLAP Query {q.upper()})", fontsize=13, fontweight="bold")
        plt.xlabel("Average Commit Latency in Ingestion (ms, log scale)")
        plt.ylabel(f"Average OLAP Query {q.upper()} Latency (ms)")
        plt.xscale("log")
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.0)
        plt.tight_layout()
        plt.savefig(output_dir / f"panel4_pareto_tradeoff_query_{q}.pdf", format="pdf", bbox_inches="tight")
        
        # Se detiene la ejecución para mostrar cada gráfica iterativamente
        plt.show()

def main():
    path = Path.cwd()
    output_dir = path / "ablation_study" / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_plot_style()
    df = load_dataset()

    print("[*] Desplegando figuras interactivas. Cierra cada ventana para continuar...")

    # plot_commit_overhead_bar(df, output_dir)
    # plt.show()

    # plot_commit_overhead_line(df, output_dir)
    # plt.show()

    # plot_small_files_count(df, output_dir)
    # plt.show()

    # plot_small_files_size(df, output_dir)
    # plt.show()

    # plot_data_skipping(df, output_dir)
    # plt.show()

    # # plot_olap_efficiency invoca plt.show() internamente para cada consulta
    # plot_olap_efficiency(df, output_dir)

    # plot_pareto_tradeoff invoca plt.show() internamente para cada consulta
    plot_pareto_tradeoff(df, output_dir)
    

    print("[✓] Visualización completada.")

if __name__ == "__main__":
    main()