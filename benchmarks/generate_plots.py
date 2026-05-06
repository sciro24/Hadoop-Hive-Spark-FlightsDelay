#!/usr/bin/env python3
"""
Generazione grafici benchmark — Flight Delay 2024

Legge benchmarks/results_local.csv e produce tutti i grafici comparativi
nella cartella benchmarks/plots/.

Uso:
    python3 benchmarks/generate_plots.py
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ─── Configurazione ──────────────────────────────────────────────────────────
RESULTS_CSV = "benchmarks/results_local.csv"
PLOTS_DIR   = "benchmarks/plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

# Stile globale
plt.rcParams.update({
    "figure.dpi":        150,
    "figure.facecolor":  "white",
    "axes.facecolor":    "#f8f9fa",
    "axes.grid":         True,
    "grid.alpha":        0.3,
    "font.size":         11,
    "axes.titlesize":    14,
    "axes.labelsize":    12,
    "legend.fontsize":   10,
    "figure.titlesize":  16,
})

# Palette colori per tecnologia
TECH_COLORS = {
    "mapreduce":  "#e74c3c",
    "hive":       "#f39c12",
    "spark_core": "#2ecc71",
    "spark_sql":  "#3498db",
}

TECH_LABELS = {
    "mapreduce":  "MapReduce",
    "hive":       "Hive",
    "spark_core": "Spark Core",
    "spark_sql":  "Spark SQL",
}

TECH_MARKERS = {
    "mapreduce":  "s",
    "hive":       "D",
    "spark_core": "^",
    "spark_sql":  "o",
}

ANALYSIS_LABELS = {
    "3.1": "3.1 — Statistiche Compagnie",
    "3.2": "3.2 — Report Ritardi",
    "3.3": "3.3 — Ranking Anomalo",
}

# Ordine sample
SAMPLE_ORDER = ["sample_010pct", "sample_025pct", "sample_050pct",
                "full_dataset", "sample_125pct", "sample_150pct"]
SAMPLE_LABELS = ["10%", "25%", "50%", "100%", "125%", "150%"]


def load_data():
    df = pd.read_csv(RESULTS_CSV)
    df["analysis"] = df["analysis"].astype(str)
    df["notes"] = df["notes"].fillna("full_dataset")
    # Ordine sample
    df["sample_order"] = df["notes"].map({s: i for i, s in enumerate(SAMPLE_ORDER)})
    df = df.sort_values(["analysis", "technology", "sample_order"])
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 1: Confronto tempi sul dataset completo (100%) — tutte le analisi
# ═══════════════════════════════════════════════════════════════════════════════
def plot_full_dataset_comparison(df):
    full = df[df["notes"] == "full_dataset"].copy()
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    analyses = sorted(full["analysis"].unique())
    x = np.arange(len(analyses))
    width = 0.18
    offsets = {"mapreduce": -1.5, "hive": -0.5, "spark_core": 0.5, "spark_sql": 1.5}
    
    for tech, offset in offsets.items():
        subset = full[full["technology"] == tech]
        if subset.empty:
            continue
        times = [subset[subset["analysis"] == a]["elapsed_sec"].values[0]
                 if not subset[subset["analysis"] == a].empty else 0
                 for a in analyses]
        bars = ax.bar(x + offset * width, times, width,
                      label=TECH_LABELS[tech], color=TECH_COLORS[tech],
                      edgecolor="white", linewidth=0.5)
        # Annotazione valori
        for bar, t in zip(bars, times):
            if t > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                        f"{t:.1f}s", ha="center", va="bottom", fontsize=8, fontweight="bold")
    
    ax.set_xlabel("Analisi")
    ax.set_ylabel("Tempo di esecuzione (secondi)")
    ax.set_title("Confronto Tempi — Dataset Completo (7.04M righe)")
    ax.set_xticks(x)
    ax.set_xticklabels([ANALYSIS_LABELS.get(a, a) for a in analyses])
    ax.legend(loc="upper left")
    ax.set_ylim(0, max(full["elapsed_sec"]) * 1.2)
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/01_full_dataset_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 01_full_dataset_comparison.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 2-4: Scalabilità per analisi (tempo vs dimensione input)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_scalability_per_analysis(df):
    for analysis in sorted(df["analysis"].unique()):
        adf = df[df["analysis"] == analysis].copy()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        for tech in sorted(adf["technology"].unique()):
            tdf = adf[adf["technology"] == tech].sort_values("sample_order")
            ax.plot(tdf["input_rows"] / 1e6, tdf["elapsed_sec"],
                    marker=TECH_MARKERS.get(tech, "o"),
                    color=TECH_COLORS.get(tech, "#666"),
                    label=TECH_LABELS.get(tech, tech),
                    linewidth=2, markersize=8)
        
        ax.set_xlabel("Dimensione input (milioni di righe)")
        ax.set_ylabel("Tempo di esecuzione (secondi)")
        ax.set_title(f"Scalabilità — Analisi {ANALYSIS_LABELS.get(analysis, analysis)}")
        ax.legend()
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:.1f}M"))
        
        plt.tight_layout()
        num = {"3.1": "02", "3.2": "03", "3.3": "04"}[analysis]
        plt.savefig(f"{PLOTS_DIR}/{num}_scalability_{analysis.replace('.', '')}.png",
                    dpi=150, bbox_inches="tight")
        plt.close()
        print(f"✅ {num}_scalability_{analysis.replace('.', '')}.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 5: Heatmap tempi (Tecnologia × Analisi) — dataset completo
# ═══════════════════════════════════════════════════════════════════════════════
def plot_heatmap(df):
    full = df[df["notes"] == "full_dataset"].copy()
    
    techs = ["mapreduce", "hive", "spark_core", "spark_sql"]
    analyses = sorted(full["analysis"].unique())
    
    matrix = []
    for tech in techs:
        row = []
        for analysis in analyses:
            match = full[(full["technology"] == tech) & (full["analysis"] == analysis)]
            row.append(match["elapsed_sec"].values[0] if not match.empty else np.nan)
        matrix.append(row)
    
    matrix = np.array(matrix)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
    
    ax.set_xticks(range(len(analyses)))
    ax.set_xticklabels([ANALYSIS_LABELS.get(a, a) for a in analyses])
    ax.set_yticks(range(len(techs)))
    ax.set_yticklabels([TECH_LABELS[t] for t in techs])
    
    # Annotazioni
    for i in range(len(techs)):
        for j in range(len(analyses)):
            val = matrix[i, j]
            if not np.isnan(val):
                color = "white" if val > matrix[~np.isnan(matrix)].mean() else "black"
                ax.text(j, i, f"{val:.1f}s", ha="center", va="center",
                        fontweight="bold", fontsize=11, color=color)
            else:
                ax.text(j, i, "—", ha="center", va="center",
                        fontsize=11, color="#999")
    
    ax.set_title("Heatmap Tempi — Dataset Completo (7.04M righe)")
    fig.colorbar(im, ax=ax, label="Tempo (secondi)")
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/05_heatmap_full.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 05_heatmap_full.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 6: Speedup relativo rispetto alla tecnologia più lenta
# ═══════════════════════════════════════════════════════════════════════════════
def plot_speedup(df):
    full = df[df["notes"] == "full_dataset"].copy()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    analyses = sorted(full["analysis"].unique())
    x = np.arange(len(analyses))
    width = 0.18
    offsets = {"mapreduce": -1.5, "hive": -0.5, "spark_core": 0.5, "spark_sql": 1.5}
    
    for tech, offset in offsets.items():
        speedups = []
        for analysis in analyses:
            times_analysis = full[full["analysis"] == analysis]
            max_time = times_analysis["elapsed_sec"].max()
            tech_time = times_analysis[times_analysis["technology"] == tech]["elapsed_sec"]
            if not tech_time.empty:
                speedups.append(max_time / tech_time.values[0])
            else:
                speedups.append(0)
        
        if any(s > 0 for s in speedups):
            bars = ax.bar(x + offset * width, speedups, width,
                          label=TECH_LABELS[tech], color=TECH_COLORS[tech],
                          edgecolor="white", linewidth=0.5)
            for bar, s in zip(bars, speedups):
                if s > 0:
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                            f"{s:.1f}×", ha="center", va="bottom", fontsize=8, fontweight="bold")
    
    ax.set_xlabel("Analisi")
    ax.set_ylabel("Speedup (rispetto alla tecnologia più lenta)")
    ax.set_title("Speedup Relativo — Dataset Completo")
    ax.set_xticks(x)
    ax.set_xticklabels([ANALYSIS_LABELS.get(a, a) for a in analyses])
    ax.axhline(y=1, color="#e74c3c", linestyle="--", alpha=0.5, label="Baseline (1×)")
    ax.legend(loc="upper right")
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/06_speedup.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 06_speedup.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 7: Confronto formato input (CSV vs Parquet dimensione)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_input_format_comparison(df):
    csv_data = df[df["input_file"].str.endswith(".csv")].copy()
    parquet_data = df[df["input_file"].str.endswith(".parquet")].copy()
    
    # Raggruppa per notes (sample) e prendi la dimensione unica
    csv_sizes = csv_data.groupby("notes")["input_size_mb"].first()
    parquet_sizes = parquet_data.groupby("notes")["input_size_mb"].first()
    
    common = sorted(set(csv_sizes.index) & set(parquet_sizes.index),
                    key=lambda x: SAMPLE_ORDER.index(x) if x in SAMPLE_ORDER else 99)
    
    if not common:
        print("⚠️  Nessun sample comune CSV/Parquet trovato, skip plot formato input")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(common))
    width = 0.35
    
    csv_vals = [csv_sizes.get(s, 0) for s in common]
    pq_vals  = [parquet_sizes.get(s, 0) for s in common]
    
    bars1 = ax.bar(x - width/2, csv_vals, width, label="CSV", color="#e74c3c", alpha=0.8)
    bars2 = ax.bar(x + width/2, pq_vals, width, label="Parquet", color="#3498db", alpha=0.8)
    
    # Annotazioni con rapporto di compressione
    for i, (csv_v, pq_v) in enumerate(zip(csv_vals, pq_vals)):
        if pq_v > 0:
            ratio = csv_v / pq_v
            ax.text(x[i] + width/2, pq_v + 5, f"{ratio:.1f}×",
                    ha="center", fontsize=9, color="#2c3e50", fontweight="bold")
    
    labels = [SAMPLE_LABELS[SAMPLE_ORDER.index(s)] if s in SAMPLE_ORDER else s for s in common]
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Dimensione dataset")
    ax.set_ylabel("Dimensione file (MB)")
    ax.set_title("Confronto Formato — CSV vs Parquet")
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/07_format_csv_vs_parquet.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 07_format_csv_vs_parquet.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 8: Overhead Hive vs tempo effettivo (costante vs variabile)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_hive_overhead(df):
    hive = df[df["technology"] == "hive"].copy()
    if hive.empty:
        return
    
    fig, axes = plt.subplots(1, len(hive["analysis"].unique()), figsize=(15, 5), sharey=True)
    if len(hive["analysis"].unique()) == 1:
        axes = [axes]
    
    for ax, analysis in zip(axes, sorted(hive["analysis"].unique())):
        adf = hive[hive["analysis"] == analysis].sort_values("sample_order")
        
        times = adf["elapsed_sec"].values
        rows  = adf["input_rows"].values / 1e6
        
        # Stima overhead (intercetta) e parte lineare
        if len(times) >= 2:
            min_time = times.min()
            overhead_est = min_time * 0.9  # approssimazione overhead fisso
            variable = times - overhead_est
            
            ax.bar(range(len(times)), [overhead_est]*len(times), 
                   color="#f39c12", alpha=0.4, label="Overhead fisso (stima)")
            ax.bar(range(len(times)), np.maximum(variable, 0), 
                   bottom=overhead_est, color="#f39c12", alpha=0.8, label="Elaborazione dati")
        
        ax.set_xticks(range(len(times)))
        ax.set_xticklabels(SAMPLE_LABELS[:len(times)], rotation=45)
        ax.set_title(f"Analisi {analysis}")
        ax.set_xlabel("Dimensione dataset")
        if ax == axes[0]:
            ax.set_ylabel("Tempo (secondi)")
    
    axes[0].legend(loc="upper left", fontsize=9)
    fig.suptitle("Hive — Overhead di Avvio vs Elaborazione Effettiva", fontsize=14, fontweight="bold")
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/08_hive_overhead.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 08_hive_overhead.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 9: Efficienza (righe/secondo) per tecnologia
# ═══════════════════════════════════════════════════════════════════════════════
def plot_throughput(df):
    full = df[df["notes"] == "full_dataset"].copy()
    full["throughput"] = full["input_rows"] / full["elapsed_sec"]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    analyses = sorted(full["analysis"].unique())
    x = np.arange(len(analyses))
    width = 0.18
    offsets = {"mapreduce": -1.5, "hive": -0.5, "spark_core": 0.5, "spark_sql": 1.5}
    
    for tech, offset in offsets.items():
        subset = full[full["technology"] == tech]
        if subset.empty:
            continue
        throughputs = []
        for a in analyses:
            match = subset[subset["analysis"] == a]
            throughputs.append(match["throughput"].values[0] / 1000 if not match.empty else 0)
        
        bars = ax.bar(x + offset * width, throughputs, width,
                      label=TECH_LABELS[tech], color=TECH_COLORS[tech],
                      edgecolor="white", linewidth=0.5)
        for bar, t in zip(bars, throughputs):
            if t > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                        f"{t:.0f}K", ha="center", va="bottom", fontsize=8, fontweight="bold")
    
    ax.set_xlabel("Analisi")
    ax.set_ylabel("Throughput (migliaia di righe/secondo)")
    ax.set_title("Throughput — Dataset Completo (7.04M righe)")
    ax.set_xticks(x)
    ax.set_xticklabels([ANALYSIS_LABELS.get(a, a) for a in analyses])
    ax.legend(loc="upper right")
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/09_throughput.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 09_throughput.png")


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 10: Panoramica scalabilità unificata (subplots)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_scalability_overview(df):
    analyses = sorted(df["analysis"].unique())
    fig, axes = plt.subplots(1, len(analyses), figsize=(18, 5), sharey=False)
    
    for ax, analysis in zip(axes, analyses):
        adf = df[df["analysis"] == analysis]
        for tech in sorted(adf["technology"].unique()):
            tdf = adf[adf["technology"] == tech].sort_values("sample_order")
            ax.plot(tdf["input_rows"] / 1e6, tdf["elapsed_sec"],
                    marker=TECH_MARKERS.get(tech, "o"),
                    color=TECH_COLORS.get(tech, "#666"),
                    label=TECH_LABELS.get(tech, tech),
                    linewidth=2, markersize=7)
        
        ax.set_title(f"Analisi {analysis}", fontweight="bold")
        ax.set_xlabel("Milioni di righe")
        if ax == axes[0]:
            ax.set_ylabel("Tempo (secondi)")
        ax.legend(fontsize=9)
    
    fig.suptitle("Scalabilità — Tutte le Analisi", fontsize=14, fontweight="bold", y=1.02)
    
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/10_scalability_overview.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ 10_scalability_overview.png")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  Generazione grafici benchmark")
    print(f"  Input:  {RESULTS_CSV}")
    print(f"  Output: {PLOTS_DIR}/")
    print(f"{'='*60}\n")
    
    df = load_data()
    print(f"Caricati {len(df)} record di benchmark\n")
    
    plot_full_dataset_comparison(df)
    plot_scalability_per_analysis(df)
    plot_heatmap(df)
    plot_speedup(df)
    plot_input_format_comparison(df)
    plot_hive_overhead(df)
    plot_throughput(df)
    plot_scalability_overview(df)
    
    print(f"\n✅ Tutti i grafici salvati in: {PLOTS_DIR}/")
