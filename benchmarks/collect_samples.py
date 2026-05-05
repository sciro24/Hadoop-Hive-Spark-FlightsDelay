#!/usr/bin/env python3
"""
Raccoglie le prime 10 righe di ogni output job e le salva in:
results/<analisi>/<tecnologia>/sample_top10.csv
"""
import os, glob

HEADERS = {
    "analysis_1": "op_unique_carrier|origin|dest|num_flights|avg_dep_delay|avg_arr_delay|cancel_rate",
    "analysis_2_delay_report": "origin|month|band|num_flights|avg_dep_delay|avg_arr_delay",
    "analysis_2_delay_causes": "origin|month|cause|avg_minutes|rank",
    "analysis_3": "origin|carrier|num_flights|avg_dep_delay|avg_arr_delay|cancel_rate|avg_dep_airport|dep_diff|rank",
}

# (cartella_sorgente, chiave_header, pattern_file, analisi_dest, tecnologia_dest)
JOBS = [
    # Analisi 1
    ("results/analysis_1/mapreduce",  "analysis_1",              "output*.csv",             "analysis_1", "mapreduce"),
    ("results/analysis_1/hive",       "analysis_1",              "output*.csv",             "analysis_1", "hive"),
    ("results/analysis_1/spark_sql",  "analysis_1",              "output*.csv",             "analysis_1", "spark_sql"),
    # Analisi 2 — delay report
    ("results/analysis_2/hive",       "analysis_2_delay_report", "output_delay_report*.csv","analysis_2", "hive"),
    ("results/analysis_2/spark_core", "analysis_2_delay_report", "output_delay_report.csv", "analysis_2", "spark_core"),
    ("results/analysis_2/spark_sql",  "analysis_2_delay_report", "output_delay_report*.csv","analysis_2", "spark_sql"),
    # Analisi 2 — delay causes (aggiunge file separato nella stessa cartella tecnologia)
    ("results/analysis_2/hive",       "analysis_2_delay_causes", "output_delay_causes*.csv","analysis_2", "hive_causes"),
    ("results/analysis_2/spark_core", "analysis_2_delay_causes", "output_delay_causes.csv", "analysis_2", "spark_core_causes"),
    ("results/analysis_2/spark_sql",  "analysis_2_delay_causes", "output_delay_causes*.csv","analysis_2", "spark_sql_causes"),
    # Analisi 3
    ("results/analysis_3/mapreduce",  "analysis_3",              "output*.csv",             "analysis_3", "mapreduce"),
    ("results/analysis_3/spark_core", "analysis_3",              "output.csv",              "analysis_3", "spark_core"),
    ("results/analysis_3/spark_sql",  "analysis_3",              "output*.csv",             "analysis_3", "spark_sql"),
]

found, missing = [], []

for src_folder, header_key, pattern, analysis_dest, tech_dest in JOBS:
    matches = glob.glob(os.path.join(src_folder, pattern))
    if not matches:
        matches = glob.glob(os.path.join(src_folder, "part-00000"))
    if not matches:
        print(f"[MISSING] {src_folder}/{pattern}")
        missing.append(f"{analysis_dest}/{tech_dest}")
        continue

    src = sorted(matches)[0]

    out_dir = os.path.join("results", analysis_dest, tech_dest)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sample_top10.csv")

    with open(src, "r") as fin, open(out_path, "w") as fout:
        header = HEADERS.get(header_key, "")
        if header:
            fout.write(header + "\n")
        
        lines = [l for l in fin if l.strip()]
        if lines and header and lines[0].startswith(header.split("|")[0]):
            lines = lines[1:]
            
        fout.writelines(lines[:10])

    print(f"[OK] {out_path}  ({len(lines)} righe)")
    found.append(out_path)

print(f"\n✅ Salvati: {len(found)}  ❌ Mancanti: {len(missing)}")
if missing:
    print("Mancanti:", missing)