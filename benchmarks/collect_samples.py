#!/usr/bin/env python3
"""
Raccoglie le prime 10 righe di ogni output job e le salva in:
results/<analisi>/<tecnologia>/sample_top10.csv
"""
import os, glob

HEADERS = {
    "analysis_1":              "carrier|origin|month|num_flights|min_arr_delay|max_arr_delay|avg_arr_delay|cancel_rate|months_active",
    "analysis_2_delay_report": "origin|month|delay_band|num_flights|avg_dep_delay|avg_arr_delay",
    "analysis_2_delay_causes": "origin|month|cause|avg_minutes|rank_pos",
    "analysis_3":              "origin|carrier|num_flights|avg_dep_delay|avg_arr_delay|cancel_rate|avg_dep_airport|dep_diff|rank",
}

JOBS = [
    ("results/analysis_1/mapreduce",  "analysis_1",              "output*.csv",              "analysis_1", "mapreduce"),
    ("results/analysis_1/hive",       "analysis_1",              "output*.csv",              "analysis_1", "hive"),
    ("results/analysis_1/spark_sql",  "analysis_1",              "output*.csv",              "analysis_1", "spark_sql"),
    ("results/analysis_2/hive",       "analysis_2_delay_report", "output_delay_report*.csv", "analysis_2", "hive"),
    ("results/analysis_2/spark_core", "analysis_2_delay_report", "output_delay_report.csv",  "analysis_2", "spark_core"),
    ("results/analysis_2/spark_sql",  "analysis_2_delay_report", "output_delay_report*.csv", "analysis_2", "spark_sql"),
    ("results/analysis_2/hive",       "analysis_2_delay_causes", "output_delay_causes*.csv", "analysis_2", "hive_causes"),
    ("results/analysis_2/spark_core", "analysis_2_delay_causes", "output_delay_causes.csv",  "analysis_2", "spark_core_causes"),
    ("results/analysis_2/spark_sql",  "analysis_2_delay_causes", "output_delay_causes*.csv", "analysis_2", "spark_sql_causes"),
    ("results/analysis_3/mapreduce",  "analysis_3",              "output*.csv",              "analysis_3", "mapreduce"),
    ("results/analysis_3/spark_core", "analysis_3",              "output.csv",               "analysis_3", "spark_core"),
    ("results/analysis_3/spark_sql",  "analysis_3",              "output*.csv",              "analysis_3", "spark_sql"),
]

# Tutti i prefissi noti di header da skippare (vecchi o corretti)
KNOWN_HEADERS = {
    "op_unique_carrier", "carrier", "origin", "band",
    "delay_band", "cause", "rank", "rank_pos"
}

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

    out_dir  = os.path.join("results", analysis_dest, tech_dest)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sample_top10.csv")

    with open(src, "r") as fin, open(out_path, "w") as fout:
        header = HEADERS.get(header_key, "")
        if header:
            fout.write(header + "\n")

        lines = [l for l in fin if l.strip()]

        # Rimuovi TUTTE le righe di header iniziali (vecchie o corrette)
        while lines and lines[0].split("|")[0].split("\t")[0].strip() in KNOWN_HEADERS:
            lines = lines[1:]

        # Normalizza separatore: converti tab → pipe
        normalized = [l.replace("\t", "|") for l in lines]

        fout.writelines(normalized[:10])

    print(f"[OK] {out_path}  ({len(lines)} righe)")
    found.append(out_path)

print(f"\n✅ Salvati: {len(found)}  ❌ Mancanti: {len(missing)}")
if missing:
    print("Mancanti:", missing)