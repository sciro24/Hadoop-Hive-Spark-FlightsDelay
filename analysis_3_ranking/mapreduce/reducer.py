#!/usr/bin/env python3
"""
Reducer Analisi 3.3 — Ranking coppie compagnia-aeroporto
Aggrega per (tipo, origin, carrier_or_ALL):
  - calcola num_flights, avg_dep_delay, avg_arr_delay, cancel_rate
  - NULL handling: dep/arr NULL vengono ignorati nelle medie (come Spark SQL)

Output finale (pipe-separated):
  origin | carrier | num_flights | avg_dep_delay | avg_arr_delay | cancel_rate | avg_dep_airport | dep_diff | rank
"""
import sys
from collections import defaultdict

# carrier_stats: (origin, carrier) → [dep_sum, arr_sum, canc_sum, total_count, dep_count, arr_count]
carrier_stats = defaultdict(lambda: [0.0, 0.0, 0.0, 0, 0, 0])
# airport_stats: origin → [dep_sum, dep_count]
airport_stats = defaultdict(lambda: [0.0, 0])

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 7:
        continue

    origin, tipo, key, dep_str, arr_str, canc_str, cnt_str = parts
    try:
        canc = float(canc_str)
        cnt  = int(cnt_str)
    except ValueError:
        continue

    if tipo == "carrier":
        carrier_stats[(origin, key)][2] += canc
        carrier_stats[(origin, key)][3] += cnt
        if dep_str != "NULL":
            dep = float(dep_str)
            carrier_stats[(origin, key)][0] += dep
            carrier_stats[(origin, key)][4] += 1
        if arr_str != "NULL":
            arr = float(arr_str)
            carrier_stats[(origin, key)][1] += arr
            carrier_stats[(origin, key)][5] += 1
    elif tipo == "airport":
        if dep_str != "NULL":
            dep = float(dep_str)
            airport_stats[origin][0] += dep
            airport_stats[origin][1] += 1

# Calcola medie aeroporto (solo su valori non-NULL)
airport_avg = {}
for origin, (dep_sum, dep_count) in airport_stats.items():
    airport_avg[origin] = round(dep_sum / dep_count, 4) if dep_count > 0 else 0.0

# Raggruppa per aeroporto per calcolare il rank
by_airport = defaultdict(list)

for (origin, carrier), (dep_sum, arr_sum, canc_sum, total, dep_cnt, arr_cnt) in carrier_stats.items():
    avg_dep  = round(dep_sum / dep_cnt, 4)  if dep_cnt > 0 else 0.0
    avg_arr  = round(arr_sum / arr_cnt, 4)  if arr_cnt > 0 else 0.0
    cancel_r = round(canc_sum / total,  4)  if total   > 0 else 0.0
    avg_airport = airport_avg.get(origin, 0.0)
    dep_diff    = round(avg_dep - avg_airport, 4)
    by_airport[origin].append((carrier, total, avg_dep, avg_arr, cancel_r, avg_airport, dep_diff))

# Stampa con rank
for origin in sorted(by_airport.keys()):
    # Ordina dalla migliore (dep_delay più basso) alla peggiore
    sorted_carriers = sorted(by_airport[origin], key=lambda x: x[2])
    for rank, (carrier, count, avg_dep, avg_arr, cancel_r, avg_airport, dep_diff) in enumerate(sorted_carriers, 1):
        print(f"{origin}\t{carrier}\t{count}\t{avg_dep}\t{avg_arr}\t{cancel_r}\t{avg_airport}\t{dep_diff}\t{rank}")