#!/usr/bin/env python3
"""
Reducer Analisi 3.3 — Ranking coppie compagnia-aeroporto
Aggrega per (tipo, origin, carrier_or_ALL):
  - calcola num_flights, avg_dep_delay, avg_arr_delay, cancel_rate
Output finale:
  origin | carrier | num_flights | avg_dep | avg_arr | cancel_rate | avg_dep_airport | dep_diff | rank
"""
import sys
from collections import defaultdict

# Strutture di accumulo
carrier_stats = defaultdict(lambda: [0.0, 0.0, 0.0, 0])  # dep_sum, arr_sum, canc_sum, count
airport_stats = defaultdict(lambda: [0.0, 0])              # dep_sum, count

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 7:
        continue

    tipo, origin, key, dep, arr, canc, cnt = parts
    try:
        dep  = float(dep)
        arr  = float(arr)
        canc = float(canc)
        cnt  = int(cnt)
    except ValueError:
        continue

    if tipo == "carrier":
        carrier_stats[(origin, key)][0] += dep
        carrier_stats[(origin, key)][1] += arr
        carrier_stats[(origin, key)][2] += canc
        carrier_stats[(origin, key)][3] += cnt
    elif tipo == "airport":
        airport_stats[origin][0] += dep
        airport_stats[origin][1] += cnt

# Calcola medie aeroporto
airport_avg = {}
for origin, (dep_sum, count) in airport_stats.items():
    airport_avg[origin] = round(dep_sum / count, 4) if count > 0 else 0.0

# Raggruppa per aeroporto per calcolare il rank
from collections import defaultdict as dd
by_airport = dd(list)

for (origin, carrier), (dep_sum, arr_sum, canc_sum, count) in carrier_stats.items():
    avg_dep  = round(dep_sum  / count, 4)
    avg_arr  = round(arr_sum  / count, 4)
    cancel_r = round(canc_sum / count, 4)
    avg_airport = airport_avg.get(origin, 0.0)
    dep_diff    = round(avg_dep - avg_airport, 4)
    by_airport[origin].append((carrier, count, avg_dep, avg_arr, cancel_r, avg_airport, dep_diff))

# Stampa con rank
for origin in sorted(by_airport.keys()):
    # Ordina dalla migliore (dep_delay più basso) alla peggiore
    sorted_carriers = sorted(by_airport[origin], key=lambda x: x[2])
    for rank, (carrier, count, avg_dep, avg_arr, cancel_r, avg_airport, dep_diff) in enumerate(sorted_carriers, 1):
        print(f"{origin}\t{carrier}\t{count}\t{avg_dep}\t{avg_arr}\t{cancel_r}\t{avg_airport}\t{dep_diff}\t{rank}")