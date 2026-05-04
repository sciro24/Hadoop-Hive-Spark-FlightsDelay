#!/usr/bin/env python3
"""
Reducer Analisi 3.1 — Statistiche compagnie aeree
Input:  chiave<TAB>valore (ordinati per chiave dallo shuffle)
Output: carrier|origin|month|num_flights|min_arr|max_arr|avg_arr|cancel_rate
"""
import sys
from collections import defaultdict

# Struttura: (carrier, origin) → { month → [flights, delays[], cancelled] }
# Usiamo un unico passaggio accumulando per (carrier, origin, month)

current_key = None
flights   = 0
delays    = []
cancelled = 0

# Buffer per aggregare tutti i mesi di una coppia (carrier, origin)
# Prima raccogliamo tutto, poi emettiamo per coppia
aggregated = defaultdict(lambda: defaultdict(lambda: {"flights": 0, "delays": [], "cancelled": 0}))

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        carrier, origin, month = key.split("|")
        arr_delay_str, cancelled_str = value.split("|")
    except ValueError:
        continue

    rec = aggregated[(carrier, origin)][month]
    rec["flights"] += 1
    rec["cancelled"] += int(cancelled_str)

    if arr_delay_str != "NULL":
        try:
            rec["delays"].append(float(arr_delay_str))
        except ValueError:
            pass

# ─── Emissione risultati ──────────────────────────────────────────────────────
try:
    for (carrier, origin), months_data in sorted(aggregated.items()):
        for month, rec in sorted(months_data.items(), key=lambda x: int(x[0])):
            n          = rec["flights"]
            delays_lst = rec["delays"]
            n_cancel   = rec["cancelled"]

            min_arr  = round(min(delays_lst), 2)  if delays_lst else "NULL"
            max_arr  = round(max(delays_lst), 2)  if delays_lst else "NULL"
            avg_arr  = round(sum(delays_lst) / len(delays_lst), 2) if delays_lst else "NULL"
            cxl_rate = round(n_cancel / n, 4) if n > 0 else 0.0

            print(f"{carrier}|{origin}|{month}|{n}|{min_arr}|{max_arr}|{avg_arr}|{cxl_rate}")
except BrokenPipeError:
    sys.stdout = None  # Sopprime ulteriori errori di pipe