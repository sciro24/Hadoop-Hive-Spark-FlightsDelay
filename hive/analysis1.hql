-- ============================================================
-- ANALISI 3.1 - Statistiche delle compagnie aeree
-- Per ogni compagnia e aeroporto di partenza:
--   - numero voli
--   - ritardo min/max/medio in arrivo
--   - tasso di cancellazione
--   - lista mesi di operazione
-- ============================================================

USE flights_db;

-- Forza esecuzione MapReduce (non locale)
SET hive.exec.mode.local.auto=false;
SET mapreduce.framework.name=yarn;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Risultato finale
INSERT OVERWRITE LOCAL DIRECTORY '/Users/diego/Documents/GitHub/Hadoop-Hive-Spark-FlightsDelay/results/analysis1/hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    op_unique_carrier                             AS airline,
    origin                                        AS airport,
    COUNT(*)                                      AS total_flights,
    MIN(arr_delay)                                AS min_arr_delay,
    MAX(arr_delay)                                AS max_arr_delay,
    ROUND(AVG(arr_delay), 2)                      AS avg_arr_delay,
    ROUND(SUM(cancelled) / COUNT(*) * 100, 2)     AS cancellation_rate_pct,
    COLLECT_SET(CAST(month AS STRING))            AS active_months
FROM flights_clean
GROUP BY op_unique_carrier, origin
ORDER BY op_unique_carrier, origin;