-- ─── Analisi 3.2 — Report Ritardi per Aeroporto e Periodo Temporale ──────────
USE flights;

-- ─── 1. Tabella risultati fasce di ritardo ────────────────────────────────────
DROP TABLE IF EXISTS results_delay_report;

CREATE TABLE results_delay_report AS
SELECT
    origin,
    month,
    delay_band,
    COUNT(*)                                        AS num_flights,
    ROUND(AVG(dep_delay), 2)                        AS avg_dep_delay,
    ROUND(AVG(arr_delay), 2)                        AS avg_arr_delay
FROM (
    SELECT
        origin,
        month,
        dep_delay,
        arr_delay,
        CASE
            WHEN dep_delay IS NULL           THEN 'unknown'
            WHEN dep_delay < 15              THEN 'low'
            WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
            WHEN dep_delay > 60              THEN 'high'
            ELSE 'unknown'
        END AS delay_band
    FROM flights_clean
    WHERE origin IS NOT NULL
      AND month  IS NOT NULL
) t
GROUP BY origin, month, delay_band;

-- ─── 2. Tabella top 3 cause di ritardo per aeroporto e mese ──────────────────
-- ─── 2. Tabella top 3 cause di ritardo per aeroporto e mese ──────────────────
DROP TABLE IF EXISTS results_delay_causes;

CREATE TABLE results_delay_causes AS
SELECT origin, month, cause, avg_minutes, rank_pos
FROM (
    SELECT
        origin,
        month,
        cause,
        avg_minutes,
        RANK() OVER (PARTITION BY origin, month ORDER BY avg_minutes DESC) AS rank_pos
    FROM (
        SELECT origin, month, 'carrier_delay'       AS cause, AVG(carrier_delay)       AS avg_minutes FROM flights_clean WHERE carrier_delay       > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'weather_delay'       AS cause, AVG(weather_delay)       AS avg_minutes FROM flights_clean WHERE weather_delay       > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'nas_delay'           AS cause, AVG(nas_delay)           AS avg_minutes FROM flights_clean WHERE nas_delay           > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'security_delay'      AS cause, AVG(security_delay)      AS avg_minutes FROM flights_clean WHERE security_delay      > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'late_aircraft_delay' AS cause, AVG(late_aircraft_delay) AS avg_minutes FROM flights_clean WHERE late_aircraft_delay > 0 GROUP BY origin, month
    ) causes_raw
) ranked
WHERE rank_pos <= 3;

-- ─── 3. Prime 10 righe risultati ─────────────────────────────────────────────
SELECT * FROM results_delay_report
ORDER BY origin, month, delay_band
LIMIT 10;

SELECT * FROM results_delay_causes
ORDER BY origin, month, rank_pos
LIMIT 10;