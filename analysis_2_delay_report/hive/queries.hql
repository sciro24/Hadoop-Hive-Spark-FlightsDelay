-- ─── Analisi 3.2 — Report Ritardi Professionale Unificato ────────────────────
USE flights;

-- 1. Tabella Temporanea Cause Pivotate
DROP TABLE IF EXISTS causes_pivoted;
CREATE TABLE causes_pivoted AS
WITH causes_all AS (
    SELECT origin, month, 'carrier' AS cause, AVG(carrier_delay) AS am FROM flights_clean WHERE carrier_delay > 0 GROUP BY origin, month
    UNION ALL
    SELECT origin, month, 'weather' AS cause, AVG(weather_delay) AS am FROM flights_clean WHERE weather_delay > 0 GROUP BY origin, month
    UNION ALL
    SELECT origin, month, 'nas'     AS cause, AVG(nas_delay)     AS am FROM flights_clean WHERE nas_delay     > 0 GROUP BY origin, month
    UNION ALL
    SELECT origin, month, 'security' AS cause, AVG(security_delay) AS am FROM flights_clean WHERE security_delay > 0 GROUP BY origin, month
    UNION ALL
    SELECT origin, month, 'late_aircraft' AS cause, AVG(late_aircraft_delay) AS am FROM flights_clean WHERE late_aircraft_delay > 0 GROUP BY origin, month
),
causes_ranked AS (
    SELECT origin, month, cause,
           ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY am DESC) as rnk
    FROM causes_all
)
SELECT 
    origin, month,
    MAX(CASE WHEN rnk = 1 THEN cause END) as top_cause_1,
    MAX(CASE WHEN rnk = 2 THEN cause END) as top_cause_2,
    MAX(CASE WHEN rnk = 3 THEN cause END) as top_cause_3
FROM causes_ranked
WHERE rnk <= 3
GROUP BY origin, month;

-- 2. Tabella Finale Unificata
DROP TABLE IF EXISTS results_unified;
CREATE TABLE results_unified AS
SELECT 
    b.origin, 
    b.month, 
    b.delay_band, 
    b.num_flights, 
    b.avg_dep, 
    b.avg_arr,
    COALESCE(c.top_cause_1, 'none') as top_cause_1,
    COALESCE(c.top_cause_2, 'none') as top_cause_2,
    COALESCE(c.top_cause_3, 'none') as top_cause_3
FROM (
    SELECT 
        origin, month,
        CASE 
            WHEN dep_delay < 15              THEN 'low'
            WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
            WHEN dep_delay > 60              THEN 'high'
        END AS delay_band,
        COUNT(*) AS num_flights,
        ROUND(AVG(COALESCE(dep_delay, 0)), 2) AS avg_dep,
        ROUND(AVG(COALESCE(arr_delay, 0)), 2) AS avg_arr
    FROM flights_clean
    WHERE dep_delay IS NOT NULL
    GROUP BY origin, month,
        CASE 
            WHEN dep_delay < 15              THEN 'low'
            WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
            WHEN dep_delay > 60              THEN 'high'
        END
) b
LEFT JOIN causes_pivoted c ON b.origin = c.origin AND b.month = c.month;

-- ─── 4. Preview ───────────────────────────────────────────────────────────────
SELECT * FROM results_unified ORDER BY origin, month, delay_band LIMIT 20;