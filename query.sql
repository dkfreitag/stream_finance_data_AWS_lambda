WITH cte_add_hour (name, ts, hour, high, low) AS (
    SELECT name,
           ts,
           HOUR(DATE_PARSE(SUBSTRING(ts, 12, 2), '%H')) as hour,
           high,
           low
    FROM "sta9760s2021-project03-database"."david_freitag_sta9760s2021_stream1"
),

cte_add_hourly_high (name, ts, hour, high, low, hourly_high) AS (
    SELECT name,
           ts,
           hour,
           high,
           low,
           MAX(high) OVER (PARTITION BY name, hour) as hourly_high
    FROM cte_add_hour
)

SELECT DISTINCT name,
                high,
                FIRST_VALUE(ts) OVER (PARTITION BY name, hour ORDER BY ts) AS ts,
                hour
FROM cte_add_hourly_high
WHERE high = hourly_high
ORDER BY name, ts
;