SELECT
    CITY,
    COUNT(*) AS total_records,
    AVG(TEMP_MAX) AS avg_max_temp,
    AVG(TEMP_MIN) AS avg_min_temp,
    AVG((TEMP_MAX + TEMP_MIN)/2) AS avg_mean_temp,
    SUM(PRECIPITATION) AS total_precipitation
FROM RAW.WEATHER_DATA
GROUP BY CITY