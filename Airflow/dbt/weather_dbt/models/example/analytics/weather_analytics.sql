SELECT
    city,
    date,
    temp_mean,

    -- 7-day moving average temperature
    AVG(temp_mean) OVER (
        PARTITION BY city
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_temp,

    -- 7-day rolling precipitation
    SUM(precipitation) OVER (
        PARTITION BY city
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_precipitation

FROM RAW.WEATHER_DATA_LAB1