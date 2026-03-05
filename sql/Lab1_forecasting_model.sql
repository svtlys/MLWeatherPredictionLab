-- DATA226 Lab 1
-- Forecasting Model

-- Get correct database
SELECT * FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1;

-- For Northridge, CA
-- Create view for max temperature forecast
CREATE OR REPLACE VIEW max_temp_data_view_n AS 
    (SELECT date as da, temp_max 
     FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
     WHERE city = 'Northridge');

SELECT * FROM max_temp_data_view_n;


-- Create view for max temperature forecast
CREATE OR REPLACE VIEW precipitation_data_view_n AS 
    (SELECT date as da, precipitation
     FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
     WHERE city = 'Northridge');

SELECT * FROM precipitation_data_view_n;


-- Create max temperature forecasting model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST lab1_max_temp_forecast_n(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'max_temp_data_view_n'),    --pass the view as input data
    TIMESTAMP_COLNAME => 'da',          -- specify time axis col
    TARGET_COLNAME => 'temp_max',       -- predict max temp
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);


-- Create precipitation forecasting model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST lab1_precipitation_forecast_n(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'precipitation_data_view_n'),    --pass the view as input data
    TIMESTAMP_COLNAME => 'da',          -- specify time axis col
    TARGET_COLNAME => 'precipitation',  -- predict precipitation
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);


-- Create predictions for next 7 days max temperature
BEGIN
    CALL lab1_max_temp_forecast_n!FORECAST(
        FORECASTING_PERIODS => 7,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE OR REPLACE TABLE max_temp_forecast_n AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM max_temp_forecast_n;


-- Create predictions for next 7 days precipitation
BEGIN
    CALL lab1_precipitation_forecast_n!FORECAST(
        FORECASTING_PERIODS => 7,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE OR REPLACE TABLE precipitation_forecast_n AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM precipitation_forecast_n;


-- For Pasadena, CA
-- Create view for max temperature forecast
CREATE OR REPLACE VIEW max_temp_data_view_p AS 
    (SELECT date as da, temp_max 
     FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
     WHERE city = 'Pasadena');

SELECT * FROM max_temp_data_view_p;


-- Create view for max temperature forecast
CREATE OR REPLACE VIEW precipitation_data_view_p AS 
    (SELECT date as da, precipitation
     FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
     WHERE city = 'Pasadena');

SELECT * FROM precipitation_data_view_p;


-- Create max temperature forecasting model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST lab1_max_temp_forecast_p(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'max_temp_data_view_p'),    --pass the view as input data
    TIMESTAMP_COLNAME => 'da',          -- specify time axis col
    TARGET_COLNAME => 'temp_max',       -- predict max temp
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);


-- Create precipitation forecasting model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST lab1_precipitation_forecast_p(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'precipitation_data_view_p'),    --pass the view as input data
    TIMESTAMP_COLNAME => 'da',          -- specify time axis col
    TARGET_COLNAME => 'precipitation',  -- predict precipitation
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);


-- Create predictions for next 7 days max temperature
BEGIN
    CALL lab1_max_temp_forecast_p!FORECAST(
        FORECASTING_PERIODS => 7,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE OR REPLACE TABLE max_temp_forecast_p AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM max_temp_forecast_p;


-- Create predictions for next 7 days precipitation
BEGIN
    CALL lab1_precipitation_forecast_p!FORECAST(
        FORECASTING_PERIODS => 7,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE OR REPLACE TABLE precipitation_forecast_p AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM precipitation_forecast_p;

-- Union Table
SELECT date as da, temp_max, precipitation, NULL as temp_max_forecast, NULL as precipitation_forecast, NULL as lower_bound, NULL as upper_bound, city
FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
WHERE city IN ('Northridge', 'Pasadena')
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, forecast AS temp_max_forecast, NULL as precipitation_forecast, lower_bound, upper_bound, 'Northridge' AS city
FROM max_temp_forecast_n
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, NULL as temp_max_forecast, forecast AS precipitation_forcast, lower_bound, upper_bound, 'Northridge' AS city
FROM precipitation_forecast_n
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, forecast AS temp_max_forecast, NULL as precipitation_forecast, lower_bound, upper_bound, 'Pasadena' AS city
FROM max_temp_forecast_p
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, NULL as temp_max_forecast, forecast AS precipitation_forecast, lower_bound, upper_bound, 'Pasadena' AS city
FROM precipitation_forecast_p
ORDER BY da DESC;


-- Optimized Union Table
SELECT da, city, MAX(temp_max) as temp_max, MAX(precipitation) as precipitation, MAX(temp_max_forecast) as temp_max_forecast, MAX(precipitation_forecast) as precipitation_forecast
FROM (SELECT date as da, temp_max, precipitation, NULL as temp_max_forecast, NULL as precipitation_forecast, NULL as lower_bound, NULL as upper_bound, city
FROM USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1
WHERE city IN ('Northridge', 'Pasadena')
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, forecast AS temp_max_forecast, NULL as precipitation_forecast, lower_bound, upper_bound, 'Northridge' AS city
FROM max_temp_forecast_n
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, NULL as temp_max_forecast, forecast AS precipitation_forcast, lower_bound, upper_bound, 'Northridge' AS city
FROM precipitation_forecast_n
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, forecast AS temp_max_forecast, NULL as precipitation_forecast, lower_bound, upper_bound, 'Pasadena' AS city
FROM max_temp_forecast_p
UNION
SELECT ts as da, NULL AS temp_max, NULL AS precipitation, NULL as temp_max_forecast, forecast AS precipitation_forecast, lower_bound, upper_bound, 'Pasadena' AS city
FROM precipitation_forecast_p ) AS combined_data
GROUP BY da, city
ORDER BY da DESC;