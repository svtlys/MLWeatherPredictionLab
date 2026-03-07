from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

@task
def train(snowflake_conn_id, train_input_table, train_view, model_name, target_col, city):
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS
        (SELECT date as ds, {target_col}, city FROM {train_input_table} WHERE city = '{city}');"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        TIMESTAMP_COLNAME => 'ds',
        TARGET_COLNAME => '{target_col}',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    cursor.execute(create_view_sql)
    cursor.execute(create_model_sql)
    print(f"Model {model_name} for {target_col} in {city} created.")

@task
def predict(snowflake_conn_id, model_name, forecast_table):
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    make_prediction_sql = f"""BEGIN
        CALL {model_name}!FORECAST(FORECASTING_PERIODS => 7, CONFIG_OBJECT => {{'prediction_interval': 0.95}});
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    cursor.execute(make_prediction_sql)

@task
def create_final_table(snowflake_conn_id, raw_table):
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    final_sql = f"""CREATE OR REPLACE TABLE FINAL_WEATHER_FORECAST AS
    SELECT da, city, MAX(temp_max) as temp_max, MAX(precipitation) as precipitation, 
           MAX(temp_max_forecast) as temp_max_forecast, MAX(precipitation_forecast) as precipitation_forecast
    FROM (
        SELECT date as da, temp_max, precipitation, NULL as temp_max_forecast, NULL as precipitation_forecast, city
        FROM {raw_table} WHERE city IN ('Northridge', 'Pasadena')
        UNION
        SELECT ts as da, NULL, NULL, forecast, NULL, 'Northridge' FROM max_temp_forecast_n
        UNION
        SELECT ts as da, NULL, NULL, NULL, forecast, 'Northridge' FROM precipitation_forecast_n
        UNION
        SELECT ts as da, NULL, NULL, forecast, NULL, 'Pasadena' FROM max_temp_forecast_p
        UNION
        SELECT ts as da, NULL, NULL, NULL, forecast, 'Pasadena' FROM precipitation_forecast_p
    ) GROUP BY da, city;"""
    
    try:
        cursor.execute(final_sql)
    finally:
        cursor.close()
        con.close()

with DAG(
    dag_id='ML_weather_forecast_pipeline', 
    start_date=datetime(2026, 2, 23), 
    schedule='30 2 * * *', 
    catchup=False
) as dag:
    
    conn_id = 'snowflake_conn'
    raw = 'USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1'

    # Northridge Tasks 
    t_n_temp = train(conn_id, raw, 'view_n_temp', 'model_n_temp', 'temp_max', 'Northridge')
    p_n_temp = predict(conn_id, 'model_n_temp', 'max_temp_forecast_n')
    
    t_n_prec = train(conn_id, raw, 'view_n_prep', 'model_n_prep', 'precipitation', 'Northridge')
    p_n_prec = predict(conn_id, 'model_n_prep', 'precipitation_forecast_n')

    # Pasadena Tasks
    t_p_temp = train(conn_id, raw, 'view_p_temp', 'model_p_temp', 'temp_max', 'Pasadena')
    p_p_temp = predict(conn_id, 'model_p_temp', 'max_temp_forecast_p')
    
    t_p_prec = train(conn_id, raw, 'view_p_prep', 'model_p_prep', 'precipitation', 'Pasadena')
    p_p_prec = predict(conn_id, 'model_p_prep', 'precipitation_forecast_p')

    # Final Union Table
    final_union = create_final_table(conn_id, raw)

    t_n_temp >> p_n_temp
    t_n_prec >> p_n_prec
    t_p_temp >> p_p_temp
    t_p_prec >> p_p_prec

    [p_n_temp, p_n_prec, p_p_temp, p_p_prec] >> final_union