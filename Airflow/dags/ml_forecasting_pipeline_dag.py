from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


@task
def train(conn_id, input_table, view, model, target, city):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    try:
        cursor.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT date AS ds, {target},
            FROM {input_table}
            WHERE city = '{city}';
        """)

        cursor.execute(f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model} (
                INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{view}'),
                TIMESTAMP_COLNAME => 'ds',
                TARGET_COLNAME => '{target}',
                CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
            );
        """)

        print(f"Model {model} created")

    finally:
        cursor.close()
        con.close()


@task
def predict(conn_id, model, table):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    try:
        cursor.execute(f"""
        BEGIN
            CALL {model}!FORECAST(FORECASTING_PERIODS => 7);
            LET x := SQLID;
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
        """)

        print(f"Predictions saved to {table}")

    finally:
        cursor.close()
        con.close()


@task
def create_final(conn_id, raw):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    con = hook.get_conn()
    cursor = con.cursor()

    try:
        cursor.execute(f"""
        CREATE OR REPLACE TABLE FINAL_WEATHER_FORECAST AS
        SELECT da, city,
               MAX(temp_max) AS temp_max,
               MAX(precipitation) AS precipitation,
               MAX(temp_max_forecast) AS temp_max_forecast,
               MAX(precipitation_forecast) AS precipitation_forecast
    FROM (
        SELECT date AS da, temp_max, precipitation,
           NULL AS temp_max_forecast, 
           NULL AS precipitation_forecast, 
           city
        FROM {raw}

        UNION ALL

        SELECT ts, NULL, NULL, forecast AS temp_max_forecast, NULL, 'Northridge'
        FROM max_temp_forecast_n

        UNION ALL

        SELECT ts, NULL, NULL, NULL, forecast AS precipitation_forecast, 'Northridge'
        FROM precipitation_forecast_n

        UNION ALL

        SELECT ts, NULL, NULL, forecast AS temp_max_forecast, NULL, 'Pasadena'
        FROM max_temp_forecast_p

        UNION ALL

        SELECT ts, NULL, NULL, NULL, forecast AS precipitation_forecast, 'Pasadena'
        FROM precipitation_forecast_p
)
        GROUP BY da, city;
        """)

        print("Final table created")

    finally:
        cursor.close()
        con.close()


# DAG
with DAG(
    dag_id='ML_weather_forecast_pipeline',
    start_date=datetime(2026, 2, 23),
    schedule='30 2 * * *',
    catchup=False
) as dag:

    conn = 'snowflake_conn'
    raw = 'USER_DB_BULLFROG.RAW.WEATHER_DATA_LAB1'

    # Northridge
    n_temp_train = train(conn, raw, 'view_n_temp', 'model_n_temp', 'temp_max', 'Northridge')
    n_temp_pred = predict(conn, 'model_n_temp', 'max_temp_forecast_n')

    n_prec_train = train(conn, raw, 'view_n_prec', 'model_n_prec', 'precipitation', 'Northridge')
    n_prec_pred = predict(conn, 'model_n_prec', 'precipitation_forecast_n')

    # Pasadena
    p_temp_train = train(conn, raw, 'view_p_temp', 'model_p_temp', 'temp_max', 'Pasadena')
    p_temp_pred = predict(conn, 'model_p_temp', 'max_temp_forecast_p')

    p_prec_train = train(conn, raw, 'view_p_prec', 'model_p_prec', 'precipitation', 'Pasadena')
    p_prec_pred = predict(conn, 'model_p_prec', 'precipitation_forecast_p')

    final = create_final(conn, raw)

    # Dependencies
    n_temp_train >> n_temp_pred
    n_prec_train >> n_prec_pred
    p_temp_train >> p_temp_pred
    p_prec_train >> p_prec_pred

    [n_temp_pred, n_prec_pred, p_temp_pred, p_prec_pred] >> final