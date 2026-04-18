{% snapshot weather_snapshot %}

{{
    config(
      target_database='USER_DB_BULLFROG',
      target_schema='RAW',
      unique_key='city || date',
      strategy='check',
      check_cols=['temp_mean', 'precipitation']
    )
}}

SELECT *
FROM RAW.WEATHER_DATA_LAB1

{% endsnapshot %}