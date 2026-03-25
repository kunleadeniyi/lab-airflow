{{
  config(
    materialized         = 'incremental',
    unique_key           = ['meeting_key', 'session_key', 'driver_number', 'lap_number'],
    incremental_strategy = 'merge',
  )
}}

SELECT
    meeting_key,
    session_key,
    driver_number,
    year,
    lap_number,
    date_start,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    is_pit_out_lap,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    _loaded_at
FROM {{ source('bronze', 'laps') }}

{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
