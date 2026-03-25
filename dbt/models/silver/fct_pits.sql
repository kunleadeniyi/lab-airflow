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
    date,
    pit_duration,
    _loaded_at
FROM {{ source('bronze', 'pits') }}
WHERE driver_number != 0

{% if is_incremental() %}
AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
