{{
  config(
    materialized         = 'incremental',
    unique_key           = ['meeting_key', 'session_key', 'driver_number', 'date'],
    incremental_strategy = 'merge',
  )
}}

SELECT
    meeting_key,
    session_key,
    driver_number,
    year,
    date,
    position,
    _loaded_at
FROM {{ source('bronze', 'positions') }}
WHERE driver_number != 0

{% if is_incremental() %}
AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
