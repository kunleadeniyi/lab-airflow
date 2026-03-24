{{
  config(
    materialized         = 'incremental',
    engine               = 'ReplacingMergeTree()',
    order_by             = '(meeting_key, session_key, driver_number, date)',
    unique_key           = ['meeting_key', 'session_key', 'driver_number', 'date'],
    incremental_strategy = 'delete+insert',
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
AND _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
