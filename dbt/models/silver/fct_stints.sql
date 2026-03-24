{{
  config(
    materialized         = 'incremental',
    engine               = 'ReplacingMergeTree()',
    order_by             = '(meeting_key, session_key, driver_number, stint_number)',
    unique_key           = ['meeting_key', 'session_key', 'driver_number', 'stint_number'],
    incremental_strategy = 'delete+insert',
  )
}}

SELECT
    meeting_key,
    session_key,
    driver_number,
    year,
    stint_number,
    lap_start,
    lap_end,
    compound,
    tyre_age_at_start,
    lap_end - lap_start + 1 AS laps_on_tyre,
    _loaded_at
FROM {{ source('bronze', 'stints') }}
WHERE driver_number != 0

{% if is_incremental() %}
AND _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
