{{
  config(
    materialized  = 'table',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(meeting_key, session_key)',
  )
}}

SELECT
    session_key,
    meeting_key,
    year,
    circuit_key,
    any(circuit_short_name) AS circuit_short_name,
    any(country_code)       AS country_code,
    any(country_key)        AS country_key,
    any(country_name)       AS country_name,
    any(date_start)         AS date_start,
    any(date_end)           AS date_end,
    any(gmt_offset)         AS gmt_offset,
    any(location)           AS location,
    any(session_name)       AS session_name,
    any(session_type)       AS session_type,
    max(_loaded_at)         AS _loaded_at
FROM {{ source('bronze', 'sessions') }}
GROUP BY session_key, meeting_key, year, circuit_key
