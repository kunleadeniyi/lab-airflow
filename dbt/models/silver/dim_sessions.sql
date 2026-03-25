{{
  config(
    materialized = 'table',
  )
}}

SELECT
    session_key,
    meeting_key,
    year,
    circuit_key,
    ANY_VALUE(circuit_short_name) AS circuit_short_name,
    ANY_VALUE(country_code)       AS country_code,
    ANY_VALUE(country_key)        AS country_key,
    ANY_VALUE(country_name)       AS country_name,
    ANY_VALUE(date_start)         AS date_start,
    ANY_VALUE(date_end)           AS date_end,
    ANY_VALUE(gmt_offset)         AS gmt_offset,
    ANY_VALUE(location)           AS location,
    ANY_VALUE(session_name)       AS session_name,
    ANY_VALUE(session_type)       AS session_type,
    MAX(_loaded_at)               AS _loaded_at
FROM {{ source('bronze', 'sessions') }}
GROUP BY session_key, meeting_key, year, circuit_key
