{{
  config(
    materialized = 'table',
  )
}}

SELECT
    meeting_key,
    year,
    circuit_key,
    ANY_VALUE(circuit_short_name)    AS circuit_short_name,
    ANY_VALUE(country_code)          AS country_code,
    ANY_VALUE(country_key)           AS country_key,
    ANY_VALUE(country_name)          AS country_name,
    ANY_VALUE(date_start)            AS date_start,
    ANY_VALUE(gmt_offset)            AS gmt_offset,
    ANY_VALUE(location)              AS location,
    ANY_VALUE(meeting_name)          AS meeting_name,
    ANY_VALUE(meeting_official_name) AS meeting_official_name,
    MAX(_loaded_at)                  AS _loaded_at
FROM {{ source('bronze', 'meetings') }}
GROUP BY meeting_key, year, circuit_key
