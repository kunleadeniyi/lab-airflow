{{
  config(
    materialized  = 'table',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(meeting_key)',
  )
}}

SELECT
    meeting_key,
    year,
    circuit_key,
    any(circuit_short_name)    AS circuit_short_name,
    any(country_code)          AS country_code,
    any(country_key)           AS country_key,
    any(country_name)          AS country_name,
    any(date_start)            AS date_start,
    any(gmt_offset)            AS gmt_offset,
    any(location)              AS location,
    any(meeting_name)          AS meeting_name,
    any(meeting_official_name) AS meeting_official_name,
    max(_loaded_at)            AS _loaded_at
FROM {{ source('bronze', 'meetings') }}
GROUP BY meeting_key, year, circuit_key
