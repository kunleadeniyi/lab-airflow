{{
  config(
    materialized  = 'table',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(driver_number)',
  )
}}

SELECT
    driver_number,
    any(broadcast_name)   AS broadcast_name,
    any(full_name)        AS full_name,
    any(name_acronym)     AS name_acronym,
    any(team_name)        AS team_name,
    any(team_colour)      AS team_colour,
    any(country_code)     AS country_code,
    max(_loaded_at)       AS _loaded_at
FROM {{ source('bronze', 'drivers') }}
WHERE driver_number != 0
GROUP BY driver_number
