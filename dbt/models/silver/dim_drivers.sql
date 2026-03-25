{{
  config(
    materialized = 'table',
  )
}}

SELECT
    driver_number,
    ANY_VALUE(broadcast_name)   AS broadcast_name,
    ANY_VALUE(full_name)        AS full_name,
    ANY_VALUE(name_acronym)     AS name_acronym,
    ANY_VALUE(team_name)        AS team_name,
    ANY_VALUE(team_colour)      AS team_colour,
    ANY_VALUE(country_code)     AS country_code,
    MAX(_loaded_at)             AS _loaded_at
FROM {{ source('bronze', 'drivers') }}
WHERE driver_number != 0
GROUP BY driver_number
