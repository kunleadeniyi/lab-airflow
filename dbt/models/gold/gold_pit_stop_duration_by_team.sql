{{
  config(
    materialized = 'table',
    engine       = 'ReplacingMergeTree()',
    order_by     = '(year, meeting_key, team_name)',
  )
}}

/*
  Average pit stop duration per team per race.
  Excludes:
    - null pit_duration (incomplete stops — car entered but did not exit)
    - pit_duration < 1.5s  (drive-through penalties recorded as pit stops)
    - pit_duration > 60s   (red flag / long stops that skew averages)
  Joins to dim_drivers to get team_name.
  Uses the most recent team_name known for a driver (dim_drivers is session-latest).
*/

WITH pit_with_team AS (
    SELECT
        p.meeting_key,
        p.session_key,
        p.year,
        p.driver_number,
        p.lap_number,
        p.pit_duration,
        d.team_name,
        d.name_acronym
    FROM {{ ref('fct_pits') }} AS p
    LEFT JOIN {{ ref('dim_drivers') }} AS d USING (driver_number)
    WHERE
        p.pit_duration IS NOT NULL
        AND p.pit_duration >= 1.5
        AND p.pit_duration <= 60.0
)

SELECT
    year,
    meeting_key,
    any(m.meeting_name)             AS meeting_name,
    any(m.circuit_short_name)       AS circuit_short_name,
    team_name,
    count()                         AS total_stops,
    round(avg(pit_duration), 3)     AS avg_pit_duration_s,
    round(min(pit_duration), 3)     AS fastest_stop_s,
    round(max(pit_duration), 3)     AS slowest_stop_s,
    round(stddevSamp(pit_duration), 3) AS stddev_s
FROM pit_with_team
LEFT JOIN {{ ref('dim_meetings') }} AS m USING (meeting_key)
GROUP BY year, meeting_key, team_name
ORDER BY year, meeting_key, avg_pit_duration_s
