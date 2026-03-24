{{
  config(
    materialized = 'table',
    engine       = 'ReplacingMergeTree()',
    order_by     = '(year, meeting_key, session_key, driver_number, lap_number)',
  )
}}

/*
  Lap time progression per driver per race session.
  Includes a rolling 3-lap average to smooth out SC/VSC laps and tyre warm-up.
  Excludes:
    - pit out laps (tyre temp distorts times)
    - null lap_duration (aborted / incomplete laps)
  Enriched with driver and session context for direct use in Superset.
*/

SELECT
    l.year,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    l.lap_duration,
    l.duration_sector_1,
    l.duration_sector_2,
    l.duration_sector_3,
    l.i1_speed,
    l.st_speed,
    -- rolling 3-lap average centred on current lap
    avg(l.lap_duration) OVER (
        PARTITION BY l.session_key, l.driver_number
        ORDER BY l.lap_number
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )                                           AS lap_duration_rolling_3,
    -- cumulative delta vs driver's personal best in this session
    l.lap_duration - min(l.lap_duration) OVER (
        PARTITION BY l.session_key, l.driver_number
    )                                           AS delta_to_personal_best,
    d.name_acronym,
    d.team_name,
    d.team_colour,
    m.meeting_name,
    m.circuit_short_name,
    s.session_name,
    s.session_type
FROM {{ ref('fct_laps') }}     AS l
LEFT JOIN {{ ref('dim_drivers') }}  AS d USING (driver_number)
LEFT JOIN {{ ref('dim_meetings') }} AS m USING (meeting_key)
LEFT JOIN {{ ref('dim_sessions') }} AS s USING (session_key)
WHERE
    l.lap_duration IS NOT NULL
    AND l.is_pit_out_lap = false
