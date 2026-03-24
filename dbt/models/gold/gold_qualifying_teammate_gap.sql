{{
  config(
    materialized = 'table',
    engine       = 'ReplacingMergeTree()',
    order_by     = '(year, meeting_key, team_name, driver_number)',
  )
}}

/*
  Qualifying gap between teammates.
  Uses each driver's best lap in the qualifying session (session_type = 'Qualifying').
  Computes:
    - best_lap_s: personal best lap time in qualifying
    - teammate_best_s: the other driver on the same team
    - gap_to_teammate_s: positive = slower than teammate, negative = faster
    - gap_pct: gap as % of the faster driver's time (consistent with F1 broadcast metric)
  One row per driver per qualifying session.
  Drivers without a teammate in the data (e.g. reserve drivers) are excluded.
*/

WITH qual_best AS (
    SELECT
        l.year,
        l.meeting_key,
        l.session_key,
        l.driver_number,
        min(l.lap_duration) AS best_lap_s
    FROM {{ ref('fct_laps') }}     AS l
    LEFT JOIN {{ ref('dim_sessions') }} AS s USING (session_key)
    WHERE
        l.lap_duration IS NOT NULL
        AND l.is_pit_out_lap = false
        AND s.session_type = 'Qualifying'
    GROUP BY l.year, l.meeting_key, l.session_key, l.driver_number
),

qual_with_team AS (
    SELECT
        q.year,
        q.meeting_key,
        q.session_key,
        q.driver_number,
        q.best_lap_s,
        d.name_acronym,
        d.full_name,
        d.team_name
    FROM qual_best AS q
    LEFT JOIN {{ ref('dim_drivers') }} AS d USING (driver_number)
    WHERE d.team_name != ''
),

-- self-join on same team + same session to get teammate's time
paired AS (
    SELECT
        a.year,
        a.meeting_key,
        a.session_key,
        a.team_name,
        a.driver_number,
        a.name_acronym,
        a.full_name,
        a.best_lap_s,
        b.driver_number    AS teammate_driver_number,
        b.name_acronym     AS teammate_acronym,
        b.best_lap_s       AS teammate_best_s
    FROM qual_with_team AS a
    JOIN qual_with_team AS b
        ON  a.session_key  = b.session_key
        AND a.team_name    = b.team_name
        AND a.driver_number != b.driver_number
)

SELECT
    p.year,
    p.meeting_key,
    m.meeting_name,
    m.circuit_short_name,
    p.session_key,
    p.team_name,
    p.driver_number,
    p.name_acronym,
    p.full_name,
    round(p.best_lap_s, 3)                                          AS best_lap_s,
    p.teammate_driver_number,
    p.teammate_acronym,
    round(p.teammate_best_s, 3)                                     AS teammate_best_s,
    round(p.best_lap_s - p.teammate_best_s, 3)                      AS gap_to_teammate_s,
    round(
        (p.best_lap_s - p.teammate_best_s) / least(p.best_lap_s, p.teammate_best_s) * 100,
        4
    )                                                               AS gap_pct,
    if(p.best_lap_s <= p.teammate_best_s, 'faster', 'slower')      AS vs_teammate
FROM paired AS p
LEFT JOIN {{ ref('dim_meetings') }} AS m USING (meeting_key)
