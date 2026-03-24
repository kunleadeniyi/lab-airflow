-- ============================================================
-- BigQuery bronze layer DDL
-- Project: dbt-airflow-project-f1
-- Dataset: bronze
--
-- Type mapping from ClickHouse:
--   UInt8 / UInt16 / UInt32  → INT64   (BQ has no unsigned types)
--   Int32                    → INT64
--   Float64 / Nullable(Float64) → FLOAT64  (BQ columns nullable by default)
--   String / LowCardinality(String)         → STRING
--   Nullable(String) / LowCardinality(Nullable(String)) → STRING
--   DateTime64(3, 'UTC') / Nullable(DateTime64) → TIMESTAMP
--   Bool                     → BOOL
--   Array(Nullable(UInt16))  → ARRAY<INT64>  (BQ arrays cannot contain NULLs;
--                                             null elements coerced to 0 in loader)
--
-- Partitioning strategy (see docs/decision_log.md — ADR-001):
--   High-volume time-series tables with a non-null date column
--     → PARTITION BY DATE(date), DAY granularity
--   laps: date_start is nullable → PARTITION BY DATE(_loaded_at)
--   Small reference tables (meetings, sessions, drivers, stints)
--     → no partitioning, clustering only (all under ~10 GB/year)
--
-- Clustering mirrors ClickHouse ORDER BY keys (max 4 columns in BQ).
-- ============================================================


-- -------------------------------------------------------
-- MEETINGS
-- One row per Grand Prix event.
-- Small table (~30 rows/year) — clustering only, no partition.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.meetings`
(
    meeting_key           INT64     NOT NULL,
    year                  INT64     NOT NULL,
    circuit_key           INT64     NOT NULL,
    circuit_short_name    STRING,
    country_code          STRING,
    country_key           INT64     NOT NULL,
    country_name          STRING,
    date_start            TIMESTAMP,
    gmt_offset            STRING,
    location              STRING,
    meeting_name          STRING,
    meeting_official_name STRING,
    _source_file          STRING    NOT NULL,
    _loaded_at            TIMESTAMP NOT NULL
)
CLUSTER BY meeting_key;


-- -------------------------------------------------------
-- SESSIONS
-- One row per session (FP1-FP3, Qualifying, Sprint, Race) per meeting.
-- Small table (~150 rows/year) — clustering only, no partition.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.sessions`
(
    session_key        INT64     NOT NULL,
    meeting_key        INT64     NOT NULL,
    year               INT64     NOT NULL,
    circuit_key        INT64     NOT NULL,
    circuit_short_name STRING,
    country_code       STRING,
    country_key        INT64     NOT NULL,
    country_name       STRING,
    date_start         TIMESTAMP,
    date_end           TIMESTAMP,
    gmt_offset         STRING,
    location           STRING,
    session_name       STRING,
    session_type       STRING,
    _source_file       STRING    NOT NULL,
    _loaded_at         TIMESTAMP NOT NULL
)
CLUSTER BY meeting_key, session_key;


-- -------------------------------------------------------
-- DRIVERS
-- One row per driver per session per meeting.
-- Small table (~3,000 rows/year) — clustering only, no partition.
-- driver_number 65535 = unassigned/reserve driver sentinel.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.drivers`
(
    driver_number  INT64     NOT NULL,
    meeting_key    INT64     NOT NULL,
    session_key    INT64     NOT NULL,
    year           INT64     NOT NULL,
    broadcast_name STRING,
    country_code   STRING,
    first_name     STRING,
    last_name      STRING,
    full_name      STRING,
    name_acronym   STRING,
    team_name      STRING,
    team_colour    STRING,
    headshot_url   STRING,
    _source_file   STRING    NOT NULL,
    _loaded_at     TIMESTAMP NOT NULL
)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- LAPS
-- One row per lap per driver per session.
-- date_start is nullable (aborted laps) — partition on _loaded_at.
-- segments_sector_*: mini-sector colour codes (0 = not recorded).
--   2048=yellow, 2049=green, 2050=purple, 2051=pit lane.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.laps`
(
    meeting_key       INT64     NOT NULL,
    session_key       INT64     NOT NULL,
    driver_number     INT64     NOT NULL,
    year              INT64     NOT NULL,
    lap_number        INT64     NOT NULL,
    date_start        TIMESTAMP,           -- NULL for aborted/incomplete laps
    lap_duration      FLOAT64,
    duration_sector_1 FLOAT64,
    duration_sector_2 FLOAT64,
    duration_sector_3 FLOAT64,
    i1_speed          INT64,
    i2_speed          INT64,
    st_speed          INT64,
    is_pit_out_lap    BOOL      NOT NULL,
    segments_sector_1 ARRAY<INT64>,        -- null elements coerced to 0 in loader
    segments_sector_2 ARRAY<INT64>,
    segments_sector_3 ARRAY<INT64>,
    _source_file      STRING    NOT NULL,
    _loaded_at        TIMESTAMP NOT NULL
)
PARTITION BY DATE(_loaded_at)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- PITS
-- One row per pit stop per driver per session.
-- pit_duration nullable: entry without exit records.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.pits`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    lap_number    INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    pit_duration  FLOAT64,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- STINTS
-- One row per tyre stint per driver per session.
-- Small table (~5,000 rows/year) — clustering only, no partition.
-- lap_end nullable: ongoing stint at session end.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.stints`
(
    meeting_key       INT64     NOT NULL,
    session_key       INT64     NOT NULL,
    driver_number     INT64     NOT NULL,
    year              INT64     NOT NULL,
    stint_number      INT64     NOT NULL,
    lap_start         INT64     NOT NULL,
    lap_end           INT64,               -- NULL when stint is ongoing
    compound          STRING,
    tyre_age_at_start INT64     NOT NULL,
    _source_file      STRING    NOT NULL,
    _loaded_at        TIMESTAMP NOT NULL
)
CLUSTER BY meeting_key, session_key, driver_number, stint_number;


-- -------------------------------------------------------
-- POSITIONS
-- Time-series: one row per position change per driver (~3.7Hz).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.positions`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    position      INT64     NOT NULL,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- INTERVALS
-- Time-series: gap to leader and interval to car ahead.
-- Both columns are STRING to preserve '+1 LAP', '+2 LAPS' values.
-- `interval` is backtick-quoted — reserved keyword in BigQuery.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.intervals`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    gap_to_leader STRING,                  -- e.g. '1.234' or '+1 LAP'
    `interval`    STRING,                  -- e.g. '0.456' or '+1 LAP' (reserved keyword)
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- CAR DATA
-- High-frequency telemetry at ~3.7Hz per driver.
-- All sensor fields are bounded integers.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.car_data`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    rpm           INT64     NOT NULL,
    speed         INT64     NOT NULL,
    n_gear        INT64     NOT NULL,
    throttle      INT64     NOT NULL,
    brake         INT64     NOT NULL,
    drs           INT64     NOT NULL,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- LOCATIONS
-- High-frequency GPS at ~3.7Hz per driver.
-- x/y/z are circuit-relative coordinates in decimeters (can be negative).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.locations`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    x             INT64     NOT NULL,
    y             INT64     NOT NULL,
    z             INT64     NOT NULL,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- RACE CONTROL
-- Track-wide events: flags, safety cars, VSC, DRS zones.
-- driver_number = 0 for track-wide events (no associated driver).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.race_control`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    category      STRING,
    flag          STRING,
    scope         STRING,
    sector        INT64,                   -- NULL for track-wide scope
    lap_number    INT64,                   -- NULL for pre-race messages
    driver_number INT64     NOT NULL,      -- 0 = no associated driver
    message       STRING,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key;


-- -------------------------------------------------------
-- TEAM RADIO
-- One row per radio transmission per driver per session.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.team_radio`
(
    meeting_key   INT64     NOT NULL,
    session_key   INT64     NOT NULL,
    driver_number INT64     NOT NULL,
    year          INT64     NOT NULL,
    date          TIMESTAMP NOT NULL,
    recording_url STRING,
    _source_file  STRING    NOT NULL,
    _loaded_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(date)
CLUSTER BY meeting_key, session_key, driver_number;


-- -------------------------------------------------------
-- LOAD AUDIT
-- One row per MinIO source file (or per meeting within a file for meetings).
-- Stores the SHA-256 content hash to power idempotent loads:
--   same hash → skip, different hash → delete existing rows and reload.
-- See docs/decision_log.md — ADR-005.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.load_audit`
(
    source_file  STRING    NOT NULL,   -- MinIO object path, optionally suffixed with #meeting=N
    content_hash STRING    NOT NULL,   -- SHA-256 hex digest of the raw file bytes
    table_name   STRING    NOT NULL,   -- bronze table name the file was loaded into
    row_count    INT64     NOT NULL,   -- number of rows inserted from this file
    loaded_at    TIMESTAMP NOT NULL    -- when this record was last written
)
CLUSTER BY source_file;
