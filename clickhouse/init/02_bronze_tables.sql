-- -------------------------------------------------------
-- MEETINGS
-- One row per Grand Prix event.
-- year comes directly from the OpenF1 API response.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.meetings
(
    meeting_key           UInt32,
    year                  UInt16,
    circuit_key           UInt32,
    circuit_short_name    LowCardinality(String),
    country_code          LowCardinality(String),
    country_key           UInt32,
    country_name          LowCardinality(String),
    date_start            DateTime64(3, 'UTC'),
    gmt_offset            LowCardinality(String),
    location              String,
    meeting_name          String,
    meeting_official_name String,
    _source_file          String,
    _loaded_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key);

-- -------------------------------------------------------
-- SESSIONS
-- One row per session (FP1-FP3, Qualifying, Sprint, Race) per meeting.
-- year comes directly from the OpenF1 API response.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.sessions
(
    session_key        UInt32,
    meeting_key        UInt32,
    year               UInt16,
    circuit_key        UInt32,
    circuit_short_name LowCardinality(String),
    country_code       LowCardinality(String),
    country_key        UInt32,
    country_name       LowCardinality(String),
    date_start         DateTime64(3, 'UTC'),
    date_end           DateTime64(3, 'UTC'),
    gmt_offset         LowCardinality(String),
    location           String,
    session_name       LowCardinality(String),
    session_type       LowCardinality(String),
    _source_file       String,
    _loaded_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key);

-- -------------------------------------------------------
-- DRIVERS
-- One row per driver per session per meeting.
-- year is injected by Airflow — the API does not return it.
-- headshot_url is String (empty string) rather than Nullable
-- to avoid nullable overhead; empty string signals no photo.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.drivers
(
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    meeting_key   UInt32,
    session_key   UInt32,
    year          UInt16,
    broadcast_name String,
    country_code  LowCardinality(String),
    first_name    String,
    last_name     String,
    full_name     String,
    name_acronym  LowCardinality(String),
    team_name     LowCardinality(String),
    team_colour   LowCardinality(String),
    headshot_url  String,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number);

-- -------------------------------------------------------
-- LAPS
-- One row per lap per driver per session.
-- year is injected by Airflow.
-- Nullable fields: incomplete laps (DNF, SC laps, pit-out
-- laps) frequently have null durations and trap speeds.
-- segments_sector_* encode mini-sector colours as integers:
--   2048=yellow, 2049=green, 2050=purple, 2051=pit lane.
-- UInt16 used because values exceed UInt8 range (>255).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.laps
(
    meeting_key       UInt32,
    session_key       UInt32,
    driver_number     UInt16,
    year              UInt16,
    lap_number        UInt8,
    date_start        Nullable(DateTime64(3, 'UTC')),  -- null for aborted/incomplete laps
    lap_duration      Nullable(Float64),
    duration_sector_1 Nullable(Float64),
    duration_sector_2 Nullable(Float64),
    duration_sector_3 Nullable(Float64),
    i1_speed          Nullable(UInt16),
    i2_speed          Nullable(UInt16),
    st_speed          Nullable(UInt16),
    is_pit_out_lap    Bool,
    segments_sector_1 Array(Nullable(UInt16)),  -- null elements = mini-sector not recorded
    segments_sector_2 Array(Nullable(UInt16)),
    segments_sector_3 Array(Nullable(UInt16)),
    _source_file      String,
    _loaded_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, lap_number);

-- -------------------------------------------------------
-- PITS
-- One row per pit stop per driver per session.
-- year is injected by Airflow.
-- pit_duration is nullable: the API occasionally returns
-- pit stop records with no duration (entry without exit).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.pits
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    lap_number    UInt8,
    date          DateTime64(3, 'UTC'),
    pit_duration  Nullable(Float64),
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, lap_number);

-- -------------------------------------------------------
-- STINTS
-- One row per tyre stint per driver per session.
-- year is injected by Airflow.
-- lap_end is nullable: the final active stint may have
-- no lap_end populated until the session finishes.
-- compound uses LowCardinality: only SOFT/MEDIUM/HARD/
-- INTERMEDIATE/WET/UNKNOWN in practice.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.stints
(
    meeting_key       UInt32,
    session_key       UInt32,
    driver_number     UInt16,
    year              UInt16,
    stint_number      UInt8,
    lap_start         UInt8,
    lap_end           Nullable(UInt8),
    compound          LowCardinality(String),
    tyre_age_at_start UInt8,
    _source_file      String,
    _loaded_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, stint_number);

-- -------------------------------------------------------
-- POSITIONS
-- Time-series: one row per position change per driver.
-- year is injected by Airflow.
-- position UInt8: max 20 cars on grid.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.positions
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    position      UInt8,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, date);

-- -------------------------------------------------------
-- INTERVALS
-- Time-series: gap to leader and interval to car ahead.
-- year is injected by Airflow.
-- Both gap fields are nullable: the race leader has a
-- gap_to_leader of 0.0 (not null), but lapped cars may
-- return null. interval is null for the leader.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.intervals
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    gap_to_leader Nullable(String),   -- e.g. '1.234' or '+1 LAP' for lapped cars
    interval      Nullable(String),   -- e.g. '0.456' or '+1 LAP'
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, date);

-- -------------------------------------------------------
-- CAR DATA
-- High-frequency telemetry at ~3.7Hz per driver.
-- year is injected by Airflow.
-- Compact integer types are deliberate: these are bounded
-- sensor readings. UInt8 throttle/brake (0-100 pct),
-- UInt8 n_gear (0-8), UInt16 rpm (0-~15000),
-- UInt16 speed (0-~380 km/h).
-- drs: 0-14 integer encoding DRS state
--   (8=available, 10=active, 12/14=active variants).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.car_data
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    rpm           UInt16,
    speed         UInt16,
    n_gear        UInt8,
    throttle      UInt8,
    brake         UInt8,
    drs           UInt8,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, date);

-- -------------------------------------------------------
-- LOCATIONS
-- High-frequency GPS at ~3.7Hz per driver.
-- year is injected by Airflow.
-- x/y/z are circuit-relative coordinates in decimeters.
-- Int32 to handle negative values (circuits wrap around origin).
-- NOTE: this table is bronze-only. The silver layer will not
-- include locations — GPS coordinates are not needed for the
-- analytical queries being answered in gold/Superset.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.locations
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    x             Int32,
    y             Int32,
    z             Int32,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, date);

-- -------------------------------------------------------
-- RACE CONTROL
-- Track-wide events: flags, safety cars, VSC, DRS zones.
-- year is injected by Airflow.
-- driver_number is Nullable: the majority of messages are
-- track-wide events with no associated driver.
-- flag and sector are Nullable for the same reason.
-- lap_number Nullable: some messages fire before lap 1.
-- category examples: "Flag", "SafetyCar", "Drs".
-- scope examples: "Track", "Driver", "Sector".
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.race_control
(
    meeting_key   UInt32,
    session_key   UInt32,
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    category      LowCardinality(String),
    flag          LowCardinality(Nullable(String)),
    scope         LowCardinality(String),
    sector        Nullable(UInt8),
    lap_number    Nullable(UInt8),
    driver_number UInt16,  -- 0 = no associated driver (track-wide events); UInt16 for sentinel values like 65535
    message       String,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, date);

-- -------------------------------------------------------
-- TEAM RADIO
-- One row per radio transmission per driver per session.
-- year is injected by Airflow.
-- recording_url points to the audio file on OpenF1 CDN.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.team_radio
(
    meeting_key   UInt32,
    session_key   UInt32,
    driver_number UInt16,  -- UInt16: API uses 65535 as sentinel for unassigned/reserve drivers
    year          UInt16,
    date          DateTime64(3, 'UTC'),
    recording_url String,
    _source_file  String,
    _loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY year
ORDER BY (meeting_key, session_key, driver_number, date);
