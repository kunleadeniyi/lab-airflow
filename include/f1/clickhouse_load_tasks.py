"""
ClickHouse bronze load functions.

Each function reads raw JSON from MinIO and inserts rows into the
corresponding bronze.* table. These are intentionally decoupled from
the API ingestion tasks — MinIO is the source of truth; ClickHouse is
a downstream consumer with its own failure boundary.

year is always passed explicitly. For tables where the OpenF1 API does
not include year in the response, year is injected from the call site
(matching the MinIO path prefix used during ingestion).

meeting_key is optional. When provided, only that meeting is loaded.
When omitted, all meetings for the year are loaded (full-year backfill).
"""

import json

import pendulum

from helpers.clickhouse import get_clickhouse_client
from helpers.logger import logger
from helpers.minio import get_minio_client

BUCKET = 'formula1'


# -------------------------------------------------------
# Internal helpers
# -------------------------------------------------------

def _parse_dt(s):
    """Parse an ISO 8601 string to a UTC-aware datetime. Returns None for None."""
    if s is None:
        return None
    return pendulum.parse(s).in_tz('UTC')


def _to_gap_str(v):
    """
    Normalise a gap/interval value to string or None.
    The OpenF1 API returns floats (1.234), strings ('+1 LAP', '+2 LAPS'),
    or null. All are stored as Nullable(String) to preserve lapped-car info.
    """
    if v is None:
        return None
    return str(v)


def _read_json(minio_client, object_name):
    """
    Download and decode a single JSON object from MinIO.
    Always returns a list. Non-list payloads (e.g. API error dicts like
    {"detail": "No results found."}, empty files) are logged and skipped.
    """
    response = minio_client.get_object(BUCKET, object_name)
    try:
        raw = response.read()
    finally:
        response.close()
        response.release_conn()

    if not raw:
        logger.warning("_read_json: %s is empty, skipping", object_name)
        return []

    data = json.loads(raw)

    if not isinstance(data, list):
        logger.warning(
            "_read_json: %s contains %s instead of a list (value=%r), skipping",
            object_name, type(data).__name__, data,
        )
        return []

    logger.info("_read_json: %s — %d rows", object_name, len(data))
    return data


def _list_objects(minio_client, prefix):
    """Return all object names under a MinIO prefix."""
    return [
        obj.object_name
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
    ]


_INSERT_CHUNK_SIZE = 50_000  # max rows per INSERT call — keeps ClickHouse memory bounded


def _insert(ch_client, table, rows, columns):
    """
    Insert rows and log the result. No-ops cleanly on empty input.
    Rows are chunked at _INSERT_CHUNK_SIZE to prevent ClickHouse MEMORY_LIMIT_EXCEEDED
    errors on large single-file payloads (car_data, positions, locations).
    """
    if not rows:
        logger.info(f"No rows to insert into {table}")
        return
    for start in range(0, len(rows), _INSERT_CHUNK_SIZE):
        chunk = rows[start:start + _INSERT_CHUNK_SIZE]
        ch_client.insert(table, chunk, column_names=columns)
        logger.info(f"Inserted rows {start}–{start + len(chunk) - 1} ({len(chunk)} rows) into {table}")
    logger.info(f"Total: {len(rows)} rows inserted into {table}")


def _prefix(year, entity, meeting_key=None):
    """Build the MinIO prefix for an entity, optionally scoped to a meeting."""
    if meeting_key:
        return f"{year}/{entity}/{meeting_key}/"
    return f"{year}/{entity}/"


# -------------------------------------------------------
# Meeting-level loaders (one file per meeting)
# -------------------------------------------------------

def _load_meetings(year, meeting_key=None):
    """
    Meetings are stored as a single file for the whole year:
      {year}/meetings/meetings.json
    meeting_key is applied as a post-read filter, not a path filter.
    """
    logger.info("_load_meetings: year=%r (type=%s) meeting_key=%r (type=%s)",
                year, type(year).__name__, meeting_key, type(meeting_key).__name__)
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, f"{year}/meetings/")
    if not objects:
        logger.warning(f"No meeting files found for year={year}")
        return

    columns = [
        'meeting_key', 'year', 'circuit_key', 'circuit_short_name',
        'country_code', 'country_key', 'country_name', 'date_start',
        'gmt_offset', 'location', 'meeting_name', 'meeting_official_name',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                if meeting_key and str(r['meeting_key']) != str(meeting_key):
                    continue
                rows.append([
                    r['meeting_key'],
                    r['year'],
                    r.get('circuit_key') or 0,
                    r.get('circuit_short_name') or '',
                    r.get('country_code') or '',
                    r.get('country_key') or 0,
                    r.get('country_name') or '',
                    _parse_dt(r.get('date_start')),
                    r.get('gmt_offset') or '',
                    r.get('location') or '',
                    r.get('meeting_name') or '',
                    r.get('meeting_official_name') or '',
                    obj,
                ])
            except Exception as e:
                logger.error("_load_meetings: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.meetings', rows, columns)


def _load_sessions(year, meeting_key=None):
    logger.info("_load_sessions: year=%r (type=%s) meeting_key=%r (type=%s)",
                year, type(year).__name__, meeting_key, type(meeting_key).__name__)
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'sessions', meeting_key))
    if not objects:
        logger.warning(f"No session files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'session_key', 'meeting_key', 'year', 'circuit_key', 'circuit_short_name',
        'country_code', 'country_key', 'country_name', 'date_start', 'date_end',
        'gmt_offset', 'location', 'session_name', 'session_type',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                date_end = _parse_dt(r.get('date_end'))
                if date_end is None:
                    logger.warning("_load_sessions: skipping session %s in %s — date_end is null", r.get('session_key'), obj)
                    continue
                rows.append([
                    r['session_key'],
                    r['meeting_key'],
                    r['year'],
                    r.get('circuit_key') or 0,
                    r.get('circuit_short_name') or '',
                    r.get('country_code') or '',
                    r.get('country_key') or 0,
                    r.get('country_name') or '',
                    _parse_dt(r.get('date_start')),
                    date_end,
                    r.get('gmt_offset') or '',
                    r.get('location') or '',
                    r.get('session_name') or '',
                    r.get('session_type') or '',
                    obj,
                ])
            except Exception as e:
                logger.error("_load_sessions: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.sessions', rows, columns)


def _load_drivers(year, meeting_key=None):
    """year is injected — the OpenF1 drivers endpoint does not return it."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'drivers', meeting_key))
    if not objects:
        logger.warning(f"No driver files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'driver_number', 'meeting_key', 'session_key', 'year',
        'broadcast_name', 'country_code', 'first_name', 'last_name',
        'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['driver_number'],
                    r['meeting_key'],
                    r['session_key'],
                    int(year),
                    r.get('broadcast_name') or '',
                    r.get('country_code') or '',   # API returns null for some drivers — '' is the sentinel
                    r.get('first_name') or '',
                    r.get('last_name') or '',
                    r.get('full_name') or '',
                    r.get('name_acronym') or '',
                    r.get('team_name') or '',
                    r.get('team_colour') or '',
                    r.get('headshot_url') or '',
                    obj,
                ])
            except Exception as e:
                logger.error("_load_drivers: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.drivers', rows, columns)


def _load_pits(year, meeting_key=None):
    """year is injected — not present in the OpenF1 pit endpoint response."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'pits', meeting_key))
    if not objects:
        logger.warning(f"No pit files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year',
        'lap_number', 'date', 'pit_duration',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r['driver_number'],
                    int(year),
                    r.get('lap_number') or 0,   # Nullable in API but UInt8 in ORDER BY — 0 sentinel
                    _parse_dt(r.get('date')),
                    r.get('pit_duration'),       # Nullable(Float64) — None is valid
                    obj,
                ])
            except Exception as e:
                logger.error("_load_pits: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.pits', rows, columns)


def _load_stints(year, meeting_key=None):
    """year is injected — not present in the OpenF1 stints endpoint response."""
    logger.info("_load_stints: year=%r (type=%s) meeting_key=%r (type=%s)",
                year, type(year).__name__, meeting_key, type(meeting_key).__name__)
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'stints', meeting_key))
    if not objects:
        logger.warning(f"No stint files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year',
        'stint_number', 'lap_start', 'lap_end', 'compound', 'tyre_age_at_start',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r.get('meeting_key') or 0,
                    r.get('session_key') or 0,
                    r.get('driver_number') or 0,   # API can return null — 0 sentinel (in ORDER BY)
                    int(year),
                    r.get('stint_number') or 0,
                    r.get('lap_start') or 0,
                    r.get('lap_end'),               # Nullable(UInt8) — None when stint is ongoing
                    r.get('compound') or '',
                    r.get('tyre_age_at_start') or 0,
                    obj,
                ])
            except Exception as e:
                logger.error("_load_stints: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.stints', rows, columns)


def _load_race_control(year, meeting_key=None):
    """year is injected. Several fields are nullable per the schema."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'race_control', meeting_key))
    if not objects:
        logger.warning(f"No race_control files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'year', 'date',
        'category', 'flag', 'scope', 'sector', 'lap_number', 'driver_number', 'message',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    int(year),
                    _parse_dt(r.get('date')),
                    r.get('category') or '',
                    r.get('flag'),          # Nullable(String) — None for non-flag messages
                    r.get('scope') or '',   # API returns null for some messages — '' sentinel
                    r.get('sector'),        # Nullable(UInt8)
                    r.get('lap_number'),    # Nullable(UInt8)
                    r.get('driver_number') or 0,  # 0 = no associated driver (track-wide events)
                    r.get('message') or '',
                    obj,
                ])
            except Exception as e:
                logger.error("_load_race_control: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.race_control', rows, columns)


def _load_team_radio(year, meeting_key=None):
    """year is injected."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'team_radio', meeting_key))
    if not objects:
        logger.warning(f"No team_radio files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year', 'date', 'recording_url',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r.get('driver_number') or 0,
                    int(year),
                    _parse_dt(r.get('date')),
                    r.get('recording_url') or '',
                    obj,
                ])
            except Exception as e:
                logger.error("_load_team_radio: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.team_radio', rows, columns)


# -------------------------------------------------------
# Session+driver-level loaders (one file per driver per session)
# -------------------------------------------------------

def _load_laps(year, meeting_key=None):
    """
    year is injected. Nullable fields: lap_duration, sector durations,
    trap speeds (incomplete laps). segments_sector_* are arrays — null
    in the API maps to an empty array in the column.
    """
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'laps', meeting_key))
    if not objects:
        logger.warning(f"No lap files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year',
        'lap_number', 'date_start', 'lap_duration',
        'duration_sector_1', 'duration_sector_2', 'duration_sector_3',
        'i1_speed', 'i2_speed', 'st_speed',
        'is_pit_out_lap',
        'segments_sector_1', 'segments_sector_2', 'segments_sector_3',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r.get('driver_number') or 0,
                    int(year),
                    r.get('lap_number') or 0,
                    _parse_dt(r.get('date_start')),     # Nullable(DateTime64) — null for aborted laps
                    r.get('lap_duration'),              # Nullable(Float64)
                    r.get('duration_sector_1'),         # Nullable(Float64)
                    r.get('duration_sector_2'),         # Nullable(Float64)
                    r.get('duration_sector_3'),         # Nullable(Float64)
                    r.get('i1_speed'),                  # Nullable(UInt16)
                    r.get('i2_speed'),                  # Nullable(UInt16)
                    r.get('st_speed'),                  # Nullable(UInt16)
                    bool(r.get('is_pit_out_lap', False)),
                    r.get('segments_sector_1') or [],   # null → empty array
                    r.get('segments_sector_2') or [],
                    r.get('segments_sector_3') or [],
                    obj,
                ])
            except Exception as e:
                logger.error("_load_laps: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.laps', rows, columns)


def _load_positions(year, meeting_key=None):
    """year is injected."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'positions', meeting_key))
    if not objects:
        logger.warning(f"No position files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year', 'date', 'position',
        '_source_file',
    ]
    for obj in objects:
        rows = []  # reset per file — positions is high-volume; accumulating all files causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r['driver_number'],
                    int(year),
                    _parse_dt(r.get('date')),
                    r['position'],
                    obj,
                ])
            except Exception as e:
                logger.error("_load_positions: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise
        _insert(ch, 'bronze.positions', rows, columns)


def _load_intervals(year, meeting_key=None):
    """year is injected. gap_to_leader and interval are both nullable."""
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'intervals', meeting_key))
    if not objects:
        logger.warning(f"No interval files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year',
        'date', 'gap_to_leader', 'interval',
        '_source_file',
    ]
    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r['driver_number'],
                    int(year),
                    _parse_dt(r.get('date')),
                    _to_gap_str(r.get('gap_to_leader')),  # Nullable(String) — preserves '+1 LAP'
                    _to_gap_str(r.get('interval')),       # Nullable(String)
                    obj,
                ])
            except Exception as e:
                logger.error("_load_intervals: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise

    _insert(ch, 'bronze.intervals', rows, columns)


def _load_car_data(year, meeting_key=None):
    """
    year is injected. High-frequency telemetry (~3.7Hz).
    All sensor fields are bounded integers — UInt types in the schema.
    """
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'car_data', meeting_key))
    if not objects:
        logger.warning(f"No car_data files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year',
        'date', 'rpm', 'speed', 'n_gear', 'throttle', 'brake', 'drs',
        '_source_file',
    ]
    for obj in objects:
        rows = []  # reset per file — car_data is ~3.7Hz telemetry; accumulating all files causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r.get('driver_number') or 0,
                    int(year),
                    _parse_dt(r.get('date')),
                    r.get('rpm') or 0,
                    r.get('speed') or 0,
                    r.get('n_gear') or 0,
                    r.get('throttle') or 0,
                    r.get('brake') or 0,
                    r.get('drs') or 0,
                    obj,
                ])
            except Exception as e:
                logger.error("_load_car_data: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise
        _insert(ch, 'bronze.car_data', rows, columns)


def _load_locations(year, meeting_key=None):
    """
    year is injected. High-frequency GPS (~3.7Hz).
    x/y/z are circuit-relative coordinates — Int32 (can be negative).
    """
    minio = get_minio_client()
    ch = get_clickhouse_client()

    objects = _list_objects(minio, _prefix(year, 'locations', meeting_key))
    if not objects:
        logger.warning(f"No location files found for year={year}, meeting_key={meeting_key}")
        return

    columns = [
        'meeting_key', 'session_key', 'driver_number', 'year', 'date', 'x', 'y', 'z',
        '_source_file',
    ]
    for obj in objects:
        rows = []  # reset per file — locations is ~3.7Hz GPS; accumulating all files causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append([
                    r['meeting_key'],
                    r['session_key'],
                    r.get('driver_number') or 0,
                    int(year),
                    _parse_dt(r.get('date')),
                    r.get('x') or 0,
                    r.get('y') or 0,
                    r.get('z') or 0,
                    obj,
                ])
            except Exception as e:
                logger.error("_load_locations: error on row %d of %s — %s | row=%r", i, obj, e, r, exc_info=True)
                raise
        _insert(ch, 'bronze.locations', rows, columns)
