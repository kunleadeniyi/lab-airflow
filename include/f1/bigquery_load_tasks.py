"""
BigQuery bronze load functions.

Each function reads raw JSON from MinIO and inserts rows into the
corresponding bronze.* table via BigQuery load jobs (NDJSON, WRITE_APPEND).
These are intentionally decoupled from the API ingestion tasks — MinIO is
the source of truth; BigQuery is a downstream consumer with its own failure
boundary.

year is always passed explicitly. For tables where the OpenF1 API does
not include year in the response, year is injected from the call site
(matching the MinIO path prefix used during ingestion).

meeting_key is optional. When provided, only that meeting is loaded.
When omitted, all meetings for the year are loaded (full-year backfill).

Row format: list[dict] — BigQuery load jobs require dicts, not lists.
Timestamps: pendulum.DateTime (UTC-aware) — the BQ client serialises
  datetime subclasses via .isoformat() internally.
_loaded_at: injected at load time (BigQuery has no DEFAULT clause).
ARRAY<INT64> null coercion: null elements in segments_sector_* arrays
  are replaced with 0 (BigQuery arrays cannot contain NULL).
"""

import json

import pendulum
from google.cloud import bigquery

from helpers.bigquery import GCP_PROJECT, get_bigquery_client
from helpers.logger import logger
from helpers.minio import get_minio_client

BUCKET = 'formula1'
DATASET = 'bronze'

# BigQuery load job chunk size.
# load_table_from_json has a ~10 MB payload limit per call; 10 k rows of
# telemetry data is well under that ceiling and keeps memory bounded for
# high-frequency tables (car_data, positions, locations).
_INSERT_CHUNK_SIZE = 10_000


# -------------------------------------------------------
# Internal helpers
# -------------------------------------------------------

def _parse_dt(s):
    """Parse an ISO 8601 string to a UTC-aware datetime. Returns None for None."""
    if s is None:
        return None
    return pendulum.parse(s).in_tz('UTC').to_datetime_string()


def _to_gap_str(v):
    """
    Normalise a gap/interval value to string or None.
    The OpenF1 API returns floats (1.234), strings ('+1 LAP', '+2 LAPS'),
    or null. All are stored as STRING to preserve lapped-car info.
    """
    if v is None:
        return None
    return str(v)


def _coerce_segments(arr):
    """
    Replace None elements in a segment colour array with 0.
    BigQuery ARRAY<INT64> cannot contain NULL values. The OpenF1 API
    returns null for mini-sectors that were not recorded.
    """
    if not arr:
        return []
    return [v if v is not None else 0 for v in arr]


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


def _table_id(table_name):
    """Return a fully-qualified BigQuery table ID: project.dataset.table."""
    return f"{GCP_PROJECT}.{DATASET}.{table_name}"


def _insert(bq_client, table_name, rows):
    """
    Append rows to a BigQuery bronze table using a load job (NDJSON).

    Uses load_table_from_json (not streaming inserts) so that the call is
    covered by the BigQuery free tier. Chunks at _INSERT_CHUNK_SIZE to stay
    under the ~10 MB per-call payload limit.

    rows: list[dict] — keys must match column names in the table schema.
    The 'interval' column in bronze.intervals is stored under the key
    'interval' in the dict; backtick quoting is only needed in SQL, not here.
    """
    if not rows:
        logger.info("No rows to insert into %s", table_name)
        return

    tid = _table_id(table_name)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    try: 
        total = 0
        for start in range(0, len(rows), _INSERT_CHUNK_SIZE):
            chunk = rows[start:start + _INSERT_CHUNK_SIZE]
            job = bq_client.load_table_from_json(chunk, tid, job_config=job_config)
            job.result()  # blocks until the load job completes
            total += len(chunk)
            logger.info(
                "Inserted rows %d–%d (%d rows) into %s",
                start, start + len(chunk) - 1, len(chunk), table_name,
            )

        logger.info("Total: %d rows inserted into %s", total, table_name)
    except Exception as e:
        logger.error(f"Exception when inserting data: \n\t {e}")
        # logger.error(f"chunk: {str(chunk)}")
        raise

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
    logger.info("_load_meetings: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, f"{year}/meetings/")
    if not objects:
        logger.warning("No meeting files found for year=%r", year)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                if meeting_key and str(r['meeting_key']) != str(meeting_key):
                    continue
                rows.append({
                    'meeting_key':           r['meeting_key'],
                    'year':                  r['year'],
                    'circuit_key':           r.get('circuit_key') or 0,
                    'circuit_short_name':    r.get('circuit_short_name'),
                    'country_code':          r.get('country_code'),
                    'country_key':           r.get('country_key') or 0,
                    'country_name':          r.get('country_name'),
                    'date_start':            _parse_dt(r.get('date_start')),
                    'gmt_offset':            r.get('gmt_offset'),
                    'location':              r.get('location'),
                    'meeting_name':          r.get('meeting_name'),
                    'meeting_official_name': r.get('meeting_official_name'),
                    '_source_file':          obj,
                    '_loaded_at':            loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_meetings: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'meetings', rows)


def _load_sessions(year, meeting_key=None):
    logger.info("_load_sessions: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'sessions', meeting_key))
    if not objects:
        logger.warning("No session files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                date_end = _parse_dt(r.get('date_end'))
                if date_end is None:
                    logger.warning(
                        "_load_sessions: skipping session %s in %s — date_end is null",
                        r.get('session_key'), obj,
                    )
                    continue
                rows.append({
                    'session_key':        r['session_key'],
                    'meeting_key':        r['meeting_key'],
                    'year':               r['year'],
                    'circuit_key':        r.get('circuit_key') or 0,
                    'circuit_short_name': r.get('circuit_short_name'),
                    'country_code':       r.get('country_code'),
                    'country_key':        r.get('country_key') or 0,
                    'country_name':       r.get('country_name'),
                    'date_start':         _parse_dt(r.get('date_start')),
                    'date_end':           date_end,
                    'gmt_offset':         r.get('gmt_offset'),
                    'location':           r.get('location'),
                    'session_name':       r.get('session_name'),
                    'session_type':       r.get('session_type'),
                    '_source_file':       obj,
                    '_loaded_at':         loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_sessions: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'sessions', rows)


def _load_drivers(year, meeting_key=None):
    """year is injected — the OpenF1 drivers endpoint does not return it."""
    logger.info("_load_drivers: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'drivers', meeting_key))
    if not objects:
        logger.warning("No driver files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'driver_number':  r['driver_number'],
                    'meeting_key':    r['meeting_key'],
                    'session_key':    r['session_key'],
                    'year':           int(year),
                    'broadcast_name': r.get('broadcast_name'),
                    'country_code':   r.get('country_code'),  # null for some drivers
                    'first_name':     r.get('first_name'),
                    'last_name':      r.get('last_name'),
                    'full_name':      r.get('full_name'),
                    'name_acronym':   r.get('name_acronym'),
                    'team_name':      r.get('team_name'),
                    'team_colour':    r.get('team_colour'),
                    'headshot_url':   r.get('headshot_url'),
                    '_source_file':   obj,
                    '_loaded_at':     loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_drivers: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'drivers', rows)


def _load_pits(year, meeting_key=None):
    """year is injected — not present in the OpenF1 pit endpoint response."""
    logger.info("_load_pits: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'pits', meeting_key))
    if not objects:
        logger.warning("No pit files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r['driver_number'],
                    'year':          int(year),
                    'lap_number':    r.get('lap_number') or 0,  # nullable in API; 0 sentinel
                    'date':          _parse_dt(r.get('date')),
                    'pit_duration':  r.get('pit_duration'),      # nullable — None for entry-only records
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_pits: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'pits', rows)


def _load_stints(year, meeting_key=None):
    """year is injected — not present in the OpenF1 stints endpoint response."""
    logger.info("_load_stints: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'stints', meeting_key))
    if not objects:
        logger.warning("No stint files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':       r.get('meeting_key') or 0,
                    'session_key':       r.get('session_key') or 0,
                    'driver_number':     r.get('driver_number') or 0,  # null → 0 sentinel
                    'year':              int(year),
                    'stint_number':      r.get('stint_number') or 0,
                    'lap_start':         r.get('lap_start') or 0,
                    'lap_end':           r.get('lap_end'),              # nullable — None when ongoing
                    'compound':          r.get('compound'),
                    'tyre_age_at_start': r.get('tyre_age_at_start') or 0,
                    '_source_file':      obj,
                    '_loaded_at':        loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_stints: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'stints', rows)


def _load_race_control(year, meeting_key=None):
    """year is injected. Several fields are nullable per the schema."""
    logger.info("_load_race_control: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'race_control', meeting_key))
    if not objects:
        logger.warning("No race_control files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'category':      r.get('category'),
                    'flag':          r.get('flag'),           # nullable — None for non-flag messages
                    'scope':         r.get('scope'),
                    'sector':        r.get('sector'),         # nullable INT64
                    'lap_number':    r.get('lap_number'),     # nullable INT64
                    'driver_number': r.get('driver_number') or 0,  # 0 = track-wide event
                    'message':       r.get('message'),
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_race_control: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'race_control', rows)


def _load_team_radio(year, meeting_key=None):
    """year is injected."""
    logger.info("_load_team_radio: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'team_radio', meeting_key))
    if not objects:
        logger.warning("No team_radio files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r.get('driver_number') or 0,
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'recording_url': r.get('recording_url'),
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_team_radio: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'team_radio', rows)


# -------------------------------------------------------
# Session+driver-level loaders (one file per driver per session)
# -------------------------------------------------------

def _load_laps(year, meeting_key=None):
    """
    year is injected. Nullable fields: lap_duration, sector durations,
    trap speeds (incomplete laps).
    segments_sector_* are ARRAY<INT64> — null elements coerced to 0 since
    BigQuery arrays cannot contain NULL values.
    """
    logger.info("_load_laps: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'laps', meeting_key))
    if not objects:
        logger.warning("No lap files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':       r['meeting_key'],
                    'session_key':       r['session_key'],
                    'driver_number':     r.get('driver_number') or 0,
                    'year':              int(year),
                    'lap_number':        r.get('lap_number') or 0,
                    'date_start':        _parse_dt(r.get('date_start')),   # nullable — aborted laps
                    'lap_duration':      r.get('lap_duration'),            # nullable
                    'duration_sector_1': r.get('duration_sector_1'),       # nullable
                    'duration_sector_2': r.get('duration_sector_2'),       # nullable
                    'duration_sector_3': r.get('duration_sector_3'),       # nullable
                    'i1_speed':          r.get('i1_speed'),                # nullable
                    'i2_speed':          r.get('i2_speed'),                # nullable
                    'st_speed':          r.get('st_speed'),                # nullable
                    'is_pit_out_lap':    bool(r.get('is_pit_out_lap', False)),
                    'segments_sector_1': _coerce_segments(r.get('segments_sector_1')),
                    'segments_sector_2': _coerce_segments(r.get('segments_sector_2')),
                    'segments_sector_3': _coerce_segments(r.get('segments_sector_3')),
                    '_source_file':      obj,
                    '_loaded_at':        loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_laps: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'laps', rows)


def _load_positions(year, meeting_key=None):
    """
    year is injected. High-frequency time-series (~3.7 Hz).
    Rows are reset per file to prevent OOM accumulation across a full race weekend.
    """
    logger.info("_load_positions: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'positions', meeting_key))
    if not objects:
        logger.warning("No position files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    for obj in objects:
        rows = []  # reset per file — accumulating all files at once causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r['driver_number'],
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'position':      r['position'],
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_positions: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise
        _insert(bq, 'positions', rows)


def _load_intervals(year, meeting_key=None):
    """
    year is injected. gap_to_leader and interval are both nullable STRING.
    The dict key 'interval' matches the BigQuery column name — no quoting
    needed in dicts (backtick quoting is only required in SQL).
    """
    logger.info("_load_intervals: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'intervals', meeting_key))
    if not objects:
        logger.warning("No interval files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    rows = []
    for obj in objects:
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r['driver_number'],
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'gap_to_leader': _to_gap_str(r.get('gap_to_leader')),  # preserves '+1 LAP'
                    'interval':      _to_gap_str(r.get('interval')),       # reserved in SQL, fine in dict
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_intervals: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise

    _insert(bq, 'intervals', rows)


def _load_car_data(year, meeting_key=None):
    """
    year is injected. High-frequency telemetry (~3.7 Hz).
    Rows are reset per file to prevent OOM accumulation across a full race weekend.
    All sensor fields are bounded integers.
    """
    logger.info("_load_car_data: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'car_data', meeting_key))
    if not objects:
        logger.warning("No car_data files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    for obj in objects:
        rows = []  # reset per file — car_data is ~3.7Hz telemetry; accumulating all files causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r.get('driver_number') or 0,
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'rpm':           r.get('rpm') or 0,
                    'speed':         r.get('speed') or 0,
                    'n_gear':        r.get('n_gear') or 0,
                    'throttle':      r.get('throttle') or 0,
                    'brake':         r.get('brake') or 0,
                    'drs':           r.get('drs') or 0,
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_car_data: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise
        _insert(bq, 'car_data', rows)


def _load_locations(year, meeting_key=None):
    """
    year is injected. High-frequency GPS (~3.7 Hz).
    x/y/z are circuit-relative coordinates in decimetres (can be negative).
    Rows are reset per file to prevent OOM accumulation across a full race weekend.
    """
    logger.info("_load_locations: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'locations', meeting_key))
    if not objects:
        logger.warning("No location files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    for obj in objects:
        rows = []  # reset per file — locations is ~3.7Hz GPS; accumulating all files causes OOM
        data = _read_json(minio, obj)
        for i, r in enumerate(data):
            try:
                rows.append({
                    'meeting_key':   r['meeting_key'],
                    'session_key':   r['session_key'],
                    'driver_number': r.get('driver_number') or 0,
                    'year':          int(year),
                    'date':          _parse_dt(r.get('date')),
                    'x':             r.get('x') or 0,
                    'y':             r.get('y') or 0,
                    'z':             r.get('z') or 0,
                    '_source_file':  obj,
                    '_loaded_at':    loaded_at,
                })
            except Exception as e:
                logger.error(
                    "_load_locations: error on row %d of %s — %s | row=%r",
                    i, obj, e, r, exc_info=True,
                )
                raise
        _insert(bq, 'locations', rows)
