"""
BigQuery bronze load functions.

Each function reads raw JSON from MinIO and inserts rows into the
corresponding bronze.* table via BigQuery load jobs (NDJSON, WRITE_APPEND).
These are intentionally decoupled from the API ingestion tasks — MinIO is
the source of truth; BigQuery is a downstream consumer with its own failure
boundary.

Deduplication strategy — content-hash idempotency (ADR-005):
  For every MinIO file:
    1. Read raw bytes and compute SHA-256
    2. Look up the stored hash in bronze.load_audit
       - No record  → first load; proceed
       - Hash match → file unchanged; skip entirely
       - Hash diff  → source was updated; DELETE existing rows then reload
    3. Record the hash in bronze.load_audit AFTER a successful insert
  This prevents duplicates on DAG retries and detects source corrections.

year is always passed explicitly. For tables where the OpenF1 API does
not include year in the response, year is injected from the call site.

meeting_key is optional. When provided, only that meeting is loaded.
When omitted, all meetings for the year are loaded (full-year backfill).

Special case — meetings:
  The meetings file ({year}/meetings/meetings.json) contains all Grand Prix
  events for the year. When meeting_key is specified the audit key becomes
  "{source_file}#meeting={meeting_key}" so that each (file, meeting)
  combination has its own idempotency record and different meetings can be
  loaded independently from the same source file.
"""

import hashlib
import json

import pendulum
from google.cloud import bigquery

from helpers.bigquery import GCP_PROJECT, get_bigquery_client
from helpers.logger import logger
from helpers.minio import get_minio_client

BUCKET = 'formula1'
DATASET = 'bronze'
_AUDIT_TABLE = 'load_audit'

# BigQuery load job chunk size.
# load_table_from_json has a ~10 MB payload limit per call; 10k rows of
# telemetry data is well under that ceiling and keeps memory bounded for
# high-frequency tables (car_data, positions, locations).
_INSERT_CHUNK_SIZE = 10_000


# -------------------------------------------------------
# Internal helpers — I/O and hashing
# -------------------------------------------------------

def _read_bytes(minio_client, object_name):
    """Download raw bytes for a MinIO object. Returns bytes (possibly empty)."""
    response = minio_client.get_object(BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _parse_json(raw, object_name):
    """
    Parse raw bytes as JSON. Returns a list.
    Non-list payloads (API error dicts, empty files) are logged and return [].
    """
    if not raw:
        logger.warning("_parse_json: %s is empty, skipping", object_name)
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        logger.warning(
            "_parse_json: %s contains %s instead of a list (value=%r), skipping",
            object_name, type(data).__name__, data,
        )
        return []
    logger.info("_parse_json: %s — %d rows", object_name, len(data))
    return data


def _hash_file(raw):
    """Return the SHA-256 hex digest of raw bytes."""
    return hashlib.sha256(raw).hexdigest()


# -------------------------------------------------------
# Internal helpers — BigQuery audit table
# -------------------------------------------------------

def _table_id(table_name):
    """Return a fully-qualified BigQuery table ID: project.dataset.table."""
    return f"{GCP_PROJECT}.{DATASET}.{table_name}"


def _get_stored_hash(bq_client, audit_key):
    """
    Look up the stored content hash for an audit key in bronze.load_audit.
    Returns the hash string, or None if this file has never been loaded.
    """
    query = f"""
        SELECT content_hash
        FROM `{_table_id(_AUDIT_TABLE)}`
        WHERE source_file = @source_file
        LIMIT 1
    """
    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('source_file', 'STRING', audit_key),
        ]
    ))
    row = next(iter(job.result()), None)
    return row['content_hash'] if row else None


def _delete_source_file(bq_client, table_name, source_file):
    """Delete all rows for a given _source_file from a bronze table."""
    query = f"""
        DELETE FROM `{_table_id(table_name)}`
        WHERE _source_file = @source_file
    """
    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('source_file', 'STRING', source_file),
        ]
    ))
    job.result()
    logger.info("Deleted rows for _source_file=%r from %s", source_file, table_name)


def _delete_meeting_rows(bq_client, source_file, meeting_key):
    """
    Delete rows for a specific meeting from bronze.meetings.
    Used when the meetings file covers multiple Grand Prix events and only
    one meeting is being reloaded.
    """
    query = f"""
        DELETE FROM `{_table_id('meetings')}`
        WHERE _source_file = @source_file
          AND meeting_key   = @meeting_key
    """
    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('source_file', 'STRING', source_file),
            bigquery.ScalarQueryParameter('meeting_key', 'INT64', int(meeting_key)),
        ]
    ))
    job.result()
    logger.info("Deleted rows for meeting_key=%s from meetings", meeting_key)


def _record_hash(bq_client, audit_key, table_name, content_hash, row_count):
    """
    Upsert a content hash record into bronze.load_audit.
    Called AFTER a successful insert so that a failed load leaves no record
    and the next run will retry the file cleanly.
    """
    query = f"""
        MERGE `{_table_id(_AUDIT_TABLE)}` AS target
        USING (SELECT @source_file AS source_file) AS source
        ON target.source_file = source.source_file
        WHEN MATCHED THEN
            UPDATE SET
                content_hash = @content_hash,
                table_name   = @table_name,
                row_count    = @row_count,
                loaded_at    = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (source_file, content_hash, table_name, row_count, loaded_at)
            VALUES (@source_file, @content_hash, @table_name, @row_count, CURRENT_TIMESTAMP())
    """
    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('source_file', 'STRING', audit_key),
            bigquery.ScalarQueryParameter('content_hash', 'STRING', content_hash),
            bigquery.ScalarQueryParameter('table_name', 'STRING', table_name),
            bigquery.ScalarQueryParameter('row_count', 'INT64', row_count),
        ]
    ))
    job.result()
    logger.info("Recorded hash for audit_key=%r → %s", audit_key, content_hash[:12])


# -------------------------------------------------------
# Internal helpers — data transformation
# -------------------------------------------------------

def _parse_dt(s):
    """Parse an ISO 8601 string to a UTC datetime string. Returns None for None."""
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


# -------------------------------------------------------
# Internal helpers — BigQuery insert
# -------------------------------------------------------

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
        logger.error("Exception when inserting data into %s: %s", table_name, e)
        raise


def _list_objects(minio_client, prefix):
    """Return all object names under a MinIO prefix."""
    return [
        obj.object_name
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
    ]


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

    Audit key is scoped to meeting_key when filtering so that each
    (file, meeting) pair has its own idempotency record. This allows
    meeting_key=1262 and meeting_key=1263 to be loaded independently
    from the same source file without one skipping the other.
    """
    logger.info("_load_meetings: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, f"{year}/meetings/")
    if not objects:
        logger.warning("No meeting files found for year=%r", year)
        return

    for obj in objects:
        # Scope audit key to meeting_key when filtering so each meeting
        # gets its own idempotency record within the shared source file.
        audit_key = f"{obj}#meeting={meeting_key}" if meeting_key else obj

        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, audit_key)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", audit_key)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", audit_key)
            if meeting_key:
                _delete_meeting_rows(bq, obj, meeting_key)
            else:
                _delete_source_file(bq, 'meetings', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, audit_key, 'meetings', current_hash, len(rows))


def _load_sessions(year, meeting_key=None):
    logger.info("_load_sessions: year=%r meeting_key=%r", year, meeting_key)
    minio = get_minio_client()
    bq = get_bigquery_client()
    loaded_at = pendulum.now('UTC').to_datetime_string()

    objects = _list_objects(minio, _prefix(year, 'sessions', meeting_key))
    if not objects:
        logger.warning("No session files found for year=%r, meeting_key=%r", year, meeting_key)
        return

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'sessions', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'sessions', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'drivers', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'drivers', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'pits', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'pits', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'stints', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'stints', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'race_control', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'race_control', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'team_radio', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'team_radio', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'laps', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'laps', current_hash, len(rows))


def _load_positions(year, meeting_key=None):
    """
    year is injected. High-frequency time-series (~3.7 Hz).
    Processed per-file to prevent OOM on full race weekend accumulation.
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
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'positions', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'positions', current_hash, len(rows))


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

    for obj in objects:
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'intervals', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'intervals', current_hash, len(rows))


def _load_car_data(year, meeting_key=None):
    """
    year is injected. High-frequency telemetry (~3.7 Hz).
    Processed per-file to prevent OOM on full race weekend accumulation.
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
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'car_data', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'car_data', current_hash, len(rows))


def _load_locations(year, meeting_key=None):
    """
    year is injected. High-frequency GPS (~3.7 Hz).
    x/y/z are circuit-relative coordinates in decimetres (can be negative).
    Processed per-file to prevent OOM on full race weekend accumulation.
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
        raw = _read_bytes(minio, obj)
        current_hash = _hash_file(raw)
        stored_hash = _get_stored_hash(bq, obj)

        if stored_hash == current_hash:
            logger.info("Skipping %s — content unchanged (hash match)", obj)
            continue

        if stored_hash is not None:
            logger.info("Content changed for %s — deleting existing rows", obj)
            _delete_source_file(bq, 'locations', obj)

        data = _parse_json(raw, obj)
        rows = []
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
        _record_hash(bq, obj, 'locations', current_hash, len(rows))
