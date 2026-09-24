import csv
import io
import itertools
import os
import zipfile
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values
from prefect import flow, get_run_logger, task

WA_DRILLHOLES_URL = (
    "https://public-services.slip.wa.gov.au/public/rest/services/"
    "SLIP_Public_Services/Industry_and_Mining/MapServer/28/query"
)
MRDS_CSV_URL = "https://mrdata.usgs.gov/mrds/mrds-csv.zip"


def _db_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "db"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "app"),
        user=os.environ.get("POSTGRES_USER", "app"),
        password=os.environ.get("POSTGRES_PASSWORD", "app"),
    )


def _esri_epoch_to_date(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date()


def _point_wkt(lon, lat):
    if lon is None or lat is None:
        return None
    return f"SRID=4326;POINT({lon} {lat})"


@task(retries=3, retry_delay_seconds=10)
def fetch_wa_drillholes(limit: int) -> list[dict]:
    """Pull a sample of collars from the WA DMIRS open-file company
    drillhole database (DMIRS-046), via its public ArcGIS REST layer."""
    logger = get_run_logger()
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "json",
        "resultRecordCount": limit,
        "orderByFields": "objectid",
    }
    response = requests.get(WA_DRILLHOLES_URL, params=params, timeout=60)
    response.raise_for_status()
    features = response.json().get("features", [])
    logger.info(f"Fetched {len(features)} WA drillhole records")
    return features


@task
def load_wa_drillholes(features: list[dict]) -> int:
    rows = []
    for feature in features:
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry") or {}
        collarid = attrs.get("collarid")
        if collarid is None:
            continue
        lon = geometry.get("x", attrs.get("longitude"))
        lat = geometry.get("y", attrs.get("latitude"))
        rows.append(
            (
                collarid,
                attrs.get("holeid"),
                lon,
                lat,
                _point_wkt(lon, lat),
                attrs.get("target_commodity"),
                attrs.get("maxdepth"),
                attrs.get("operator"),
                attrs.get("project"),
                attrs.get("holetype"),
                attrs.get("anumber"),
                _esri_epoch_to_date(attrs.get("period_from")),
                _esri_epoch_to_date(attrs.get("period_to")),
                _esri_epoch_to_date(attrs.get("extract_date")),
            )
        )

    if not rows:
        return 0

    with _db_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO drillholes_wadrillhole (
                collarid, hole_id, longitude, latitude, location,
                target_commodity, max_depth, operator, project, hole_type,
                anumber, period_from, period_to, extract_date
            ) VALUES %s
            ON CONFLICT (collarid) DO UPDATE SET
                hole_id = EXCLUDED.hole_id,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                location = EXCLUDED.location,
                target_commodity = EXCLUDED.target_commodity,
                max_depth = EXCLUDED.max_depth,
                operator = EXCLUDED.operator,
                project = EXCLUDED.project,
                hole_type = EXCLUDED.hole_type,
                anumber = EXCLUDED.anumber,
                period_from = EXCLUDED.period_from,
                period_to = EXCLUDED.period_to,
                extract_date = EXCLUDED.extract_date
            """,
            rows,
            template="(%s, %s, %s, %s, ST_GeogFromText(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
    return len(rows)


@task(retries=3, retry_delay_seconds=10)
def fetch_mrds_sites(limit: int) -> list[dict]:
    """Download the USGS MRDS CSV export and take a sample of records."""
    logger = get_run_logger()
    response = requests.get(MRDS_CSV_URL, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        with archive.open("mrds.csv") as raw:
            text = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            reader = csv.DictReader(text)
            rows = list(itertools.islice(reader, limit))

    logger.info(f"Fetched {len(rows)} MRDS records")
    return rows


@task
def load_mrds_sites(rows: list[dict]) -> int:
    def _float(value):
        try:
            return float(value) if value not in (None, "") else None
        except ValueError:
            return None

    parsed = []
    for row in rows:
        dep_id = row.get("dep_id")
        if not dep_id:
            continue
        lon = _float(row.get("longitude"))
        lat = _float(row.get("latitude"))
        parsed.append(
            (
                dep_id,
                row.get("site_name"),
                lon,
                lat,
                _point_wkt(lon, lat),
                row.get("country"),
                row.get("state"),
                row.get("commod1") or None,
                row.get("dev_stat") or None,
                row.get("dep_type") or None,
            )
        )

    if not parsed:
        return 0

    with _db_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO drillholes_mrdssite (
                dep_id, site_name, longitude, latitude, location,
                country, state, commodity, development_status, deposit_type
            ) VALUES %s
            ON CONFLICT (dep_id) DO UPDATE SET
                site_name = EXCLUDED.site_name,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                location = EXCLUDED.location,
                country = EXCLUDED.country,
                state = EXCLUDED.state,
                commodity = EXCLUDED.commodity,
                development_status = EXCLUDED.development_status,
                deposit_type = EXCLUDED.deposit_type
            """,
            parsed,
            template="(%s, %s, %s, %s, ST_GeogFromText(%s), %s, %s, %s, %s, %s)",
        )
    return len(parsed)


@flow(name="drillhole-ingest")
def drillhole_ingest(wa_limit: int = 2000, mrds_limit: int = 2000):
    """Ingest a sample of WA DMIRS company drillholes (primary source) and
    USGS MRDS mineral occurrences (secondary source) into Postgres/PostGIS."""
    logger = get_run_logger()

    wa_features = fetch_wa_drillholes(wa_limit)
    wa_loaded = load_wa_drillholes(wa_features)
    logger.info(f"Loaded {wa_loaded} WA drillholes")

    mrds_rows = fetch_mrds_sites(mrds_limit)
    mrds_loaded = load_mrds_sites(mrds_rows)
    logger.info(f"Loaded {mrds_loaded} MRDS sites")


if __name__ == "__main__":
    drillhole_ingest()
