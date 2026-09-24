"""Lands each drillhole source's raw response bytes untouched (no parsing,
no transformation) to local storage or S3, for later reprocessing.

This is separate from drillhole_ingest.py, which parses and loads a sample
of each source into Postgres/PostGIS.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from prefect import flow, get_run_logger, task

WA_DRILLHOLES_QUERY_URL = (
    "https://public-services.slip.wa.gov.au/public/rest/services/"
    "SLIP_Public_Services/Industry_and_Mining/MapServer/28/query"
)
WA_PAGE_SIZE = 10000  # layer's maxRecordCount, confirmed via .../28?f=json
MRDS_CSV_URL = "https://mrdata.usgs.gov/mrds/mrds-csv.zip"

RAW_STORAGE_BACKEND = os.environ.get("RAW_STORAGE_BACKEND", "local")
RAW_LOCAL_DIR = os.environ.get("RAW_LOCAL_DIR", "/app/data/raw")
RAW_S3_BUCKET = os.environ.get("RAW_S3_BUCKET")
RAW_S3_PREFIX = os.environ.get("RAW_S3_PREFIX", "raw")


def _run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@task
def save_raw(source: str, run_stamp: str, filename: str, content: bytes) -> str:
    """Persist raw bytes exactly as received - no parsing, no reshaping."""
    logger = get_run_logger()
    key = f"{source}/{run_stamp}/{filename}"

    if RAW_STORAGE_BACKEND == "s3":
        import boto3

        if not RAW_S3_BUCKET:
            raise RuntimeError("RAW_S3_BUCKET must be set when RAW_STORAGE_BACKEND=s3")
        s3_key = f"{RAW_S3_PREFIX}/{key}"
        boto3.client("s3").put_object(Bucket=RAW_S3_BUCKET, Key=s3_key, Body=content)
        uri = f"s3://{RAW_S3_BUCKET}/{s3_key}"
    else:
        path = Path(RAW_LOCAL_DIR) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        uri = str(path)

    logger.info(f"Saved {len(content)} raw bytes to {uri}")
    return uri


@task(retries=3, retry_delay_seconds=10)
def download_wa_drillholes_raw(run_stamp: str, max_records: int | None) -> list[str]:
    """Page through the WA DMIRS ArcGIS REST query at its maxRecordCount
    (10000) and save each page's raw JSON response body untouched.

    The full layer currently holds ~3.5M records, so a full landing is a
    large, slow pull - pass max_records to cap it for testing."""
    logger = get_run_logger()
    uris = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "json",
            "resultRecordCount": WA_PAGE_SIZE,
            "resultOffset": offset,
            "orderByFields": "objectid",
        }
        response = requests.get(WA_DRILLHOLES_QUERY_URL, params=params, timeout=120)
        response.raise_for_status()
        content = response.content
        uris.append(save_raw("wa_dmirs", run_stamp, f"page_{offset:08d}.json", content))

        payload = response.json()
        features = payload.get("features", [])
        logger.info(f"WA DMIRS page at offset {offset}: {len(features)} features")

        offset += len(features)
        if not features or not payload.get("exceededTransferLimit"):
            break
        if max_records is not None and offset >= max_records:
            break

    return uris


@task(retries=3, retry_delay_seconds=10)
def download_mrds_raw(run_stamp: str) -> str:
    """Download the USGS MRDS CSV zip export untouched."""
    logger = get_run_logger()
    response = requests.get(MRDS_CSV_URL, timeout=120)
    response.raise_for_status()
    logger.info(f"Downloaded {len(response.content)} raw bytes from MRDS")
    return save_raw("usgs_mrds", run_stamp, "mrds-csv.zip", response.content)


@flow(name="drillhole-raw-landing")
def drillhole_raw_landing(wa_max_records: int | None = None):
    """Download each source's raw data untouched to RAW_STORAGE_BACKEND
    (local filesystem by default, or S3 when RAW_STORAGE_BACKEND=s3)."""
    logger = get_run_logger()
    run_stamp = _run_stamp()
    logger.info(f"Landing raw sources for run {run_stamp} via {RAW_STORAGE_BACKEND} backend")

    wa_uris = download_wa_drillholes_raw(run_stamp, wa_max_records)
    logger.info(f"Landed {len(wa_uris)} WA DMIRS raw page(s)")

    mrds_uri = download_mrds_raw(run_stamp)
    logger.info(f"Landed MRDS raw file at {mrds_uri}")


if __name__ == "__main__":
    drillhole_raw_landing()
