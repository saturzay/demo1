# Data Contract: Drillhole & Mineral Occurrence Data

## Status
Draft. Covers the two tables populated by the `drillhole-ingest` Prefect flow
(`prefect/flows/drillhole_ingest.py`).

## Owner
- **Producer**: `drillhole-ingest` Prefect flow (runs on the `default-pool` worker)
- **Consumer**: `drillholes` Django app (models, admin, and any downstream queries)

---

## Table: `drillholes_wadrillhole`

**Source**: WA DMIRS open-file company drillhole database (DMIRS-046), via the
SLIP public ArcGIS REST service.

- URL: `https://public-services.slip.wa.gov.au/public/rest/services/SLIP_Public_Services/Industry_and_Mining/MapServer/28/query`
- License: WA Government open data (SLIP public services) — no auth/API key required
- Access pattern: ArcGIS REST `query` endpoint, paged via `resultRecordCount`
- Refresh cadence: on-demand flow run (no schedule currently deployed); source itself is
  updated periodically by DMIRS, not real-time
- Natural key: `collarid` (upstream, unique per collar)

| Column | Type | Nullable | Source field | Notes |
|---|---|---|---|---|
| `id` | bigint | no | — | internal PK, auto-generated |
| `collarid` | integer | no | `attributes.collarid` | unique constraint; upsert key |
| `hole_id` | varchar(100) | yes | `attributes.holeid` | operator-assigned hole name |
| `location` | geography(Point,4326) | yes | `geometry.x/y` | derived via `ST_GeogFromText`; prefer over raw lat/lon for spatial queries |
| `latitude` / `longitude` | double precision | yes | `geometry.x/y`, falls back to `attributes.longitude/latitude` | WGS84 (SRID 4326) |
| `target_commodity` | varchar(255) | yes | `attributes.target_commodity` | free text, not a controlled vocabulary upstream |
| `max_depth` | double precision | yes | `attributes.maxdepth` | metres |
| `operator` | varchar(255) | yes | `attributes.operator` | company name, free text |
| `project` | varchar(255) | yes | `attributes.project` | free text |
| `hole_type` | varchar(50) | yes | `attributes.holetype` | e.g. `RAB`, `RC`, `DD` |
| `anumber` | integer | yes | `attributes.anumber` | DMIRS report/authorization number |
| `period_from` / `period_to` | date | yes | `attributes.period_from/period_to` | Esri epoch ms → date; drilling period |
| `extract_date` | date | yes | `attributes.extract_date` | date DMIRS extracted the record, **not** a load timestamp |

**Invariants**
- `collarid` is unique; loads are idempotent upserts (`ON CONFLICT (collarid) DO UPDATE`).
- Rows with a null `collarid` are dropped during load (see `load_wa_drillholes`).
- `location` is derived at load time; a row can have non-null lat/lon without a valid
  `location` only if both coordinates are null.

**Known limitations**
- The flow currently pulls a bounded sample (`wa_limit`, default 2000), not the full
  dataset. Do not assume completeness for analysis.
- `target_commodity`, `operator`, `project`, `hole_type` are free text from the upstream
  source — no enum/lookup enforcement.

---

## Table: `drillholes_mrdssite`

**Source**: USGS Mineral Resources Data System (MRDS), CSV export.

- URL: `https://mrdata.usgs.gov/mrds/mrds-csv.zip`
- License: US Government public domain data, no auth/API key required
- Access pattern: full zip download (~26 MB), parsed with `csv.DictReader`, sampled
- Refresh cadence: on-demand flow run; USGS updates the export periodically (not versioned/dated in the file itself)
- Natural key: `dep_id` (upstream deposit ID)

| Column | Type | Nullable | Source field | Notes |
|---|---|---|---|---|
| `id` | bigint | no | — | internal PK, auto-generated |
| `dep_id` | varchar(50) | no | `dep_id` | unique constraint; upsert key |
| `site_name` | varchar(255) | yes | `site_name` | |
| `location` | geography(Point,4326) | yes | `longitude`/`latitude` | derived via `ST_GeogFromText` |
| `latitude` / `longitude` | double precision | yes | `latitude`/`longitude` | WGS84; global coverage, not WA-only |
| `country` | varchar(100) | yes | `country` | |
| `state` | varchar(100) | yes | `state` | free text, may be blank outside the US |
| `commodity` | varchar(255) | yes | `commod1` | primary commodity only (`commod2`/`commod3` not loaded) |
| `development_status` | varchar(100) | yes | `dev_stat` | |
| `deposit_type` | varchar(255) | yes | `dep_type` | |

**Invariants**
- `dep_id` is unique; loads are idempotent upserts (`ON CONFLICT (dep_id) DO UPDATE`).
- Rows with a missing/blank `dep_id` are dropped during load.
- Lat/lon fields that fail `float()` parsing are stored as null rather than raising.

**Known limitations**
- The flow pulls a bounded sample (`mrds_limit`, default 2000) taken from the top of the
  CSV, not a representative or WA-filtered subset — this is a global dataset.
- Only `commod1` is captured; secondary/tertiary commodities are discarded.
- No `extract_date`/version field exists in the source, so there's no way to tell how
  stale a given MRDS row is relative to the live USGS dataset.

---

## Change management
- Schema changes to either table must be reflected in `drillholes/models.py`, a Django
  migration, and this document in the same change.
- If upstream field names or URLs change, update the constants at the top of
  `prefect/flows/drillhole_ingest.py` and re-verify with a manual flow run before merging.
