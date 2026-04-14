# prefire-imagery-ca

This repo combines two concerns:

1. **ETL pipeline** — converts MrSID imagery to Cloud-Optimized GeoTIFFs, extracts metadata, and loads everything into S3.
2. **COG-serving Lambda** — an AWS CDK stack that deploys a Dockerized Lambda + API Gateway to serve COG subsets to clients.

---

## ETL Pipeline

### Usage

```bash
python -m src.main [convert|load|extract|all]
```

| Command   | Description                                                  |
| --------- | ------------------------------------------------------------ |
| `convert` | SID → GeoTIFF → COG → validate                               |
| `load`    | Upload COGs to S3 → extract metadata → upload metadata to S3 |
| `extract` | Build metadata / STAC / CSV for local COGs (standalone)      |
| `all`     | Run `convert` then `load` (default)                          |

### Pipeline Flow

```
convert ──→ load
              ├─ [1] Upload COGs to S3
              ├─ [2] Extract metadata (using S3 URIs)
              └─ [3] Upload metadata to S3
```

The load step calls extract internally after COGs are uploaded, so the extracted metadata contains the correct S3 URIs and ETags.

The extract step checks S3 for an existing `summary.csv`. If found it downloads and appends to it; otherwise it creates a new one.

### Environment Variables

All variables are loaded from a `.env` file via `python-dotenv`.

| Variable             | Required By            | Description                                                        |
| -------------------- | ---------------------- | ------------------------------------------------------------------ |
| `SID_DIRECTORY`      | convert                | Directory containing `.sid` source files                           |
| `GEOTIFF_DIRECTORY`  | convert                | Output directory for GeoTIFF files                                 |
| `COG_DIRECTORY`      | convert, load, extract | Directory for COG files (output of convert, input to load/extract) |
| `COUNTY_LIST`        | convert                | Comma-separated list of counties to process                        |
| `BUCKET_NAME`        | load, extract          | Target S3 bucket name                                              |
| `STAC_COLLECTION`    | load, extract          | STAC collection ID used in metadata                                |
| `METADATA_DIRECTORY` | load, extract          | Local directory for metadata output (JSON, STAC, CSV)              |

---

## COG-Serving Lambda

The `lambda/` directory contains a CDK app that deploys a Docker-based Lambda and API Gateway for serving COG subsets.

### API

**POST** `/{env}/`

Request body (JSON):

```json
{
    "fips": "081",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[-122.42, 37.77], "..."]]
    }
}
```

The geometry must be in WGS84 (EPSG:4326). The handler reprojects it to EPSG:26910, applies a 50 m buffer, extracts the intersecting COG window, writes the subset to the output bucket, and returns a presigned S3 URL (15-minute TTL by default).

### CDK Deploy

```bash
cd lambda
cdk deploy -c env=dev   # or env=prod
```

This creates:

- `prefire-{env}-cog` — existing read-only COG source bucket (imported by name)
- `prefire-{env}-output` — output bucket with a 1-day lifecycle rule for subset COGs
- A Docker Lambda (`CogHandler`) with 512 MB memory / 30 s timeout
- An API Gateway (`CogApi`) with CORS enabled

### Lambda Environment Variables

| Variable             | Default    | Description                             |
| -------------------- | ---------- | --------------------------------------- |
| `COG_BUCKET`         | set by CDK | Source COG bucket name                  |
| `OUTPUT_BUCKET`      | set by CDK | Output bucket for subset COGs           |
| `ENV`                | set by CDK | Deployment environment (`dev` / `prod`) |
| `URL_EXPIRY_SECONDS` | `900`      | Presigned URL TTL in seconds            |

---

## Project Structure

```
src/                         # ETL pipeline
├── main.py                  # Entry point, argument parsing
├── convert/
│   ├── convert.py           # Convert orchestrator
│   ├── sid_to_geotiff.py    # MrSID → GeoTIFF (mrsidgeodecode)
│   ├── geotiff_to_cog.py    # GeoTIFF → COG (GDAL)
│   └── validate_cogs.py     # Validate COG files (rio cogeo)
├── extract/
│   ├── extract.py           # Extract orchestrator
│   ├── create_metadata.py   # Build metadata dict, write JSON/CSV
│   ├── create_stac.py       # Build STAC item from metadata
│   ├── extract_cog_metadata.py    # COG-specific metadata (GDAL)
│   ├── extract_raster_metadata.py # Raster/spatial metadata (GDAL)
│   └── metadata_templates/
│       └── template.json    # Metadata JSON template
├── load/
│   ├── load.py              # Load orchestrator
│   ├── cog_to_s3.py         # Upload COGs to S3
│   ├── metadata_to_s3.py    # Upload metadata to S3
│   └── utils/
│       └── upload_to_s3.py  # Generic S3 upload helper
└── shared/
    ├── validate_env.py      # Environment variable validation
    └── print_progress_bar.py # CLI progress bar

lambda/                      # COG-serving Lambda (CDK app)
├── app.py                   # CDK entry point
├── api.py                   # Lambda handler
├── county_polygons.json     # County boundary shapes (FIPS → geometry)
├── Dockerfile               # Lambda container image
├── cdk.json                 # CDK config
├── stacks/
│   └── prefire_stack.py     # CDK stack definition
└── scripts/
    └── fetch_county_polygons.py  # Script to refresh county polygons
```
