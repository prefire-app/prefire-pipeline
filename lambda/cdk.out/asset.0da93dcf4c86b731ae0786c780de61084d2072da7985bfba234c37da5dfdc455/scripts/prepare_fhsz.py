"""
Preprocessing script: split CALFIRE FHSZ shapefile by county and upload to S3.

Usage:
    python prepare_fhsz.py --shapefile /path/to/fhsz.shp [--env dev] [--prefix fhsz] [--simplify 0.0001]

For each county in county_polygons.json the script:
  1. Reads the FHSZ shapefile, reprojects to WGS84 (EPSG:4326).
  2. Loads county_polygons.json (EPSG:26910) and reprojects to WGS84.
  3. Spatial-joins FHSZ polygons to counties (intersects predicate).
  4. Uploads one compact GeoJSON per county:
       s3://prefire-{env}-data/{prefix}/{fips}.geojson

Each county file is typically 1-5 MB, avoiding the need to load the full
statewide dataset (~150 MB) into Lambda memory.

Requirements (local only, not in Lambda):
    pip install geopandas boto3
"""

import argparse
import json
import sys
from pathlib import Path

import boto3
import geopandas as gpd
from shapely.geometry import shape


BUCKET_TMPL = "prefire-{env}-data"
DEFAULT_PREFIX = "fhsz"
COUNTY_JSON = Path(__file__).parent.parent / "county_polygons.json"


def _load_county_gdf() -> gpd.GeoDataFrame:
    """Read county_polygons.json (EPSG:26910) as a GeoDataFrame in EPSG:4326."""
    if not COUNTY_JSON.exists():
        print(f"ERROR: county_polygons.json not found at {COUNTY_JSON}")
        sys.exit(1)
    data = json.loads(COUNTY_JSON.read_text())
    rows = [{"fips": feat["fips"], "geometry": shape(feat["geometry"])} for feat in data]
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:26910")
    return gdf.to_crs("EPSG:4326")


def prepare(shapefile: str, env: str, prefix: str, simplify_tolerance: float | None) -> None:
    print(f"Loading county polygons from {COUNTY_JSON} ...")
    counties_gdf = _load_county_gdf()
    print(f"  {len(counties_gdf)} counties: {sorted(counties_gdf['fips'].tolist())}")

    print(f"Reading FHSZ shapefile: {shapefile}")
    fhsz_gdf = gpd.read_file(shapefile)
    print(f"  {len(fhsz_gdf)} features, CRS: {fhsz_gdf.crs}")

    if "FHSZ_Descr" not in fhsz_gdf.columns:
        print(f"  ERROR: 'FHSZ_Descr' column not found. Available columns: {list(fhsz_gdf.columns)}")
        sys.exit(1)

    # Reproject FHSZ to WGS84
    if fhsz_gdf.crs is None:
        print("  WARNING: shapefile has no CRS; assuming EPSG:4326")
    elif fhsz_gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting FHSZ from {fhsz_gdf.crs} to EPSG:4326 ...")
        fhsz_gdf = fhsz_gdf.to_crs("EPSG:4326")

    fhsz_gdf = fhsz_gdf[["FHSZ_Descr", "geometry"]].copy()
    fhsz_gdf = fhsz_gdf[~fhsz_gdf.geometry.is_empty & ~fhsz_gdf.geometry.isna()]

    if simplify_tolerance:
        print(f"  Simplifying geometries with tolerance={simplify_tolerance} ...")
        fhsz_gdf["geometry"] = fhsz_gdf["geometry"].simplify(simplify_tolerance, preserve_topology=True)

    print("  Spatial joining FHSZ polygons to counties ...")
    joined = gpd.sjoin(
        fhsz_gdf,
        counties_gdf[["fips", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    s3 = boto3.client("s3")
    bucket = BUCKET_TMPL.format(env=env)
    total_uploaded = 0

    for fips, group in joined.groupby("fips"):
        county_fhsz = group[["FHSZ_Descr", "geometry"]].drop_duplicates()
        geojson_bytes = county_fhsz.to_json().encode("utf-8")
        size_kb = len(geojson_bytes) / 1024
        key = f"{prefix}/{fips}.geojson"
        print(f"  County {fips}: {len(county_fhsz):>4} features, {size_kb:>7.1f} KB → s3://{bucket}/{key}")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=geojson_bytes,
            ContentType="application/geo+json",
        )
        total_uploaded += 1

    print(f"\nDone. Uploaded {total_uploaded} county files to s3://{bucket}/{prefix}/")


def main():
    parser = argparse.ArgumentParser(
        description="Split FHSZ shapefile by county and upload per-county GeoJSONs to S3."
    )
    parser.add_argument("--shapefile", required=True, help="Path to the FHSZ .shp file")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"], help="Deployment environment (default: dev)")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"S3 key prefix (default: {DEFAULT_PREFIX})")
    parser.add_argument(
        "--simplify",
        type=float,
        default=None,
        metavar="TOLERANCE",
        help="Geometry simplification tolerance in degrees (e.g. 0.0001). Omit for full precision.",
    )
    args = parser.parse_args()
    prepare(args.shapefile, args.env, args.prefix, args.simplify)


if __name__ == "__main__":
    main()
