import json
import math
import os
import uuid
from pathlib import Path

import numpy as np
import boto3
import rasterio
from rasterio.io import MemoryFile
from rasterio.session import AWSSession
from rasterio.windows import WindowError, from_bounds, Window
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer

_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:26910", always_xy=True)
BUFFER_M = 50  # metres to buffer around the user-drawn polygon

_FHSZ_RADIUS_M = 200 * 0.3048  # 200 ft → 60.96 m
_FHSZ_ZONE_NAMES = {1: "Moderate", 2: "High", 3: "Very High"}

COG_BUCKET = os.environ["COG_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
URL_EXPIRY = int(os.environ.get("URL_EXPIRY_SECONDS", 900))  # 15 min default
FHSZ_BUCKET = os.environ.get("FHSZ_BUCKET", "")
FHSZ_COG_KEY = os.environ.get("FHSZ_COG_KEY", "fhsz/fhsz_cog.cog")

s3_client = boto3.client("s3")

_KEY_CACHE: list[str] | None = None
_COUNTY_SHAPES: dict[str, object] = {}


def _load_county_shapes():
    global _COUNTY_SHAPES
    if _COUNTY_SHAPES:
        return
    data = json.loads((Path(__file__).parent / "county_polygons.json").read_text())
    _COUNTY_SHAPES = {feat["fips"]: shape(feat["geometry"]) for feat in data}


def _geom_in_county(fips: str, utm_geom) -> bool:
    _load_county_shapes()
    county_geom = _COUNTY_SHAPES.get(fips)
    if county_geom is None:
        return True  # unknown county — let rasterio decide
    return county_geom.intersects(utm_geom)

def _all_keys() -> list[str]:
    global _KEY_CACHE
    if _KEY_CACHE is not None:
        return _KEY_CACHE
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=COG_BUCKET):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    _KEY_CACHE = keys
    return keys

def _find_key(fips: str) -> str | None:
    for key in _all_keys():
        if fips in key:
            return key
    return None

# California bounding box (WGS84) for input validation
_CA_LAT_MIN, _CA_LAT_MAX = 32.0, 42.5
_CA_LON_MIN, _CA_LON_MAX = -125.0, -113.5


def _fhsz_handler(event: dict) -> dict:
    params = event.get("queryStringParameters") or {}
    lat_str = params.get("lat")
    lon_str = params.get("lon")
    if not lat_str or not lon_str:
        return _err(400, "lat and lon query parameters are required")
    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except ValueError:
        return _err(400, "lat and lon must be valid numbers")
    if not (_CA_LAT_MIN <= lat <= _CA_LAT_MAX and _CA_LON_MIN <= lon <= _CA_LON_MAX):
        return _err(400, "coordinates are outside California bounds")
    if not FHSZ_BUCKET:
        return _err(500, "FHSZ_BUCKET is not configured")

    aws_session = AWSSession(boto3.Session())
    with rasterio.Env(aws_session, GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES", VSI_CACHE=True):
        with rasterio.open(f"s3://{FHSZ_BUCKET}/{FHSZ_COG_KEY}") as src:
            # Compute pixel radius for 200 ft: COG is WGS84, use y-res * 111320 m/deg
            meters_per_pixel = src.res[1] * 111320
            radius_px = max(1, math.ceil(_FHSZ_RADIUS_M / meters_per_pixel))
            row, col = src.index(lon, lat)
            window = Window(
                col_off=col - radius_px,
                row_off=row - radius_px,
                width=2 * radius_px + 1,
                height=2 * radius_px + 1,
            ).intersection(Window(0, 0, src.width, src.height))
            data = src.read(1, window=window)

    valid = data[data > 0]
    zone = _FHSZ_ZONE_NAMES.get(int(valid.max())) if len(valid) > 0 else None
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"zone": zone}),
    }


_CORS_HEADERS = {"Access-Control-Allow-Origin": "*"}

def _err(code: int, msg: str):
    return {"statusCode": code, "headers": _CORS_HEADERS, "body": json.dumps({"error": msg})}

'''
API Handler to serve COG subset presigned URLs.

Expects a POST with JSON body:
{
  "fips": "081",
  "geometry": { "type": "Polygon", "coordinates": [[[-122.42, 37.77], ...]] }
}

The geometry is in WGS84 (EPSG:4326). The handler reprojects it to EPSG:26910,
adds a 50 m buffer, computes the bounding box, extracts the COG subset, and
returns a presigned URL to the result.
'''
def handler(event, context):
    path = event.get("path", "")
    http_method = event.get("httpMethod", "POST")

    if path.endswith("/fhsz") and http_method == "GET":
        return _fhsz_handler(event)

    body_raw = event.get("body")
    if not body_raw:
        return _err(400, "Request body is required")
    if isinstance(body_raw, str):
        body = json.loads(body_raw)
    else:
        body = body_raw



    fips = body.get("fips")
    geom_json = body.get("geometry")
    if not fips or not geom_json:
        return _err(400, "fips and geometry are required")

    user_geom_wgs84 = shape(geom_json)
    if not user_geom_wgs84.is_valid:
        return _err(400, "Invalid geometry")

    if not _geom_in_county(fips, user_geom_wgs84):
        return _err(400, "geometry does not intersect the requested county")

    key = _find_key(fips)
    if not key:
        return _err(404, f"No COG found for FIPS {fips}")

    aws_session = AWSSession(boto3.Session())
    with rasterio.Env(aws_session, GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES", VSI_CACHE=True):
        with rasterio.open(f"s3://{COG_BUCKET}/{key}") as src:
            to_src_crs = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            user_geom_native = transform(to_src_crs.transform, user_geom_wgs84)
            buffered = user_geom_native.buffer(BUFFER_M)
            minx, miny, maxx, maxy = buffered.bounds
            window = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
            try:
                window = window.intersection(Window(0, 0, src.width, src.height))
            except WindowError:
                return _err(400, f"buffered polygon does not overlap COG extent {tuple(src.bounds)}")
            bands = list(range(1, min(src.count, 4) + 1))
            data = src.read(indexes=bands, window=window)
            nodata = src.nodata
            if nodata is not None:
                valid = np.any(data != nodata)
            else:
                valid = np.any(data != 0)
            if not valid:
                return _err(400, "region contains no valid data (all nodata/zero pixels)")
            profile = src.profile.copy()
            profile.update(
                driver="GTiff", count=len(bands),
                height=data.shape[1], width=data.shape[2],
                transform=src.window_transform(window), tiled=False
            )
            profile.pop("blockxsize", None)
            profile.pop("blockysize", None)
            with MemoryFile() as mem:
                with mem.open(**profile) as dst:
                    dst.write(data)
                output_key = f"subsets/{fips}/{uuid.uuid4()}.tif"
                s3_client.put_object(
                    Bucket=OUTPUT_BUCKET,
                    Key=output_key,
                    Body=mem.read(),
                    ContentType="image/tiff",
                )

    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": output_key},
        ExpiresIn=URL_EXPIRY,
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"url": url, "expires_in": URL_EXPIRY, "key": output_key}),
    }