import json
import os
import sys

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ["COG_BUCKET"] = "prefire-dev-cog"
os.environ["OUTPUT_BUCKET"] = "prefire-dev-output"
os.environ["FHSZ_BUCKET"] = "prefire-dev-data"
os.environ["FHSZ_COG_KEY"] = "fhsz/fhsz_cog.cog"

from api import handler

# Small polygon in San Mateo county (WGS84)
event = {
    "body": json.dumps({
        "fips": "081",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-122.42, 37.55], [-122.42, 37.551], [-122.419, 37.551], [-122.419, 37.55], [-122.42, 37.55]]]
        }
    })
}

result = handler(event, {})
print("Status:", result["statusCode"])

if result["statusCode"] == 200:
    body = json.loads(result["body"])
    print("S3 key:       ", body["key"])
    print("Expires in:   ", body["expires_in"], "seconds")
    print("Presigned URL:", body["url"])

    response = requests.get(body["url"])
    response.raise_for_status()

    out_path = os.path.join(os.path.dirname(__file__), "output.tif")
    with open(out_path, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {len(response.content):,} bytes → {out_path}")
else:
    print("Error:", result["body"])

print()
print("--- FHSZ endpoint tests ---")

# Test 1: missing params
result = handler({"path": "/fhsz", "httpMethod": "GET", "queryStringParameters": {}}, {})
assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
print("PASS  missing lat/lon → 400")

# Test 2: coordinates outside California
result = handler({
    "path": "/fhsz", "httpMethod": "GET",
    "queryStringParameters": {"lat": "51.5", "lon": "-0.12"}
}, {})
assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
print("PASS  London coordinates → 400 (outside CA bounds)")

# Test 3: non-numeric params
result = handler({
    "path": "/fhsz", "httpMethod": "GET",
    "queryStringParameters": {"lat": "abc", "lon": "-122.0"}
}, {})
assert result["statusCode"] == 400, f"Expected 400, got {result['statusCode']}"
print("PASS  non-numeric lat → 400")

# Test 4: valid coordinate in Sonoma wildland (expected: a zone or null depending on COG)
result = handler({
    "path": "/fhsz", "httpMethod": "GET",
    "queryStringParameters": {"lat": "39.1940", "lon": "-121.0037"}
}, {})
assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}: {result['body']}"
body = json.loads(result["body"])
assert "zone" in body, f"Response missing 'zone' key: {body}"
print(f"PASS  Nevada CO wildland (39.1940, -121.0037) → zone={body['zone']}") 

# Test 5: coordinate in urban San Jose (Santa Clara county, expected: null)
result = handler({
    "path": "/fhsz", "httpMethod": "GET",
    "queryStringParameters": {"lat": "37.338", "lon": "-121.886"}
}, {})
assert result["statusCode"] == 200, f"Expected 200, got {result['statusCode']}: {result['body']}"
body = json.loads(result["body"])
assert "zone" in body
print(f"PASS  Downtown San Jose (37.338, -121.886) → zone={body['zone']}")

print()
print("All FHSZ local tests passed.")
