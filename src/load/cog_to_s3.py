import glob
import os

import boto3

from src.load.utils.upload_to_s3 import upload_to_s3


def cog_to_s3(cog_directory: str, bucket_name: str, prefix: str) -> dict[str, dict]:
    s3_client = boto3.client("s3")
    cogs = sorted(glob.glob(os.path.join(cog_directory, "*.cog")))
    total = len(cogs)
    uri_map: dict[str, dict] = {}

    for i, cog in enumerate(cogs, 1):
        fname = os.path.basename(cog)
        object_name = f"{prefix}/{fname}"
        try:
            print(f"[{i}/{total}] Uploading {fname} → s3://{bucket_name}/{object_name}")
            upload_to_s3(cog, bucket_name, object_name)
            head = s3_client.head_object(Bucket=bucket_name, Key=object_name)
            uri_map[fname] = {
                "s3_uri": f"s3://{bucket_name}/{object_name}",
                "etag":   head.get("ETag", "").strip('"'),
            }
        except Exception as e:
            print(f"  ERROR uploading {fname}: {e}")

    return uri_map
