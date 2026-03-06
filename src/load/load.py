import os
from dotenv import load_dotenv

from src.load.cog_to_s3 import cog_to_s3
from src.shared.validate_env import validate_env
import src.extract.extract as extract_pipeline
# Adding this hard coded as I ran upload separately and don't want to re-upload just to get the URIs in place for the extract step. In a real run, this would be returned by cog_to_s3 and passed to extract_pipeline.main()
COG_URIS = {
    "alameda_ca001_2024_1.cog": {
        "s3_uri": "s3://prefire-dev-cog/cogs/alameda_ca001_2024_1.cog",
        "etag": "653fce47f9b633e9f22a0ae271cd8618-6028",   
    },
    "los_angeles_ca037_2024_1_no_channel_islands.cog": {
        "s3_uri": "s3://prefire-dev-cog/cogs/los_angeles_ca037_2024_1_no_channel_islands.cog",
        "etag": "e1406527184c180d164ccca19221e0a3-6143",
    },
    "san_mateo_ca081_2024_1.cog": {
        "s3_uri": "s3://prefire-dev-cog/cogs/san_mateo_ca081_2024_1.cog",
        "etag": "44e42ab8c86b8c87a7ac4768818dac90-4165",
    },
    "santa_clara_ca085_2024_1.cog": {
        "s3_uri": "s3://prefire-dev-cog/cogs/santa_clara_ca085_2024_1.cog",
        "etag": "5bb5ffe4ca2d97558ef6e8e252477794-9317",
    }
}

def main() -> None:
    print("Starting load process...")
    load_dotenv()
    if not validate_env():
        return

    bucket   = os.getenv("BUCKET_NAME", "")
    cog_dir  = os.getenv("COG_DIRECTORY", "")
    prefix   = "cogs"
    collection = os.getenv("STAC_COLLECTION") 

    print("\n--- Step 1: Upload COGs to S3 ---")
    # cog_uris = cog_to_s3(cog_dir, bucket, prefix)


    print("\n--- Step 2: Extract metadata ---")
    extract_pipeline.main(cog_uris=COG_URIS, collection=collection)

    print("\n--- Step 3: Load metadata to S3 (not yet implemented) ---")

    print("\nLoad process complete.")


def _parse_args(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Prefire pipeline: load step")
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["load_cog_to_s3", "extract", "all"],
        help="What to run (default: all)",
    )
    return parser.parse_args(argv)


def cli(argv=None) -> None:
    args = _parse_args(argv)
    load_dotenv()
    if not validate_env():
        return
    if args.command == "load_cog_to_s3":
        bucket  = os.getenv("BUCKET_NAME", "")
        cog_dir = os.getenv("COG_DIRECTORY", "")
        cog_to_s3(cog_dir, bucket, "cogs")
    elif args.command == "extract":
        collection = os.getenv("STAC_COLLECTION") 
        extract_pipeline.main(cog_uris=COG_URIS, collection=collection)
    else:   # all
        main()


if __name__ == "__main__":
    cli()
