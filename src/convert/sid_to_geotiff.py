import glob
import os
import re
import subprocess

from src.shared.print_progress_bar import print_progress_bar

def _extract_county(filename):
    basename = os.path.splitext(os.path.basename(filename))[0]
    parts = basename.split("_")
    for i, part in enumerate(parts):
        if re.match(r'^[a-z]{2}\d{3}$', part):
            return "_".join(parts[:i])
    return parts[0]

def sid_to_geotiff(sid_directory, geotiff_directory, county_list):
    all_sid_files = glob.glob(os.path.join(sid_directory, "*.sid"))
    sid_files = [f for f in all_sid_files if _extract_county(f) in county_list]
    total = len(sid_files)
    print_progress_bar(0, total, prefix='Progress:', suffix='Complete', length=50)
    for i, filename in enumerate(sid_files, start=1):
        print(f"\nProcessing {filename}...")
        geotiff_filename = os.path.join(geotiff_directory, os.path.basename(filename).replace(".sid", ".tif"))
        subprocess.run(["mrsidgeodecode", "-i", filename, "-o", geotiff_filename])
        print(f"Converted {filename} to {geotiff_filename}")
        print_progress_bar(i, total, prefix='Progress:', suffix='Complete', length=50)

    print("All files processed (MrSID -> GeoTIFF).")