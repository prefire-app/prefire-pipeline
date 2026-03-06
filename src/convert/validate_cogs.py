def validate_cogs(COG_DIRECTORY):
    import subprocess
    import glob
    import os

    cogs = glob.glob(os.path.join(COG_DIRECTORY, "*.cog"))
    total = len(cogs)
    print(f"Validating {total} COG files...")
    for i, filename in enumerate(cogs, start=1):
        try:
            result = subprocess.run(["rio", "cogeo", "validate", filename], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if "is a valid cloud optimized GeoTIFF" in result.stdout:
                print(f"{filename} is a valid COG.")
            elif "is NOT a valid cloud optimized GeoTIFF" in result.stdout:
                raise ValueError(f"{filename} is NOT a valid COG.")
        except Exception as e:
            print(f"Error validating {filename}: {e}")


