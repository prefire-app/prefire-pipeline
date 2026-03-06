from dotenv import load_dotenv
import os
import sys

from src.convert.sid_to_geotiff import sid_to_geotiff
from src.convert.geotiff_to_cog import geotiff_to_cog
from src.convert.validate_cogs import validate_cogs
from src.shared.validate_env import validate_env

def main():
    load_dotenv()
    if not validate_env():
        return
    
    
    

    print("Running the convert pipeline...")
    if (sys.argv and len(sys.argv) > 1 and "sid_to_geotiff" in sys.argv):
        try:
            print("Converting SID files to GeoTIFF...")
            sid_to_geotiff(os.getenv("SID_DIRECTORY"), os.getenv("GEOTIFF_DIRECTORY"), os.getenv("COUNTY_LIST"))
        except Exception as e:
            print(f"Error converting SID to GeoTIFF: {e}")
    if (sys.argv and len(sys.argv) > 1 and "geotiff_to_cog" in sys.argv):
        try:
            print("Converting GeoTIFF files to COG...")
            geotiff_to_cog(os.getenv("GEOTIFF_DIRECTORY"), os.getenv("COG_DIRECTORY"))
        except Exception as e:
            print(f"Error converting GeoTIFF to COG: {e}")
        
    if (sys.argv and len(sys.argv) > 1 and "validate_cogs" in sys.argv):
        try:
            print("Validating COG files...")
            validate_cogs(os.getenv("COG_DIRECTORY"))
        except Exception as e:
            print(f"Error validating COGs: {e}")

if __name__ == "__main__":
    main()