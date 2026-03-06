"""extract_raster_metadata.py

Opens a COG with GDAL and extracts core raster/spatial metadata:
  - CRS (EPSG, WKT)
  - geotransform, width, height
  - bands, dtype, nodata, colorinterp
  - pixel size + units
  - bbox and footprint in EPSG:4326
"""

from __future__ import annotations

from osgeo import gdal, osr

gdal.UseExceptions()


def extract_raster_metadata(cog_path: str) -> dict:
    ds = gdal.Open(cog_path, gdal.GA_ReadOnly)
    if ds is None:
        raise IOError(f"GDAL could not open: {cog_path}")

    gt = ds.GetGeoTransform()          # (x_origin, x_res, rot_x, y_origin, rot_y, y_res)
    width = ds.RasterXSize
    height = ds.RasterYSize
    wkt = ds.GetProjection()

    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    srs.AutoIdentifyEPSG()
    epsg_code = srs.GetAuthorityCode(None)
    epsg = int(epsg_code) if epsg_code else None

    if srs.IsProjected():
        units = srs.GetLinearUnitsName()
    elif srs.IsGeographic():
        units = srs.GetAngularUnitsName()
    else:
        units = None

    pixel_size_x = abs(gt[1])
    pixel_size_y = abs(gt[5])

    bands = ds.RasterCount
    band1 = ds.GetRasterBand(1)
    dtype = gdal.GetDataTypeName(band1.DataType)
    nodata = band1.GetNoDataValue()
    colorinterp = [
        gdal.GetColorInterpretationName(ds.GetRasterBand(i).GetColorInterpretation())
        for i in range(1, bands + 1)
    ]

    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)
    wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    coord_transform = osr.CoordinateTransformation(srs, wgs84)

    corners_native = [
        (gt[0],                          gt[3]),                            # UL
        (gt[0] + width * gt[1],          gt[3] + width * gt[2]),            # UR
        (gt[0] + width * gt[1] + height * gt[2],
         gt[3] + width * gt[4] + height * gt[5]),                           # LR
        (gt[0] + height * gt[2],         gt[3] + height * gt[5]),           # LL
    ]
    corners_wgs84 = [
        coord_transform.TransformPoint(x, y)[:2] for x, y in corners_native
    ]
    lons = [c[0] for c in corners_wgs84]
    lats = [c[1] for c in corners_wgs84]

    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    bbox = [min_lon, min_lat, max_lon, max_lat]

    footprint = {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, max_lat],
            [max_lon, max_lat],
            [max_lon, min_lat],
            [min_lon, min_lat],
            [min_lon, max_lat],
        ]],
    }

    ds = None  # close dataset

    return {
        "bbox": bbox,
        "footprint": footprint,
        "crs": {
            "epsg": epsg,
            "wkt": wkt,
        },
        "width": width,
        "height": height,
        "pixel_size": {
            "x": pixel_size_x,
            "y": pixel_size_y,
            "units": units,
        },
        "transform": list(gt),
        "bands": bands,
        "dtype": dtype,
        "nodata": nodata,
        "colorinterp": colorinterp,
    }
