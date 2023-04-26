"""
prediction_comaprison.py

Comparison of incumbent forest cover change prediction rasters and those
provided by the forestatrisk package from CIRAD.
"""

__date__ = "2023-04-19"
__author__ = "Jack J Window"
__email__ = "jack.window@trove-research.com"

# %% --------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------
import numpy as np
import rasterio
import xarray as xr
import rioxarray as rxr
import matplotlib.pyplot as plt
# from matplotlib.colors import LogNorm
import tqdm
import dask
import os
from io import BytesIO
import shapely as shp
import geopandas as gpd
import fiona


# %% --------------------------------------------------------------------------
# path setups
# -----------------------------------------------------------------------------
# Incumbent fcc maps
trove_2020 = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Forecasting\Global_geospatial_analysis\REDD\defor_bau\def_bau_km2_2020.tif"
trove_2030 = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Forecasting\Global_geospatial_analysis\REDD\defor_bau\def_bau_km2_2030.tif"
trove_2040 = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Forecasting\Global_geospatial_analysis\REDD\defor_bau\def_bau_km2_2040.tif"
trove_2050 = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Forecasting\Global_geospatial_analysis\REDD\defor_bau\def_bau_km2_2050.tif"
trove_fcc_paths = [trove_2020, trove_2030, trove_2040, trove_2050]

# Example country: Philippines
# forestatrisk fcc maps
far_2020 = r"C:\Users\JackWindow\OneDrive - Trove Research Ltd\Documents\forest-at-risk\ASI\Philippines\deforestation_probability_2020_PHL.tif"
far_2030 = r"C:\Users\JackWindow\OneDrive - Trove Research Ltd\Documents\forest-at-risk\ASI\Philippines\predicted_fcc_2030_PHL.tif"
far_2040 = r"C:\Users\JackWindow\OneDrive - Trove Research Ltd\Documents\forest-at-risk\ASI\Philippines\predicted_fcc_2040_PHL.tif"
far_2050 = r"C:\Users\JackWindow\OneDrive - Trove Research Ltd\Documents\forest-at-risk\ASI\Philippines\predicted_fcc_2050_PHL.tif"
far_paths = {"2020": far_2020, "2030": far_2030, "2040": far_2040, "2050": far_2050}

# %% --------------------------------------------------------------------------
# Example country bounds
# -----------------------------------------------------------------------------
# Country boundaries
ne_path = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\shapefiles\ne_10m_admin_0_countries\ne_10m_admin_0_countries.shp"
ne_bounds_gdf = gpd.read_file(ne_path) 
# Isolate country shapefile
phl_gdf = ne_bounds_gdf.loc[ne_bounds_gdf["NAME"] == "Philippines"]

aea_crs = rasterio.crs.CRS.from_wkt('PROJCS["unknown",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["latitude_of_center",-15],PARAMETER["longitude_of_center",125],PARAMETER["standard_parallel_1",7],PARAMETER["standard_parallel_2",-32],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]')

# %% --------------------------------------------------------------------------
# Cropping trove datasets
# -----------------------------------------------------------------------------
# Open trove fcc datasets
trove_fcc_maps = {str(year): rxr.open_rasterio(trove_path, chunks=True) for year, trove_path in zip([2020, 2030, 2040, 2050], trove_fcc_paths)}
# Crop to Philippines bounds
trove_fcc_maps = {
    year: trove_fcc_map.rio.clip_box(
    *phl_gdf.bounds.values[0], crs="epsg:4326"
    ).rio.clip(
        phl_gdf.geometry.values
    ) for year, trove_fcc_map in trove_fcc_maps.items()
}

# %% --------------------------------------------------------------------------
# Open forestatrisk datasets
# -----------------------------------------------------------------------------
# Open dataset and set crs
def open_far(year):
    far_ds = rxr.open_rasterio(far_paths[year], chunks=True)
    # Convert binary to deforested area in km^2
    far_ds = xr.where(far_ds==255, 0, far_ds) * 30**2 / 1e4
    far_ds = far_ds.rio.write_crs(aea_crs)

    # far_2030_ds = far_2030_ds.rio.reproject(dst_crs=phl_gdf.crs, nodata=0)
    # far_2030_ds = xr.where(far_2030_ds==255, 0, far_2030_ds)
    # far_2030_ds = rxr.open_rasterio(far_2030)
    # far_2030_ds = xr.where(far_2030_ds==255, 0, far_2030_ds)
    # far_2030_ds = far_2030_ds.rio.write_crs(aea_crs)

    # Resample from 30m -> 5500m
    w = int(np.floor(far_ds.rio.width / 183.3))
    h = int(np.floor(far_ds.rio.height / 183.3))

    # Reproject to wgs84 and resample
    # far_ds = far_ds.rio.reproject(
    #     phl_gdf.crs,
    #     shape=(h, w),
    #     resampling=rasterio.enums.Resampling.average,
    #     nodata=0
    # ) * 183.3**2 # avg value of px * number of pxs is sum of unsampled raster
    far_ds = far_ds.rio.reproject_match(
        trove_fcc_maps[year],
        resampling=rasterio.enums.Resampling.average
    )
    return far_ds

# far_2030_ds = open_far("2030")
# far_2050_ds = open_far("2050")
# far_diff_50_30 = far_2050_ds - far_2030_ds
# far_2030_ds_r = far_2030_ds_r.rio.reproject(
#     dst_crs = phl_gdf.crs,
#     nodata=0
# )
# These maps currently have nodata=-999., and actual values 0-7: is this deforested area?
# Need to convert far maps to this format

# forestatrisk maps are forest cover change from 2020, so incremental forest cover changes need to be calculated

# Trove is deforestation rate at given year
# To calculate deforested area, find deforestation rates for each year from linear interpoaltion
# Then add these (km2/yr) to get deforested area cumulatively.
# Alternatively, the far deforestation rate seems to give forest cover at each year, from which deforestation rate 
# can be calculated by taking the difference between each decade and dividing it by 10.
# 
# %% --------------------------------------------------------------------------
# {1:Enter description for cell}
# -----------------------------------------------------------------------------
trove_2030_ds = trove_fcc_maps[2030].rio.clip(
    phl_gdf.geometry.values
)
trove_2030_ds = xr.where(trove_2030_ds==-999, 0, trove_2030_ds)

trove_2050_ds = trove_fcc_maps[2050].rio.clip(
    phl_gdf.geometry.values
)
trove_2050_ds = xr.where(trove_2050_ds==-999, 0, trove_2050_ds)

# Coordinates aren't exact, need to realign
trove_2030_ds.coords["x"] = trove_2050_ds.x.values
trove_2030_ds.coords["y"] = trove_2050_ds.y.values

diff_50_30 = trove_2050_ds - trove_2030_ds
# %%
