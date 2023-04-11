
# %% --------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------
# import forestatrisk as far
# %%
# import rasterio
import rioxarray as rxr
import geopandas as gpd
# import xarray as xr
# import numpy as np
from collections import Counter
import pandas as pd
import os
# %%
# path = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\forestatrisk_data\fcc_2050_AFR_aea.tif"
region = "ASI"
# path = r"C:\Users\JackWindow\Documents\forestatrisk_data\fcc_2050_ASI_aea.tif"
def path_gen(year, region):
    if year == 2020:
        return r"C:\Users\JackWindow\Documents\forestatrisk_data\prob_2020_{}_aea.tif".format(region)
    else:
        return r"C:\Users\JackWindow\Documents\forestatrisk_data\fcc_{}_{}_aea.tif".format(year, region)

scale = 65535
years = [2020, 2030, 2040, 2050]
paths_dict = {year: path_gen(year, region) for year in years}
rasters_dict = {year: rxr.open_rasterio(path, chunks=True) for year, path in paths_dict.items()}
# raster = rxr.open_rasterio(path, chunks=True)
# path_2020 = r"C:\Users\JackWindow\Documents\forestatrisk_data\prob_2020_ASI_aea.tif"
# large_raster = rxr.open_rasterio(path_2020, chunks=True)
# %% --------------------------------------------------------------------------
# Utlities
# -----------------------------------------------------------------------------
def countpx(raster, val=None):
    """Count the number of pixels in a raster"""
    if val is None:
        return raster.size
    else:
        return Counter(raster.sel(band=1).values.flat)[val]
    
def area_deforested(raster, units="ha"):
    """Calculate the area of deforestation in a raster"""
    if units == "ha":
        return countpx(raster, 1) * 900 / 10000
    elif units == "m^2":
        return countpx(raster, 1) * 900 

def destname(country_gdf, region, year):
    country_name = country_gdf["NAME"].values[0]
    adm0_iso = country_gdf["ADM0_ISO"].values[0]
    duplicate_ISOs = len(continent_gdf.loc[continent_gdf["ADM0_ISO"]==adm0_iso]) > 1
    if duplicate_ISOs:
        alias = f"{adm0_iso}_{name_formatter(country_gdf['NAME'].values[0])}"
    else:
        alias = adm0_iso
    if year == 2020:
        return f"{region}/{country_name}/deforestation_probability_{year}_{alias}.tif"
    else:
        return f"{region}/{country_name}/predicted_fcc_{year}_{alias}.tif"
    # alias = name_formatter(country_gdf["ABBREV"].values[0])
    # if year == 2020:
    #     return f"{region}/{country_name}/deforestation_probability_{year}_{alias}.tif"
    # else:
    #     return f"{region}/{country_name}/predicted_fcc_{year}_{alias}.tif"

def name_formatter(abbreviated_name_str):
    return "_".join([n.split(".")[0] for n in abbreviated_name_str.split(" ")])

# %% --------------------------------------------------------------------------
# Country boundaries
# -----------------------------------------------------------------------------
ne_path = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\shapefiles\ne_10m_admin_0_countries\ne_10m_admin_0_countries.shp"
ne_bounds_gdf = gpd.read_file(ne_path)
aea_crs = rasters_dict[2020].rio.crs
continent_bounds = rasters_dict[2020].rio.bounds()
ne_bounds_gdf = ne_bounds_gdf.to_crs(aea_crs)
continent_gdf = gpd.clip(ne_bounds_gdf, continent_bounds)

continent_summary = pd.DataFrame.from_dict(
    {
        "Country": [],
        "ADM0_ISO": [],
        "2030 FCC (ha) ": [],
        "2030 Emissions (MgCO2)": [],
        "2040 FCC (ha)": [],
        "2040 Emissions (MgCO2)": [],
        "2050 FCC (ha)": [],
        "2050 Emissions (MgCO2)": []
    }
)

agb_path = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\rasters\Global_C_Density_Maps\Global_Maps_C_Density_2010_1763\data\aboveground_biomass_carbon_2010.tif"
agb = rxr.open_rasterio(agb_path, chunks=True)

# %% --------------------------------------------------------------------------
# Clip to country
# -----------------------------------------------------------------------------
def process_country(country, continent_summary):
    # country_name = "Cambodia"
    country_name = country["NAME"]
    country = continent_gdf[continent_gdf['NAME'] == country_name]
    clipped_rasters = {year: raster.rio.clip(country.geometry.values, from_disk=True) for year, raster in rasters_dict.items()}
    for year, raster in clipped_rasters.items():
        raster.rio.to_raster(destname(country, region, year))

    # clipped_pres = large_raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # clipped_fut = raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # future_fcc = area_deforested(clipped_fut)  # in hectares
    deforested_areas = {year: area_deforested(raster) for year, raster in clipped_rasters.items()}

    # --------------------------------------------------------------------------
    # Clip biomass
    # -----------------------------------------------------------------------------
    agb_clip = agb.rio.clip(country.to_crs(agb.rio.crs).geometry.values, from_disk=True)
    agb_clip = agb_clip.rio.reproject(dst_crs=aea_crs, nodata=0)

    # --------------------------------------------------------------------------
    # Make mask of deforested pixels
    # -----------------------------------------------------------------------------
    # mask = clipped_fut.rio.reproject_match(agb_clip, nodata=0)
    masks = {year: raster.rio.reproject_match(agb_clip, nodata=0) for year, raster in clipped_rasters.items() if year != 2020}
    del clipped_rasters
    masked_agb = {year: agb_clip * mask for year, mask in masks.items()}
    del masks
    # agb_masked = agb_clip * mask
    # MgC/ha * CO2/C * ha = MgCO2
    emissions = {year: agb_masked.sum().values[0] * 44/12 * deforested_areas[year] for year, agb_masked in masked_agb.items()}
    del masked_agb
    # emissions = agb_masked.sum() * 44/12 * future_fcc  

    # --------------------------------------------------------------------------
    # Save country data
    # -----------------------------------------------------------------------------
    if not os.path.exists(f"{region}/{country_name}"):
        os.makedirs(f"{region}/{country_name}")

    country_summary = pd.DataFrame.from_dict(
        {
            "Country": [country_name],
            "ADM0_ISO": country["ADM0_A3"].values,
            "2030 FCC (ha) ": [deforested_areas[2030]],
            "2030 Emissions (MgCO2)": [emissions[2030]],
            "2040 FCC (ha)": [deforested_areas[2040]],
            "2040 Emissions (MgCO2)": [emissions[2040]],
            "2050 FCC (ha)": [deforested_areas[2050]],
            "2050 Emissions (MgCO2)": [emissions[2050]],
        }
    )
    country_summary.to_csv(f"{region}/{country_name}/{name_formatter(country['NAME'].values[0])}_summary.csv", index=False)
    continent_summary = pd.concat([continent_summary, country_summary])
    return continent_summary


## Area of deforested pixels for each country
# future rasters are projected forest cover change (binary)
# each pixel is 900m2

# For each country use future raster to calculate predicted deforested area
# Save the clipped raster of the country
# Save area deforested of all countries in a dataframe
# From present day identify area of pixels most at risk
# Save clipped deforestation probability
# Figure out emissions - biomass layer or far emissions tool
# Just use agb layer with conversion=0.47 (default far params)

# crashed during indian island territories
# check emissions dicts are correct
# figure out what's taking up so much memory

# %%
