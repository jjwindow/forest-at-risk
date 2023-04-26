
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
import dask
from dask.diagnostics import ProgressBar
import tqdm
from glob import glob
import rasterio
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
    
def srs_to_gdf(row, crs):
    """
    Convert a row from a series to a geopandas dataframe. Used when iterating
    through a dataframe with the iterrows method. Allows for access to the 
    geometry attribute of the row.
    """
    row_gdf = gpd.GeoDataFrame(row.to_frame().transpose()).set_crs(crs)
    return row_gdf

def destname(country_gdf, region, year):
    country_name = country_gdf["ADMIN"].values[0]
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
ne_bounds_gdf = gpd.read_file(ne_path)  # EPSG 4326
aea_crs = rasters_dict[2020].rio.crs
continent_bounds = rasters_dict[2020].rio.bounds()

# %% --------------------------------------------------------------------------
# projection 1L: countries to aea
# -----------------------------------------------------------------------------
# import matplotlib.pyplot as plt
# import shapely as shp
# # Country boundaries to aea sa 
# ne_bounds_gdf = ne_bounds_gdf.to_crs(aea_crs)
# # Everything in same crs: clip
# continent_gdf = gpd.clip(ne_bounds_gdf, continent_bounds)
# # Dodgy country boudnaries
# continent_gdf.boundary.plot()

# %% --------------------------------------------------------------------------
# Projection 2: bounds to wgs84
# -----------------------------------------------------------------------------
# # Make gdf to reproject bounds
# bounds_gdf = gpd.GeoDataFrame(
#     pd.DataFrame.from_dict(
#         {
#             "Bounds": ["Asia"],
#             "geometry": [shp.box(*continent_bounds)]
#         }
#     ),
#     geometry="geometry"
# ).set_crs(aea_crs)

# Reproject to wgs84
# bounds_gdf = bounds_gdf.to_crs("EPSG:4326")
# continent_gdf = gpd.clip(ne_bounds_gdf, bounds_gdf)

# %% --------------------------------------------------------------------------
# {1:Enter description for cell}
# -----------------------------------------------------------------------------

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


# %% --------------------------------------------------------------------------
# Biomass
# -----------------------------------------------------------------------------

agb_path = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\rasters\Global_C_Density_Maps\Global_Maps_C_Density_2010_1763\data\aboveground_biomass_carbon_2010.tif"
agb_latlon = rxr.open_rasterio(agb_path, chunks=True)

env = rasterio.Env(
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
    CPL_VSIL_CURL_USE_HEAD=False,
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF",
)
with env:
    with rasterio.open(agb_path) as src:
        with rasterio.vrt.WarpedVRT(src, crs=aea_crs) as vrt:
            agb = rxr.open_rasterio(vrt, chunks=True)

# %% --------------------------------------------------------------------------
# Clip to country
# -----------------------------------------------------------------------------
def process_country(country, continent_summary):
    # country_name = "Cambodia"
    # breakpoint()
    country = srs_to_gdf(country, aea_crs)  # Convert to gdf for ability to reproject later
    country_name = country["ADMIN"].values[0]
    if os.path.exists(f"{region}/{country_name}") and len(glob(f"{region}/{country_name}/*")) > 0:
        print(f"Skipping {country_name}...")
        return continent_summary
    elif not os.path.exists(f"{region}/{country_name}"):
        os.makedirs(f"{region}/{country_name}")
    # country = continent_gdf[continent_gdf['NAME'] == country_name]
    # Clip fcc rasters to country, box-first.
    clipped_rasters = {
        year: raster.rio.clip_box(
            minx=country.bounds["minx"].values[0],
            miny=country.bounds["miny"].values[0],
            maxx=country.bounds["maxx"].values[0],
            maxy=country.bounds["maxy"].values[0]
        ).rio.clip(
            [country.geometry.values[0]], 
            from_disk=True
        ) for year, raster in rasters_dict.items()
    }
    # for year, raster in clipped_rasters.items():
        # raster.rio.to_raster(destname(country, region, year))
    jobs = [dask.delayed(raster.rio.to_raster)(destname(country, region, year), compute=False) for year, raster in clipped_rasters.items()]
    dask.compute(jobs)

    # clipped_pres = large_raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # clipped_fut = raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # future_fcc = area_deforested(clipped_fut)  # in hectares
    deforested_areas = {year: area_deforested(raster) for year, raster in clipped_rasters.items()}
    # --------------------------------------------------------------------------
    # Clip biomass
    # -----------------------------------------------------------------------------
    agb_clip = agb.rio.clip_box(
        minx=country.bounds["minx"].values[0],
        miny=country.bounds["miny"].values[0],
        maxx=country.bounds["maxx"].values[0],
        maxy=country.bounds["maxy"].values[0]
    ).rio.clip(country.geometry, from_disk=True)
    # agb_clip = agb.rio.clip(country.to_crs(agb.rio.crs).geometry.values, from_disk=True)
    # agb_clip = agb_clip.rio.reproject(dst_crs=aea_crs, nodata=0)
    masks = {year: raster.rio.reproject_match(agb_clip, nodata=0) for year, raster in clipped_rasters.items() if year != 2020}
    del clipped_rasters
    # --------------------------------------------------------------------------
    # Make mask of deforested pixels
    # -----------------------------------------------------------------------------
    # mask = clipped_fut.rio.reproject_match(agb_clip, nodata=0)
    masked_agb = {year: agb_clip * mask for year, mask in masks.items()}
    del masks
    # agb_masked = agb_clip * mask
    # MgC/ha * CO2/C * ha = MgCO2
    emissions = {year: agb_masked.sum().values * 44/12 * deforested_areas[year] for year, agb_masked in masked_agb.items()}
    del masked_agb
    # emissions = agb_masked.sum() * 44/12 * future_fcc  

    # --------------------------------------------------------------------------
    # Save country data
    # -----------------------------------------------------------------------------

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
    country_summary.to_csv(f"{region}/{country_name}/{name_formatter(country['ADMIN'].values[0])}_summary.csv", index=False)
    continent_summary = pd.concat([continent_summary, country_summary])
    return continent_summary

def process_country_sequential(country, continent_summary):
    # country_name = "Cambodia"
    # breakpoint()
    country = srs_to_gdf(country, aea_crs)  # Convert to gdf for ability to reproject later
    country_name = country["ADMIN"].values[0]
    # if os.path.exists(f"{region}/{country_name}") and len(glob(f"{region}/{country_name}/*")) > 0:
    #     print(f"Skipping {country_name}...")
    #     return continent_summary
    if not os.path.exists(f"{region}/{country_name}"):
        os.makedirs(f"{region}/{country_name}")
    # country = continent_gdf[continent_gdf['NAME'] == country_name]
    # Clip fcc rasters to country, box-first.
    clipped_rasters = {
        year: dask.delayed(raster.rio.clip_box(
            minx=country.bounds["minx"].values[0],
            miny=country.bounds["miny"].values[0],
            maxx=country.bounds["maxx"].values[0],
            maxy=country.bounds["maxy"].values[0]
        ).rio.clip)(
            [country.geometry.values[0]], 
            from_disk=True
        ) for year, raster in rasters_dict.items()
    }
    # for year, raster in clipped_rasters.items():
        # raster.rio.to_raster(destname(country, region, year))
    jobs = [dask.delayed(raster.rio.to_raster)(destname(country, region, year), compute=False) for year, raster in clipped_rasters.items()]
    dask.compute(jobs)

    # clipped_pres = large_raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # clipped_fut = raster.rio.clip(cambodia.geometry.values, from_disk=True)
    # future_fcc = area_deforested(clipped_fut)  # in hectares
    deforested_areas = {year: dask.delayed(area_deforested)(raster) for year, raster in clipped_rasters.items()}
    # --------------------------------------------------------------------------
    # Clip biomass
    # -----------------------------------------------------------------------------
    agb_clip = agb.rio.clip_box(
        minx=country.bounds["minx"].values[0],
        miny=country.bounds["miny"].values[0],
        maxx=country.bounds["maxx"].values[0],
        maxy=country.bounds["maxy"].values[0]
    ).rio.clip(country.geometry, from_disk=True)
    # agb_clip = agb.rio.clip(country.to_crs(agb.rio.crs).geometry.values, from_disk=True)
    # agb_clip = agb_clip.rio.reproject(dst_crs=aea_crs, nodata=0)
    masks = {year: raster.rio.reproject_match(agb_clip, nodata=0) for year, raster in clipped_rasters.items() if year != 2020}
    # del clipped_rasters
    # --------------------------------------------------------------------------
    # Make mask of deforested pixels
    # -----------------------------------------------------------------------------
    # mask = clipped_fut.rio.reproject_match(agb_clip, nodata=0)
    def multiply_mask(mask):
        return agb_clip * mask
    # masked_agb = {year: dask.delayed(multiply_mask)(mask) for year, mask in masks.items()}
    masked_agb = {year: agb for year, agb in dask.compute([dask.delayed(multiply_mask)(mask) for mask in list(masks.values())])}
    del masks
    # agb_masked = agb_clip * mask
    # MgC/ha * CO2/C * ha = MgCO2
    with ProgressBar():
        deforested = dask.compute(list(deforested_areas.values()))
    deforested_areas = {year: area for year, area in zip([2030, 2040, 2050], deforested[:3])}    
    emissions = {year: agb_masked.sum().values * 44/12 * deforested_areas[year] for year, agb_masked in masked_agb.items()}
    
    # deforested_areas = {year: area for year, area in zip([2030, 2040, 2050], computed[:3])}
    # emissions = {year: em for year, em in zip([2030, 2040, 2050], computed[3:])}
    del masked_agb
    # emissions = agb_masked.sum() * 44/12 * future_fcc  

    # --------------------------------------------------------------------------
    # Save country data
    # -----------------------------------------------------------------------------

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
    country_summary.to_csv(f"{region}/{country_name}/{name_formatter(country['ADMIN'].values[0])}_summary.csv", index=False)
    continent_summary = pd.concat([continent_summary, country_summary])
    return continent_summary
# %% --------------------------------------------------------------------------
# {1:Enter description for cell}
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    with tqdm.tqdm(total=len(continent_gdf)) as pbar:
        for index, country in continent_gdf[35:].iterrows():
            if country["ADMIN"] == "India":
                continue
            elif country["CONTINENT"] == "North America":
                continue
            pbar.set_description(f"Processing {country['ADMIN']}")
            continent_summary = process_country_sequential(country, continent_summary)
            pbar.update()
    continent_summary.to_csv(f"{region}/{region}_summary.csv", index=False)

oversized_countries = ["Australia", "Indonesia"]

# continent_summary = process_country(continent_gdf.loc[continent_gdf.index==228], continent_summary)
# print(continent_summary)

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
