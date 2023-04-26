"""
Enter script name

Enter short description of the script
"""

__date__ = "2023-04-20"
__author__ = "Jack J Window"
__email__ = "jack.window@trove-research.com"


# %% --------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------
import numpy as np
import xarray as xr
import rioxarray as rxr
# import matplotlib.pyplot as plt
# from matplotlib.colors import LogNorm
import tqdm
import dask
from dask.diagnostics import ProgressBar
import os
import shapely as shp
import geopandas as gpd
from threading import Lock

# %% --------------------------------------------------------------------------
# Setup paths
# -----------------------------------------------------------------------------

years = [2030, 2040, 2050]
continents = ["ASI", "AFR", "AME"]

# directory of paths: sources[continent][year] = path
source_root = r"C:\Users\JackWindow\Documents\forestatrisk_data"
sources = {
    cont: {
        year: f"fcc_{year}_{cont}_aea.tif" for year in years
    } for cont in continents
}
# Same as above for destinations
destination_root = r"C:\Users\JackWindow\Trove Research Ltd\Trove Research Ltd - Documents\6 - Trove Intelligence Content\Geospatial\rasters\forestatrisk_defor_rates"
destinations = {
    cont: {
        year: f"defor_rate_{year}_{cont}_aea.tif" for year in years
    } for cont in continents
}

def calculate_rates(cont):
    source_paths = sources[cont]
    destination_paths = destinations[cont]
    jobs = []

    for year in years:
        if year == 2030:
            defor_rate = rxr.open_rasterio(
                os.path.join(source_root, source_paths[year]),
                chunks=True,
                # lock=Lock()  # Just in case
            ) * 90
            defor_rate.attrs["units"] = "m^2/yr"
        else:
            # Open forest cover change raster
            fcc_post = rxr.open_rasterio(
                os.path.join(source_root, source_paths[year]), 
                chunks=True,
                # lock=Lock()  # Just in case
            )
            # All forest cover change is measured from 2020. To get the deforestation rate
            # for each decade, we need to subtract the previous decade from the current.
            prior_path = os.path.join(source_root, source_paths[year-10])
            fcc_prior = rxr.open_rasterio(
                prior_path,
                chunks=True,
                # lock=Lock()  # Just in case
            )
            # Subtract the two rasters
            # No need to divide by 10 yet, it will just force the data to float and take
            # up more memory. Currently each pixel is 0 or 1. 
            defor_rate = fcc_post - fcc_prior
            defor_rate = defor_rate.astype(np.uint8)
            # Convert to square meters of deforestation (over the decade)
            # Again, meters to avoid float
            # 1 pixel = 30m x 30m = 900m^2
            # Divided by 10 to get the rate per year
            # 900m^2 / 10yrs = 90m^2/yr
            defor_rate = defor_rate * 90
            # Set the units
            defor_rate.attrs["units"] = "m^2/yr"
        
        # Write to disk using dask delayed and compute as a batch.
        write_job = defor_rate.rio.to_raster(
            os.path.join(destination_root, destination_paths[year]),
            lock=Lock(),
            compute=False,
            dtype=np.uint8,
            compress="lzw"
        )
        jobs.append(write_job)
    return jobs


# %% --------------------------------------------------------------------------
# Execution
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    all_jobs = []
    for cont in continents:
        # Calculate the deforestation rates
        jobs = calculate_rates(cont)
        # Execute the jobs
        all_jobs += jobs
    # Execute all jobs
    with ProgressBar():
        dask.compute(all_jobs)
    print("Done")

# %% --------------------------------------------------------------------------
# Open raster (test)
# -----------------------------------------------------------------------------
# cont = "AFR"
# year = 2040
# path = os.path.join(source_root, sources[cont][year])

# # Open forest cover change raster
# fcc_post = rxr.open_rasterio(
#     path, 
#     chunks=True,
#     # lock=Lock()  # Just in case
# )
# # All forest cover change is measured from 2020. To get the deforestation rate
# # for each decade, we need to subtract the previous decade from the current.
# prior_path = os.path.join(source_root, sources[cont][year-10])
# fcc_prior = rxr.open_rasterio(
#     prior_path,
#     chunks=True,
#     # lock=Lock()  # Just in case
# )

# %% --------------------------------------------------------------------------
# Deforestation rate
# -----------------------------------------------------------------------------
# #  Subtract the two rasters
# # No need to divide by 10 yet, it will just force the data to float and take
# # up more memory. Currently each pixel is 0 or 1. 
# defor_rate = fcc_post - fcc_prior
# defor_rate = defor_rate.astype(np.uint8)
# # Convert to square meters of deforestation (over the decade)
# # Again, meters to avoid float
# # 1 pixel = 30m x 30m = 900m^2
# # Divided by 10 to get the rate per year
# # 900m^2 / 10yrs = 90m^2/yr
# defor_rate = defor_rate * 90
# # Set the units
# defor_rate.attrs["units"] = "m^2/yr"

# # %% --------------------------------------------------------------------------
# # saving
# # -----------------------------------------------------------------------------
# # Write to disk using dask delayed and compute as a batch.
# # This is to avoid memory issues
# # Save to disk
# destination = os.path.join(destination_root, destinations[cont][year])
# write = defor_rate.rio.to_raster(destination, lock=Lock(), compute=False, dtype="int16", compress="lzw")
# with ProgressBar():
#     write.compute()

# %% --------------------------------------------------------------------------
# {1:Enter description for cell}
# -----------------------------------------------------------------------------


# %%
