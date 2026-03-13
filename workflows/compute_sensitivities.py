#!/usr/bin/env python

import xarray as xr
import s3fs
import numpy as np
from datetime import timedelta
import PyCO2SYS as pyco2
import argparse

# -----------------------------
# Parse arguments
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--config", choices=["monthly", "daily"], required=True)
args = parser.parse_args()

CONFIG = args.config

# -----------------------------
# Configurations
# -----------------------------
if CONFIG == "monthly":

    file_suffix = "monthly"
    USE_S3 = True

    S3_DATA_FILE = (
        "s3://us-west-2.opendata.source.coop/cworthy/"
        "oae-efficiency-atlas/data/control/"
        "g.e22.GOMIPECOIAF_JRA-1p4-2018.TL319_g17.SMYLE.005.pop.h.TEMP.030601-036812.nc"
    )

    selectors = {
        "time": slice(480, 492 + 12 * 22),
        "z_t": 0,
    }

    VAR_MAP = {
        "temp": "TEMP",
        "salt": "SALT",
        "ALK": "ALK",
        "DIC": "DIC",
        "PO4": "PO4",
        "SiO3": "SiO3",
    }

else:

    file_suffix = "daily"
    USE_S3 = False

    LOCAL_PREFIX = (
        "/global/cfs/projectdirs/m4746/Users/nora/Ocean-CDR-Atlas-v0/"
        "data/archive/smyle.cdr-atlas-v0.control.001_backup/ocn/hist/"
        "smyle.cdr-atlas-v0.control.001.pop.h.nday1"
    )

    LOCAL_SUFFIX = "nc"

    selectors = {
        "z_t": 0,
    }

    VAR_MAP = {
        "temp": "SST",
        "salt": "SSS",
        "ALK": "ALK",
        "DIC": "DIC",
        "PO4": "PO4",
        "SiO3": "SiO3",
    }


# -----------------------------
# Load dataset
# -----------------------------
def load_data():

    ds = xr.Dataset()

    if USE_S3:

        fs = s3fs.S3FileSystem(anon=True)

        for key, var in VAR_MAP.items():

            s3_path = S3_DATA_FILE.replace("TEMP", var)

            with fs.open(s3_path, "rb") as f:
                ds0 = xr.open_dataset(f, decode_timedelta=False)
                ds0 = ds0.isel(**selectors)
                ds0.load()
                ds[key] = ds0[var]

    else:

        path = f"{LOCAL_PREFIX}.*.{LOCAL_SUFFIX}"

        ds0 = xr.open_mfdataset(
            path,
            decode_times=True,
            decode_timedelta=True,
        )

        ds0 = ds0.isel(**selectors)

        for key, var in VAR_MAP.items():
            ds[key] = ds0[var]

    ds["time_bound"] = ds0["time_bound"]

    return ds


# -----------------------------
# Fix time axis
# -----------------------------
def move_time_to_middle(ds):

    time_bounds = ds["time_bound"].values

    mid_times = [
        start + timedelta(days=(end - start).days / 2)
        for start, end in time_bounds
    ]

    ds = ds.assign_coords(time=("time", mid_times))

    return ds


# -----------------------------
# Carbonate sensitivities
# -----------------------------
def compute_carbonate_sensitivity(ds):

    csys = pyco2.sys(
        par1=ds.ALK * 1000 / 1025,
        par2=ds.DIC * 1000 / 1025,
        par1_type=1,
        par2_type=2,
        salinity=ds.salt,
        temperature=ds.temp,
        total_silicate=ds.SiO3 * 1000 / 1025,
        total_phosphate=ds.PO4 * 1000 / 1025,
    )

    beta = (
        (csys["dic"] - (csys["HCO3"] + 2 * csys["CO3"]) /
         csys["isocapnic_quotient"])
        / csys["CO2"]
    )

    eta = 1 / csys["isocapnic_quotient"]

    return beta, eta


# -----------------------------
# Run pipeline
# -----------------------------
print("Loading data...")
ds = load_data()

print("Fixing time axis...")
ds = move_time_to_middle(ds)

print("Computing carbonate sensitivities...")
beta, eta = compute_carbonate_sensitivity(ds)

print("Building dataset...")
ds_out = xr.Dataset(
    data_vars={
        "dDICdCO2": (["time", "nlat", "nlon"], beta),
        "dDICdALK": (["time", "nlat", "nlon"], eta),
    },
    coords={
        "time": ds.time,
        "TLAT": ds.TLAT,
        "TLONG": ds.TLONG,
    },
)

OUTPUT_FILE = (
    f"/global/cfs/projectdirs/m4746/Projects/OAE-Efficiency-Map/data/"
    f"carbonate-sensitivities/carbonate_sensitivity_{file_suffix}.nc"
)

print(f"Writing {OUTPUT_FILE}")
ds_out.to_netcdf(OUTPUT_FILE)

print("Done.")
