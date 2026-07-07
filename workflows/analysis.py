#!/usr/bin/env python
# coding: utf-8

import numpy as np
import xarray as xr
import cftime
import glob

# -----------------------------
# Polygon maps
# -----------------------------
BASINS = [
    "North_Atlantic_basin",
    "North_Pacific_basin",
    "South",
    "Southern_Ocean",
]

NPOLYGON = {
    "North_Atlantic_basin": 150,
    "North_Pacific_basin": 200,
    "South": 300,
    "Southern_Ocean": 40,
}

# Forward and inverse maps
POLYGON_MASTER_MAP = {}  # (basin, polygon) → index
INVERSE_POLYGON_MAP = {}  # index → (basin, polygon)
idx = 0
for b in BASINS:
    for p in range(NPOLYGON[b]):
        POLYGON_MASTER_MAP[(b, p)] = idx
        INVERSE_POLYGON_MAP[idx] = (b, p)
        idx += 1

# -----------------------------
# Time utilities
# -----------------------------
def _time_dim(ds):
    """Return the name of the time dimension ('time' or 'elapsed_time')."""
    if "time" in ds.dims:
        return "time"
    if "elapsed_time" in ds.dims:
        return "elapsed_time"
    raise ValueError("Dataset has neither 'time' nor 'elapsed_time' dimension")

def compute_dt_seconds(ds):
    """Return time step length in seconds for each time index."""
    tdim = _time_dim(ds)
    dt = ds.time_bound.isel(d2=1).squeeze() - ds.time_bound.isel(d2=0).squeeze()
    dt_seconds = np.array([d.total_seconds() for d in dt.values])
    return xr.DataArray(dt_seconds, dims=tdim)

def compute_elapsed_days(ds):
    """
    Compute elapsed days relative to one month before the first time in ds.time.
    
    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        Must have a 'time' coordinate (cftime or datetime).
    
    Returns
    -------
    xarray.DataArray
        Elapsed days coordinate.
    """
    # First time in the dataset
    t0 = ds.time.squeeze().values[0]

    # Only works with cftime.DatetimeNoLeap
    if not isinstance(t0, cftime.DatetimeNoLeap):
        raise ValueError("ds.time must be cftime.DatetimeNoLeap for this function.")

    # Subtract one month safely
    month = t0.month - 1
    year = t0.year
    if month == 0:
        month = 12
        year -= 1
    # Use day=1, safe for all months
    start = cftime.DatetimeNoLeap(year, month, 1)

    # Compute elapsed days
    elapsed = np.array([(t - start).days for t in ds.time.squeeze().values])

    return xr.DataArray(elapsed, dims=_time_dim(ds), attrs={"units": "days"})

def finalize(arr, ds):
    """Attach elapsed_time coordinate and return cleaned DataArray."""
    elapsed_days = compute_elapsed_days(ds)
    return arr.assign_coords(elapsed_time=elapsed_days)

# -----------------------------
# Experiment paths and dataset loaders
# -----------------------------
def dor_experiment_paths(
    polygon_id,
    intervention_month="01",
    intervention_year="1999",
    realization="001",
):
    """Return (subdir, file_glob) for true CESM-MARBL experiment."""
    base_path = "/global/cfs/projectdirs/m4746/Projects/Ocean-CDR-Atlas-v0/data/archive"

    b, p = INVERSE_POLYGON_MAP[int(polygon_id)]
    multiplier = f"{int(polygon_id) * 4:05d}"

    subdir = (
        f"smyle.cdr-atlas-v0.glb-dor_"
        f"{b}_{p:03d}_{intervention_year}-{intervention_month}-01_"
        f"{multiplier}.{realization}"
    )

    file_glob = f"{base_path}/{subdir}/ocn/hist/*.pop.h.*.nc"

    return subdir, file_glob

def oae_experiment_paths(
    polygon_id,
    intervention_month="01",
    intervention_year="1999",
    realization="001",
):
    """Return (subdir, file_glob) for true CESM-MARBL experiment."""
    base_path = "/pscratch/sd/m/mattlong/atlas_cache/experiments/"

    b, p = INVERSE_POLYGON_MAP[int(polygon_id)]
    multiplier = f"{int(polygon_id) * 4:05d}"

    subdir = (
        f"smyle.cdr-atlas-v0.glb-oae_"
        f"{b}_{p:03d}_{intervention_year}-{intervention_month}-01_"
        f"{multiplier}.{realization}"
    )

    file_glob = f"{base_path}/{polygon_id}/{intervention_month}/alk-forcing.{polygon_id}-{intervention_year}-{intervention_month}.pop.h.*.nc"

    return subdir, file_glob
    
def deficit_tracer_experiment_paths(
    suffix,
    intervention_month="01",
    intervention_year="1999",
    realization="001",
):
    """Return (subdir, file_glob) for deficit tracer experiment."""
    base_path = (
        "/global/cfs/cdirs/m4746/Users/nora/"
        "Ocean-CDR-Atlas-v0/data/archive"
    )

    subdir = (
        f"smyle.cdr-atlas-v0.glb-antitracer_"
        f"{intervention_year}-{intervention_month}_{suffix}.{realization}"
    )

    file_glob = (
        f"{base_path}/{subdir}/ocn/hist/"
        f"smyle.cdr-atlas-v0.glb-antitracer_"
        f"{intervention_year}-{intervention_month}_{suffix}.{realization}"
        ".pop.h.*.nc"
    )

    return subdir, file_glob

MODES = {
    "dor": dor_experiment_paths,
    "oae": oae_experiment_paths,
}

def open_true_experiment(*args, mode, first_file=False, **kwargs):

    experiment_paths = MODES[mode]

    _, path = experiment_paths(*args, **kwargs)

    
    if first_file:
        files = sorted(glob.glob(path))
        if not files:
            raise FileNotFoundError(f"No files match: {path}")
        return xr.open_dataset(files[0], decode_timedelta=False)
        
    return xr.open_mfdataset(path, decode_timedelta=False)


def open_deficit_tracer_experiment(*args, first_file=False, **kwargs):
    _, path = deficit_tracer_experiment_paths(*args, **kwargs)

    if first_file:
        files = sorted(glob.glob(path))
        if not files:
            raise FileNotFoundError(f"No files match: {path}")
        return xr.open_dataset(files[0], decode_timedelta=False)
    return xr.open_mfdataset(path, decode_timedelta=False)

analysis_base = (
    "/global/cfs/projectdirs/m4746/Users/nora/"
    "Ocean-CDR-Atlas-v0/data/analysis"
)


def open_true_curve(
    pid,
    mode,
    intervention_month="01",
    intervention_year="1999",
    realization="001",
):

    experiment_paths = MODES[mode]
        
    subdir, _ = experiment_paths(
        pid,
        intervention_month=intervention_month,
        intervention_year=intervention_year,
        realization=realization,
    )

    output_file = f"{analysis_base}/{subdir}/integrated.nc"
    return xr.open_dataset(output_file, decode_timedelta=False)


def open_deficit_tracer_curve(
    pid,
    suffix,
    intervention_month="01",
    intervention_year="1999",
    realization="001",
):
    subdir, _ = deficit_tracer_experiment_paths(
        suffix,
        intervention_month=intervention_month,
        intervention_year=intervention_year,
        realization=realization,
    )

    output_file = f"{analysis_base}/{subdir}/integrated_{pid}.nc"
    return xr.open_dataset(output_file, decode_timedelta=False)

# ------------------
# Flux computations
# -----------------------------
def compute_forcing(ds, flux, cumulative=True):
    """Compute DIC forcing integrated over time."""
    dt = compute_dt_seconds(ds)
    forcing = ds[flux] * dt # nmol/cm^2/s * s = nmol/cm^2
    if cumulative:
        forcing = forcing.cumsum(_time_dim(ds))
    return forcing.where(ds.KMT > 0)

def compute_air_sea_flux(ds, flux1, flux2, cumulative=True):
    """Compute change in DIC inventory from two flux variables."""
    dt = compute_dt_seconds(ds)
    delta_dic = (ds[flux1] - ds[flux2]) * dt # mmol/m^3 cm/s * s = mmol/m^3 cm
    if cumulative:
        delta_dic = delta_dic.cumsum(_time_dim(ds))
    return delta_dic.where(ds.KMT > 0)

def compute_spatial_integral(da, ds):
    """Spatially integrate a DataArray over the ocean grid."""
    return (da * ds.TAREA).sum(["nlat", "nlon"])

