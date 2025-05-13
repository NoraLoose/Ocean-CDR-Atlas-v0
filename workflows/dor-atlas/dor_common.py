import pathlib
import sys
import traceback

import cftime
import joblib
import numpy as np
import pandas as pd
import typer
import xarray as xr
from rich.console import Console

# Append parent directory to path to import atlas
parent_dir = pathlib.Path.cwd().parent
sys.path.append(str(parent_dir))
try:
    import atlas

except ImportError:
    typer.echo("Error: Atlas module not found. Make sure it's in the parent directory.")
    sys.exit(1)

console = Console()


# But loads of these data variables aren't really dependent data variables at all...
COORDS = [
    "dz",
    "dzw",
    "KMT",
    "KMU",
    "REGION_MASK",
    "UAREA",
    "TAREA",
    "HU",
    "HT",
    "DXU",
    "DYU",
    "DXT",
    "DYT",
    "HTN",
    "HTE",
    "HUS",
    "HUW",
    "time_bound",
    # everything beyond here are just like fundamental constants or conversion factors, do we actually need these?
    "ANGLE",
    "ANGLET",
    "days_in_norm_year",
    "grav",
    "omega",
    "cp_sw",
    "vonkar",
    "rho_air",
    "rho_sw",
    "rho_fw",
    "stefan_boltzmann",
    "latent_heat_vapor",
    "latent_heat_fusion",
    "latent_heat_fusion_mks",
    "ocn_ref_salinity",
    "sea_ice_salinity",
    "T0_Kelvin",
    "salt_to_ppt",
    "ppt_to_salt",
    "mass_to_Sv",
    "heat_to_PW",
    "salt_to_Svppt",
    "salt_to_mmday",
    "momentum_factor",
    "hflux_factor",
    "fwflux_factor",
    "salinity_factor",
    "sflux_factor",
    "nsurface_t",
    "nsurface_u",
    "radius",
    "sound",
    "cp_air",
    "area_m2",
]


def get_cases_df():
    """Get DataFrame of available cases from the atlas."""
    try:
        calc = atlas.global_irf_map(cdr_forcing="DOR", vintage="001")
        done_cases = calc.df.index.to_list()
        if "smyle.cdr-atlas-v0.control.001" in done_cases:
            done_cases.remove("smyle.cdr-atlas-v0.control.001")
        done_cases = sorted(done_cases)
        df = calc.df.loc[done_cases]
        return df
    except Exception as _:
        console.print(
            f"[bold red]Error getting cases: {traceback.format_exc()}[/bold red]"
        )
        raise typer.Exit(1)


def get_case_metadata(case: str, df: pd.DataFrame) -> pd.Series | pd.DataFrame:
    """Get metadata for a specific case."""
    case_metadata = df.loc[case]
    return case_metadata


def generate_padded_ids(start: int, end: int) -> list[str]:
    """Generate zero-padded string IDs for a range of integers."""
    return [f"{i:03d}" for i in range(start, end)]


def generate_padded_months(months: list[int]) -> list[str]:
    """Generate zero-padded string months."""
    return [f"{month:02d}" for month in months]


def get_nc_glob_pattern(data_dir: str, polygon_id: str, intervention_month: str) -> str:
    """Generate glob pattern for NetCDF files."""
    return f"{data_dir}/{polygon_id}/{intervention_month}/*.nc"


def setup_memory(cache_dir):
    """Setup joblib memory cache"""
    global memory
    memory = joblib.Memory(cache_dir, verbose=0)
    return memory


def set_elapsed_time(ds: xr.Dataset):
    elapsed_time_integer_months = xr.DataArray(
        np.arange(180), dims=["elapsed_time"], attrs={"units": "months"}
    )
    ds["elapsed_time"] = elapsed_time_integer_months.astype("int32")
    return ds


def add_polygon_id_coord(ds: xr.Dataset, case_metadata: pd.Series):
    """Add polygon ID coordinate to dataset."""
    polygon_master = int(case_metadata.polygon_master)
    if polygon_master < 0 or polygon_master > 689:
        raise ValueError(
            f"Polygon id must be in range [0, 690). Found polygon_id={polygon_master}"
        )
    # Add as an integer coordinate
    polygon_id_coord = xr.DataArray(
        name="polygon_id",
        dims="polygon_id",
        data=[polygon_master],
        attrs={"long_name": "polygon ID"},
    ).astype("int32")
    return ds.assign_coords(polygon_id=polygon_id_coord)


def add_intervention_date_coord(ds: xr.Dataset, case_metadata: pd.Series):
    """Add intervention date coordinate to dataset."""
    intervention_date_coord = xr.DataArray(
        data=[
            cftime.DatetimeNoLeap.strptime(
                case_metadata.start_date, "%Y-%m", calendar="noleap", has_year_zero=True
            )
        ],
        dims=["intervention_date"],
        attrs={"long_name": "intervention date"},
    )

    return ds.assign_coords(intervention_date=intervention_date_coord)


def expand_ensemble_dims(ds: xr.Dataset) -> xr.Dataset:
    """Add new dimensions across the ensemble."""
    copied = ds.copy()

    # All data variables should be ensemble variables
    for name in list(ds.data_vars):
        copied[name] = copied[name].expand_dims(["polygon_id", "intervention_date"])

    # Absolute time is a function of intervention_date because of the different starting times
    if "time" in copied.dims:
        copied["time"] = copied["time"].expand_dims(["intervention_date"])
    if "time_bound" in copied.dims:
        copied["time_bound"] = copied["time_bound"].expand_dims(["intervention_date"])

    return copied


def add_elapsed_time_coord(ds: xr.Dataset):
    """Add elapsed time coordinate."""
    # Extract year and month
    year = ds.time.dt.year
    month = ds.time.dt.month

    # Compute current year based on formula
    current_year = 1999 + year - int("0347")  # Equivalent to 1999 + year - 347

    # Create DataArray of datetime objects
    current_time = xr.DataArray(
        [
            cftime.datetime(y, m, 1, calendar="noleap", has_year_zero=True)
            for y, m in zip(current_year, month)
        ],
        dims="time",
        name="current_time",
    )
    return (
        ds.drop_indexes("time")
        .rename_dims(time="elapsed_time")
        .assign_coords(elapsed_time=current_time.data - ds.intervention_date.data)
    )
