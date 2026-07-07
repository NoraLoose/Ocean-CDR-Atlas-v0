import os
import xarray as xr
import numpy as np
import argparse
from analysis import INVERSE_POLYGON_MAP, compute_spatial_integral, compute_forcing, compute_air_sea_flux, finalize, deficit_tracer_experiment_paths

def main(suffix: str, mode: str, intervention_month="01", intervention_year="1999", polygon_ids=None):
    """
    Precompute approximate delta DIC and efficiency curves for all polygons in a given suffix.
    Saves results to NetCDF.
    """
    # Open dataset once
    subdir, path = deficit_tracer_experiment_paths(suffix, intervention_month, intervention_year)

    ds = xr.open_mfdataset(path, decode_timedelta=False)

    # If no polygons specified, use all available in the dataset
    if polygon_ids is None:
        # assume polygon IDs from the dataset keys
        polygon_ids = [
            pid.split("DELTADIC")[-1].split("_")[0]
            for pid in ds.data_vars
            if "DELTADIC" in pid
        ]
        # remove empty strings and duplicates
        polygon_ids = sorted([pid for pid in set(polygon_ids) if pid])


    base_path = "/global/cfs/cdirs/m4746/Users/nora/Ocean-CDR-Atlas-v0/data/analysis/"
    for pid in polygon_ids:
        # Build output path
        output_file = f"{base_path}/{subdir}/integrated_{pid}.nc"
        # Skip if file already exists
        if os.path.exists(output_file):
            print(f"Skipping polygon {pid}, file already exists: {output_file}")
            continue
    
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        if mode == "dor":
            forcing = compute_spatial_integral(
                - compute_forcing(ds, f"DELTADIC{pid}_FORCING", cumulative=True), ds
            ) # minus sign because I applied positive DIC forcing (from alk forcing files)
            delta_dic = compute_spatial_integral(
                -compute_air_sea_flux(ds, f"STF_DELTADIC{pid}", f"DELTADIC{pid}_FORCING", cumulative=True),
                ds
        ) # (total surface flux - external dic forcing) to get fco2; minus sign because I applied positive DIC forcing (from alk forcing files)
        elif mode == "oae":
            forcing = compute_spatial_integral(
                compute_forcing(ds, f"DELTAALK{pid}_FORCING", cumulative=True), ds
            )       
            delta_dic = compute_spatial_integral(
                compute_air_sea_flux(ds, f"STF_DELTADIC{pid}", f"DELTADIC{pid}_FORCING", cumulative=True),
                ds
            ) # (total surface flux - external dic forcing) to get fco2
    
        # Wrap in Dataset
        ds_out = xr.Dataset({
            "forcing": finalize(forcing.assign_coords(polygon_id=pid), ds),
            "uptake": finalize(delta_dic.assign_coords(polygon_id=pid), ds)
        })
    
        # Save to NetCDF
        ds_out.to_netcdf(output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Precompute deficit tracer DIC and efficiency curves for all polygons.")
    parser.add_argument("suffix", type=str, help="Suffix for the approximate experiment (e.g., 'all', 'test3')")
    parser.add_argument("mode", type=str, help="Mode ('dor', 'oae')")
    parser.add_argument("--polygons", type=str, nargs="+", default=None, help="Optional list of polygon IDs to compute")

    args = parser.parse_args()
    main(args.suffix, args.mode, polygon_ids=args.polygons)

