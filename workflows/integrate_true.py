import os
import argparse
import xarray as xr

from analysis import (
    compute_spatial_integral,
    compute_forcing,
    compute_air_sea_flux,
    finalize,
    dor_experiment_paths,
    oae_experiment_paths,
)

MODES = {
    "dor": (dor_experiment_paths, "DIC_FLUX"),
    "oae": (oae_experiment_paths, "ALK_FLUX"),
}


def default_polygon_ids():
    return [f"{pid:03d}" for pid in range(0, 690)]


def main(
    mode,
    polygon_ids=None,
    intervention_month="01",
    intervention_year="1999",
):
    """
    Precompute forcing and uptake curves for DOR or OAE experiments.
    Saves one NetCDF per polygon.
    """
    analysis_base = (
        "/global/cfs/projectdirs/m4746/Users/nora/"
        "Ocean-CDR-Atlas-v0/data/analysis"
    )

    experiment_paths, flux_varname = MODES[mode]

    if polygon_ids is None:
        polygon_ids = default_polygon_ids()

    for pid in polygon_ids:
        subdir, path = experiment_paths(pid, intervention_month, intervention_year)
        output_file = f"{analysis_base}/{subdir}/integrated.nc"
    
        if os.path.exists(output_file):
            print(f"Skipping polygon {pid}, file exists: {output_file}")
            continue

        print(f"Processing polygon {pid}")

        ds = xr.open_mfdataset(path, decode_timedelta=False)

        forcing = compute_spatial_integral(
            compute_forcing(ds, flux_varname, cumulative=True),
            ds,
        )

        uptake = compute_spatial_integral(
            compute_air_sea_flux(ds, "FG_CO2", "FG_ALT_CO2", cumulative=True),
            ds,
        )

        ds_out = xr.Dataset(
            {
                "forcing": finalize(forcing, ds),
                "uptake": finalize(uptake, ds),
            }
        )

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        ds_out.to_netcdf(output_file)

        ds.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Precompute uptake and forcing curves per polygon for DOR or OAE experiments."
    )
    parser.add_argument(
        "mode",
        choices=["dor", "oae"],
        help="Experiment type: 'dor' (DIC_FLUX) or 'oae' (ALK_FLUX)",
    )
    parser.add_argument(
        "--polygons",
        type=str,
        nargs="+",
        default=None,
        help="Optional list of polygon IDs to compute",
    )

    args = parser.parse_args()

    main(mode=args.mode, polygon_ids=args.polygons)
