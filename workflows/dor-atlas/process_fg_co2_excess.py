import os
import traceback

import dask
import loky
import typer
import xarray as xr
from dor_cli import setup_directories
from dor_config import DORConfig
from rich.console import Console
from vis_pyramid import get_nc_glob_pattern, setup_memory

app = typer.Typer()

console = Console()
config = DORConfig()

dirs = setup_directories()
memory = setup_memory(dirs["joblib_cache_dir"])


@memory.cache
def create_fg_co2_excess(polygon_id: str, intervention_month: str, data_dir: str):
    try:
        path = get_nc_glob_pattern(data_dir, polygon_id, intervention_month)
        console.print(path)
        with dask.config.set(
            pool=loky.ProcessPoolExecutor(max_workers=os.cpu_count() // 2, timeout=120)
        ):
            dset = xr.open_mfdataset(
                path,
                coords="minimal",
                combine="by_coords",
                data_vars="minimal",
                compat="override",
                decode_times=True,
                parallel=True,
                decode_timedelta=True,
            )

            dset["FG_CO2_excess"] = dset.FG_CO2 - dset.FG_ALT_CO2
            ds = dset[["FG_CO2_excess"]].drop_vars(["time"])
            ds = dask.optimize(ds.chunk(elapsed_time=-1))[0]
            outpath = f"{config.scratch_dir}/fg-co2-excess/{polygon_id}-{intervention_month}.zarr"
            ds.to_zarr(outpath, consolidated=True, zarr_format=2, mode="w")
            console.print(outpath)

    except Exception:
        console.print(
            f"[bold red]Error processing {polygon_id}/{intervention_month}: "
            f"{traceback.format_exc()}[/bold red]"
        )


@app.command()
def process_fg_co2_excess(
    polygon_id: int = typer.Option(..., "-p", help="Polygon ID"),
    data_dir: str = typer.Option(config.compressed_data_dir, help="Data directory"),
):
    """Process FG CO2 excess for a given polygon and intervention month."""
    # ensure polygon_id is padded with leading zeros
    padded_polygon_id = f"{polygon_id:03d}"
    padded_intervention_months = ["01", "04", "07", "10"]
    for intervention_month in padded_intervention_months:
        console.print(
            f"[bold green]Processing {padded_polygon_id} for {intervention_month}...[/bold green]"
        )
        create_fg_co2_excess(padded_polygon_id, intervention_month, data_dir)
    console.print(
        f"[bold green]Finished processing {padded_polygon_id} for all intervention months.[/bold green]"
    )


if __name__ == "__main__":
    app()
