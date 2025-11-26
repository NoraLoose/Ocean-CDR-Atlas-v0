#!/usr/bin/env python3
import re
import xarray as xr
from pathlib import Path

# --------------------------------------------------------
# Basin → global polygon index mapping
# --------------------------------------------------------
basins = [
    "North_Atlantic_basin",
    "North_Pacific_basin",
    "South",
    "Southern_Ocean",
]

npolygon = {
    "North_Atlantic_basin": 150,
    "North_Pacific_basin": 200,
    "South": 300,
    "Southern_Ocean": 40,
}

polygon_master_map = {}
inverse_polygon_map = {}

idx = 0
for b in basins:
    for p in range(npolygon[b]):
        polygon_master_map[(b, p)] = idx
        inverse_polygon_map[idx] = (b, p)
        idx += 1

# --------------------------------------------------------
# Input files
# --------------------------------------------------------
indir = Path("/global/cfs/projectdirs/m4746/Projects/OAE-Efficiency-Map/data/alk-forcing/OAE-Efficiency-Map")
files = sorted(indir.glob("alk-forcing-*-1999-01.nc"))

pat = re.compile(r"alk-forcing-(.+)\.(\d+)-1999-01\.nc$")

# --------------------------------------------------------
# Output file
# --------------------------------------------------------
outfile = "/global/cfs/projectdirs/m4746/Projects/OAE-Efficiency-Map/data/deficit-tracer-forcing/deficit_tracer_forcing_1999-01.nc"

# --------------------------------------------------------
# Create output skeleton using first file
# --------------------------------------------------------
print("Creating output skeleton:", outfile)

ds0 = xr.open_dataset(files[0])
# keep grid fields & KMT only
skeleton = ds0[["KMT"]]
skeleton.to_netcdf(outfile, mode="w")
ds0.close()

# --------------------------------------------------------
# Append each forcing field sequentially
# --------------------------------------------------------
print("Appending forcing fields...")

for f in files:
    m = pat.search(f.name)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {f.name}")

    basin = m.group(1)
    polygon = int(m.group(2))

    if (basin, polygon) not in polygon_master_map:
        raise ValueError(f"Unknown (basin, polygon): {(basin, polygon)}")

    # Create unified polygon index
    master_idx = polygon_master_map[(basin, polygon)]
    varname = f"deficit_tracer_forcing_{master_idx:03d}"

    print(f" → Writing {varname} from {f.name}")

    # Open lazily (doesn't load into RAM)
    ds = xr.open_dataset(f, chunks={})

    # Rename and write
    v = ds["alk_forcing"].rename(varname)
    encoding = {varname: {"zlib": True, "complevel": 4}}

    v.to_netcdf(outfile, mode="a", encoding=encoding)

    ds.close()

print("\nDONE.")
print("Output saved to:", outfile)
