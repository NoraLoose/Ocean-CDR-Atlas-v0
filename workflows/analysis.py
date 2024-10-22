import os
from glob import glob

import cftime
import xarray as xr

import pop_tools

from config import paths, machine_name, project_name


nmolcm2s_to_molm2yr = 1.0e-9 * 1.0e4 * 86400.0 * 365.0
nmolcm2_to_molm2 = 1.0e-9 * 1.0e4


def _list_hist_files(case, stream):
    """list history files"""
    archive_root = f"{paths['data']}/archive/{case}"
    if stream == "pop.h":
        subdir = "ocn/hist"
        datestr_glob = "????-??"
    elif stream == "pop.h.ecosys.nday1":
        subdir = "ocn/hist"
        datestr_glob = "????-??-??"
        rename_underscore2_vars = True
    else:
        raise ValueError(f"access to stream: '{stream}' not defined")

    glob_str = f"{archive_root}/{subdir}/{case}.{stream}.{datestr_glob}.nc"
    files = sorted(glob(glob_str))
    assert files, f"no files found.\nglob string: {glob_str}"
    return files


def open_gx1v7_dataset(case, stream="pop.h"):
    """access data from a case"""
    grid = pop_tools.get_grid("POP_gx1v7")
    files = _list_hist_files(case, stream)

    rename_underscore2_vars = True if stream == "pop.h.ecosys.nday1" else False

    def preprocess(ds):
        return ds.set_coords(["KMT", "TAREA"]).reset_coords(
            ["ULONG", "ULAT"], drop=True
        )

    ds = xr.open_mfdataset(
        files,
        coords="minimal",
        combine="by_coords",
        compat="override",
        data_vars="minimal",
        preprocess=preprocess,
        decode_times=False,
        chunks={"time": 1},
    )

    # fix time
    tb_var = ds.time.attrs["bounds"]
    time_units = ds.time.units
    calendar = ds.time.calendar

    ds["time"] = cftime.num2date(
        ds[tb_var].mean("d2"),
        units=time_units,
        calendar=calendar,
    )
    ds.time.encoding.update(
        dict(
            calendar=calendar,
            units=time_units,
        )
    )

    # add ∆t
    d2 = ds[tb_var].dims[-1]
    ds["time_delta"] = ds[tb_var].diff(d2).squeeze()
    ds = ds.set_coords("time_delta")
    ds.time_delta.attrs["long_name"] = "∆t"
    ds.time_delta.attrs["units"] = "days"    

    # replace coords to account for land-block elimination
    ds["TLONG"] = grid.TLONG
    ds["TLAT"] = grid.TLAT
    ds["KMT"] = ds.KMT.fillna(0)

    # add area field
    ds["area_m2"] = ds.TAREA * 1e-4
    ds.area_m2.encoding = dict(**ds.TAREA.encoding)
    ds.area_m2.attrs = dict(**ds.TAREA.attrs)
    ds.area_m2.attrs["units"] = "m^2"

    if rename_underscore2_vars:
        rename_dict = dict()
        for v in ds.data_vars:
            if v[-2:] == "_2":
                rename_dict[v] = v[:-2]
        ds = ds.rename(rename_dict)

    return ds


def reduction(ds):
    
    # time_delta in days, convert to years
    dt = ds.time_delta / 365
    
    with xr.set_options(keep_attrs=True):
        
        # air-sea CO2 flux
        fg_co2_add = (-1.0) * (ds.FG_CO2 - ds.FG_ALT_CO2).where(ds.KMT > 0)
        fg_co2_add *= nmolcm2s_to_molm2yr
        fg_co2_add.attrs["units"] = "mol m$^{-2}$ yr$^{-1}$"
        
        alk_flux = (compute_global_ts(ds.ALK_FLUX * nmolcm2s_to_molm2yr, ds.area_m2) * dt).cumsum("time")
        alk_flux.attrs["units"] = "mol"
        
        dic_flux = (compute_global_ts(ds.DIC_FLUX * nmolcm2s_to_molm2yr, ds.area_m2) * dt).cumsum("time")        
        dic_flux.attrs["units"] = "mol"        

        alk_flux_total = (ds.ALK_FLUX * nmolcm2s_to_molm2yr * dt).sum("time").where(ds.KMT > 0)
        alk_flux_total.attrs["units"] = "mol m$^{-2}$"
        
        dic_flux_total = (ds.DIC_FLUX * nmolcm2s_to_molm2yr * dt).sum("time").where(ds.KMT > 0)
        dic_flux_total.attrs["units"] = "mol m$^{-2}$"
        
        dic_add = (-1.0) * (compute_global_ts(fg_co2_add, ds.area_m2) * dt).cumsum("time")
        dic_add.attrs["long_name"] = "Change in DIC inventory"
        dic_add.attrs["units"] = dic_add.attrs["units"].replace("yr$^{-1}$", "").strip()

        dic_surf = ds.DIC.isel(z_t=0)
        alk_surf = ds.ALK.isel(z_t=0)        
        dic_add_surf = ds.DIC.isel(z_t=0) - ds.DIC_ALT_CO2.isel(z_t=0)
        alk_add_surf = ds.ALK.isel(z_t=0) - ds.ALK_ALT_CO2.isel(z_t=0)
        
        pH_add_surf = ds.PH - ds.PH_ALT_CO2
        pco2_add_surf = ds.pCO2SURF - ds.pCO2SURF_ALT_CO2
        
    dso = xr.Dataset(
        dict(
            AREA_M2=ds.area_m2,
            FG_CO2_ADD=fg_co2_add,
            DIC_ADD_TOTAL=dic_add,
            ALK_FLUX=alk_flux,
            DIC_FLUX=dic_flux,
            ALK_FLUX_TOTAL=alk_flux_total,
            DIC_FLUX_TOTAL=dic_flux_total,
            DIC_SURF=dic_surf,
            ALK_SURF=alk_surf,
            DIC_ADD_SURF=dic_add_surf,
            ALK_ADD_SURF=alk_add_surf,
            PH_ADD_SURF=pH_add_surf,
            pCO2_ADD_SURF=pco2_add_surf
        )
    ).drop(["TAREA", "z_t"]).set_coords(["AREA_M2"])
    dso.attrs["case"] = ds.title
    return dso
    
def compute_additional_CO2_flux(ds):
    """compute the additional CO2 flux"""
    with xr.set_options(keep_attrs=True):
        flux_effect = (-1.0) * (ds.FG_CO2 - ds.FG_ALT_CO2).where(ds.KMT > 0)
        flux_effect *= nmolcm2s_to_molm2yr
        flux_effect.attrs["units"] = "mol m$^{-2}$ yr$^{-1}$"
        flux_effect.attrs["sign_convention"] = "postive up"
        flux_effect["area_m2"] = ds.TAREA * 1e-4
    return flux_effect


def compute_time_cumulative_integral(da, convert_time=1.0):
    """integrate a DataArray in time"""
    with xr.set_options(keep_attrs=True):
        dao = da.weighted(da.time_delta * convert_time).sum("time")
    dao.attrs["units"] = dao.attrs["units"].replace("yr$^{-1}$", "")
    return dao


def compute_global_ts(da, area_m2):
    """integrate DataArray globally"""
    with xr.set_options(keep_attrs=True):
        dao = (da * area_m2).sum(["nlat", "nlon"])
    dao.attrs["units"] = dao.attrs["units"].replace("m$^{-2}$", "")
    return dao


def compute_additional_DIC_global_ts(ds):
    """return the globally-integrated, time-integrated flux"""

    add_co2_ts = compute_global_ts(compute_additional_CO2_flux(ds))

    # compute cumulative integral in time
    dt = add_co2_ts.time_delta / 365  # time_delta in days, convert to years
    with xr.set_options(keep_attrs=True):
        dao = (-1.0) * (add_co2_ts * dt).cumsum("time")
    dao.attrs["long_name"] = "Change in DIC inventory"
    dao.attrs["units"] = dao.attrs["units"].replace("yr$^{-1}$", "").strip()
