import os
from subprocess import check_call
import subprocess
from glob import glob

import itertools
import uuid
import time
import warnings
from pathlib import Path
import yaml

from tqdm.notebook import tqdm


import json
import textwrap

from jinja2 import Template

import numpy as np
import pandas as pd
import xarray as xr

from dataclasses import dataclass, asdict
import click
import papermill as pm
from papermill.engines import NBClientEngine

import machine
import cesm
from config import paths, project_sname, account, kernel_name

PYTHON_MODULE = "python/3.11-24.1.0"

scriptroot = paths["workflow"]

path_validation_data = f"{paths['data']}/validation"
os.makedirs(path_validation_data, exist_ok=True)

path_analysis_data = f"{paths['data']}/analysis"
os.makedirs(path_analysis_data, exist_ok=True)

path_validation_nb_out = f"{scriptroot}/output/validation"
os.makedirs(path_validation_nb_out, exist_ok=True)

path_analysis_nb_out = f"{scriptroot}/output/analysis"
os.makedirs(path_analysis_nb_out, exist_ok=True)
archive_root = f"{paths['data']}/archive"


build_blueprint = {
    "smyle": cesm.create_smyle_clone,
    "hr_4p2z": cesm.create_hr_4p2z_clone,
}


def get_cftime(ds):
    """make a time axis that is the average of the time_bounds"""
    return xr.DataArray(
        cftime.num2date(
            ds[ds.time.attrs["bounds"]].mean("d2"),
            units=ds.time.units,
            calendar=ds.time.calendar,
        ),
    )

def verify_binary(path):
    """Checks the cesm.exe for 'not found' libraries and prints the full list."""
    exe = os.path.join(path, "bld", "cesm.exe")
    if not os.path.exists(exe):
        raise FileNotFoundError(f"Binary missing: {exe}")

    # Use the current environment (including any LD_LIBRARY_PATH we set in Python)
    result = subprocess.run(["ldd", exe], capture_output=True, text=True)
    
    print(f"\n--- Dependency Check for {os.path.basename(path)} ---")
    print(result.stdout)
    
    if "not found" in result.stdout:
        missing = [line for line in result.stdout.split('\n') if "not found" in line]
        print("❌ FATAL: Missing libraries detected!")
        for m in missing:
            print(f"  {m.strip()}")
        raise RuntimeError("Library resolution failed. Check your modules and LD_LIBRARY_PATH.")
    
    print("✅ All libraries linked correctly.")
    
def submit_bundle(cases, n_bundle=100, nodes_per_case=7, queue="regular"):
    """submit a bundle of cases"""

    submit_out_root = f"{scriptroot}/output/bundle-out"
    os.makedirs(submit_out_root, exist_ok=True)

    queue_job_root = f"{scriptroot}/output/bundled-jobs-caselists"
    os.makedirs(queue_job_root, exist_ok=True)

    print(queue)
    header = lambda jobname, n_nodes: textwrap.dedent(
        f"""\
        #!/bin/bash    
        #SBATCH --job-name bundle.{jobname}
        #SBATCH --account {account}
        #SBATCH --qos={queue}
        #SBATCH --nodes={n_nodes}
        #SBATCH --ntasks-per-node=128
        #SBATCH --time=48:00:00
        #SBATCH --exclusive
        #SBATCH --constraint=cpu

        set -e

        module load {PYTHON_MODULE}

        # 1. Neutralize the Cray "Leapfrog" effect
        # Stops the system from prepending 2026 paths automatically
        unset CRAY_LD_LIBRARY_PATH

        # 2. Brute-force the 2023.2 paths to the very front of the line
        export NETCDF_LIB="/opt/cray/pe/netcdf/4.9.0.13/intel/2023.2/lib"
        export PNETCDF_LIB="/opt/cray/pe/parallel-netcdf/1.12.3.13/intel/2023.2/lib"
        export INTEL_LIB="/opt/intel/oneapi/mkl/2023.2.0/lib/intel64"
        export COMPILER_LIB="/opt/intel/oneapi/compiler/2023.2.0/linux/compiler/lib/intel64"

        export LD_LIBRARY_PATH=$NETCDF_LIB:$PNETCDF_LIB:$INTEL_LIB:$COMPILER_LIB:$LD_LIBRARY_PATH

        # 3. Standard Stability Fixes
        ulimit -s unlimited
        export MKL_DEBUG_CPU_TYPE=5
        export FI_CXI_RX_MATCH_MODE=software
        export FI_MR_CACHE_MONITOR=memhooks

        echo "--- RUNTIME LINKER RESOLUTION ---"
        # We use a dummy check for the first case in the bundle to verify paths
        # This will show up in the slurm-.out file
        ldd {paths['scratch']}/{cases[0]}/bld/cesm.exe | grep -i netcdf
        echo "---------------------------------"        
        """
    )

    bundle_id = str(uuid.uuid4())
    n_this_bundle = n_bundle if len(cases) > n_bundle else len(cases)
    n_nodes = n_this_bundle * nodes_per_case
    script = [header(bundle_id, n_nodes)]

    submitted = []
    submit_batch = []
    for n, case in enumerate(cases):
        
        #verify_binary(f"{paths['scratch']}/{case}")
    
        # append to the script
        script.append(
            textwrap.dedent(
                f"""
                cd {paths['cases']}/{case}
                ./case.submit --no-batch &> {submit_out_root}/{case}.submit &
                """
            )
        )

        submit_batch.append(case)

        # write casename to file with jobname id so we can know which
        # cases are in this bundle by querying the queue
        with open(f"{queue_job_root}/{bundle_id}.caselist", "a") as fid:
            fid.write(f"{case}\n")

        if (len(script) - 1 == n_bundle) or n + 1 == len(cases):
            script.append("wait")

            bundle_submit = f"bundle.{bundle_id}.submit"
            with open(f"output/{bundle_submit}", "w") as fid:
                fid.writelines(script)

            # submit the bundle to the queue
            check_call(
                f"sbatch {bundle_submit} > {bundle_submit}.out",
                shell=True,
                cwd=f"{scriptroot}/output",
            )

            # reset to begin again
            submitted.extend(submit_batch)

            bundle_id = str(uuid.uuid4())
            remaining_cases = cases[n:]
            n_this_bundle = (
                n_bundle if len(remaining_cases) > n_bundle else len(remaining_cases)
            )
            n_nodes = n_this_bundle * nodes_per_case
            script = [header(bundle_id, n_nodes)]
            submit_batch = []

    assert len(submitted) == len(cases) and sorted(submitted) == sorted(
        cases
    ), "Not all cases were submitted"


def submit_cases(cases, n_simult=10):
    """submit N jobs at a time"""

    submit_out_root = f"{scriptroot}/output/submit-out"
    os.makedirs(submit_out_root, exist_ok=True)

    header = textwrap.dedent(
        """\
    #!/bin/bash
    set -e

    module load python

    i=0
    pids=()
    """
    )

    script = [header]
    submitted = []
    submit_batch = []
    for n, case in enumerate(cases):
        script.append(
            textwrap.dedent(
                f"""
            cd {paths['cases']}/{case}
            ./case.submit &> {submit_out_root}/{case}.submit &

            pids[${{i}}]=$!
            i=$(($i+1))

            """
            )
        )
        submit_batch.append(case)

        if (len(script) - 1 == n_simult) or n + 1 == len(cases):
            script.append(
                textwrap.dedent(
                    """        
            for pid in ${pids[*]}; do
                echo "waiting on ${pid}"
                wait $pid
            done
            """
                )
            )
            with open("output/batch.case.submit", "w") as fid:
                fid.writelines(script)

            submitted.extend(submit_batch)
            submit_batch = []
            check_call(
                "bash batch.case.submit > batch.case.submit.out",
                shell=True,
                cwd=f"{scriptroot}/output",
            )
            script = [header]

    assert len(submitted) == len(cases) and sorted(submitted) == sorted(
        cases
    ), "Not all cases were submitted"

def _build_script(blueprint, case, clobber=False, **kwargs):
    """generate a script to build the model"""

    json_data = json.dumps(kwargs)
    cmd = [
        "python",
        "atlas.py",
        "--blueprint",
        blueprint,
        "--case",
        case,
        "--clobber",
        clobber,
        "--kwargs",
        f"'{json_data}'",
    ]

    cmd = " ".join([str(s) for s in cmd])

    header = textwrap.dedent(
        f"""\
    #!/bin/bash
    #SBATCH -J build.{case}
    #SBATCH -A {account}
    #SBATCH -e {scriptroot}/output/build-out/{case}-%J.out
    #SBATCH -o {scriptroot}/output/build-out/{case}-%J.out
    #SBATCH --time=01:00:00
    #SBATCH --mem=12GB
    #SBATCH --qos=shared
    #SBATCH --nodes=1
    #SBATCH --ntasks=1
    #SBATCH --constraint=cpu

    module purge
    module restore
    module load conda
    conda activate cworthy

    # 1. Hard-pin the MKL library directory (Fixes the "not found" errors)
    export LD_LIBRARY_PATH=/opt/intel/oneapi/mkl/2023.2.0/lib/intel64:$LD_LIBRARY_PATH
    
    # 2. Re-assert the Math accuracy fix (The evp-patch replacement)
    export MKL_DEBUG_CPU_TYPE=5
    
    # 3. Extra Safety: Ensure the compiler's own internal libs are visible
    export LD_LIBRARY_PATH=/opt/intel/oneapi/compiler/2023.2.0/linux/compiler/lib/intel64:$LD_LIBRARY_PATH

    export NETCDF_PATH=/opt/cray/pe/netcdf/4.9.0.13/intel/2023.2
    export PNETCDF_PATH=/opt/cray/pe/parallel-netcdf/1.12.3.13/intel/2023.2    
    export LD_LIBRARY_PATH=$NETCDF_PATH/lib:$PNETCDF_PATH/lib:$LD_LIBRARY_PATH
    
    echo "--- FULL LOADED MODULE LIST ---"
    module list
    echo "----------------------------------"

    echo "--- CRAY ENVIRONMENT AUDIT ---"
    echo "CRAY_LD_LIBRARY_PATH is: $CRAY_LD_LIBRARY_PATH"
    module show cray-netcdf 2>&1 | grep -i prefix
    echo "------------------------------"
    
    {cmd}

    echo "--- POST-BUILD LINKER CHECK ---"
    # This checks exactly which NetCDF the binary is grabbing
    ldd "{paths['scratch']}/{case}/bld/cesm.exe" | grep -i netcdf
    
    echo "--- POST-BUILD SAFETY CHECK ---"
    EXE_PATH="{paths['scratch']}/{case}/bld/cesm.exe"
    
    if [ ! -f "$EXE_PATH" ]; then
        echo "❌ ERROR: Build failed to produce an executable at $EXE_PATH."
        exit 1
    fi

    echo "🔍 FULL RUNTIME DEPENDENCY LIST (ldd):"
    echo "------------------------------------------------------------"
    ldd "$EXE_PATH"
    echo "------------------------------------------------------------"

    if ldd "$EXE_PATH" | grep -q "not found"; then
        echo "❌ FATAL: Missing dependencies detected above!"
        exit 1
    else
        echo "✅ Binary verified. All shared libraries resolved."
    fi
    # ---------------------------------------
    """
    )
    
    build_script = f"{scriptroot}/output/build-in/{case}.build"
    with open(build_script, "w") as fid:
        fid.write(header)

    return build_script


def submit_build(blueprint, case, clobber=False, run_local=False, **kwargs):
    """build a case via submitting a job to the queue"""
    build_script = _build_script(blueprint, case, clobber, **kwargs)
    if run_local:
        check_call(["bash", build_script])
    else:
        check_call(["sbatch", build_script])


class md_jinja_engine(NBClientEngine):
    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name, **kwargs):
        jinja_data = {} if "jinja_data" not in kwargs else kwargs["jinja_data"]

        # call the papermill execution engine:
        super().execute_managed_notebook(nb_man, kernel_name, **kwargs)

        for cell in nb_man.nb.cells:
            if cell.cell_type == "markdown":
                cell["source"] = Template(cell["source"]).render(**jinja_data)


# what's the right way to register an engine?
pm.engines.papermill_engines._engines["md_jinja"] = md_jinja_engine

@dataclass
class ForcingField:
    """Generic forcing field descriptor (beta, eta, or other sensitivity fields)."""
    file: Path
    varname: str
    year_first: int
    year_last: int
    year_align: int
    tintalgo: str = "linear"
    taxMode: str = "cycle"

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            file=Path(data["file"]),
            varname=data["varname"],
            year_first=int(data["year_first"]),
            year_last=int(data["year_last"]),
            year_align=int(data["year_align"]),
            tintalgo=data.get("tintalgo", "linear"),
            taxMode=data.get("taxMode", "cycle"),
        )

    def to_dict(self):
        """Convert to a JSON-serializable dictionary."""
        out = asdict(self)
        out["file"] = str(self.file)
        return out

    def to_namelist_dict(self, prefix: str = "beta_"):
        """Returns a flat dictionary with keys prefixed by `prefix`."""
        return {
            f"{prefix}file": str(self.file),
            f"{prefix}varname": self.varname,
            f"{prefix}year_first": self.year_first,
            f"{prefix}year_last": self.year_last,
            f"{prefix}year_align": self.year_align,
            f"{prefix}tintalgo": self.tintalgo,
            f"{prefix}taxMode": self.taxMode,
        }
        
class global_irf_map:
    """
    Build and manage IRF (Impulse Response Function) simulation metadata
    for global-scale CDR experiments (OAE, ERW, DOR, ANTITRACER).

    Parameters
    ----------
    cdr_forcing : str
        Type of CDR forcing: "OAE", "ERW", "DOR", "ANTITRACER", etc.
    vintage : str
        CESM/SMYLE experiment vintage tag.
    antitracer_config : dict | str | Path, optional
        ANTITRACER configuration. Must contain the keys:
        - 'suffix' : str
        - 'date'   : str
        - 'experiments' : list[{"basin": str, "polygon": int, "forcing_file": str | Path, "varname": str, "is_alk": bool}]
        - 'beta_forcing' : dict with keys "file", "varname", "year_align", "year_first", "year_last"
        - 'eta_forcing'  : dict with keys "file", "varname", "year_align", "year_first", "year_last"
    """

    def __init__(
        self,
        cdr_forcing: str,
        vintage: str,
        antitracer_config: dict | str | Path | None = None,
    ) -> None:

        # -------------------------
        # Basic metadata
        # -------------------------
        self.blueprint = "smyle"
        self.simulation_name = f"glb-{cdr_forcing.lower()}"
        self.cdr_forcing = cdr_forcing.upper()
        self.vintage = vintage

        # -------------------------
        # Handle ANTITRACER config
        # -------------------------
        if isinstance(antitracer_config, (str, Path)):
            # YAML → dict
            with open(antitracer_config, "r") as f:
                self.antitracer_config: dict | None = yaml.safe_load(f)
        else:
            self.antitracer_config = antitracer_config

        if self.cdr_forcing == "ANTITRACER":
            self._validate_antitracer_config()
        # -------------------------
        # Reference information
        # -------------------------
        self.reference_case = "g.e22.GOMIPECOIAF_JRA-1p4-2018.TL319_g17.SMYLE.005"

        self.time_reference = xr.cftime_range(
            "0306-01-01", "0368-12-31", freq="ME", calendar="noleap"
        )

        self._df_case_status = None
        self.df_validation = None
        self.df_analysis = None

        # Build case table
        self.set_experiments()

    # -------------------------------------------------------------------------
    # VALIDATION HELPERS
    # -------------------------------------------------------------------------
    def _validate_antitracer_config(self) -> None:
        """
        Validate ANTITRACER configuration and initialize ForcingField objects.
        """
        # 1. Basic type check
        if not isinstance(self.antitracer_config, dict) or not self.antitracer_config:
            raise ValueError("antitracer_config must be a non-empty dictionary.")

        # 2. Check top-level required keys
        required_keys = ["suffix", "date", "experiments", "beta_forcing", "eta_forcing"]
        for key in required_keys:
            if key not in self.antitracer_config:
                raise ValueError(
                    f"ANTITRACER config missing required key '{key}'. "
                    f"Required keys: {required_keys}"
                )

        # 3. Validate and initialize ForcingField objects
        try:
            self.beta_info = ForcingField.from_dict(self.antitracer_config["beta_forcing"])
        except KeyError as e:
            raise KeyError(f"Missing parameter in 'beta_forcing' block: {e}")

        if not self.beta_info.file.exists():
            raise FileNotFoundError(f"beta_file not found: {self.beta_info.file}")

        try:
            self.eta_info = ForcingField.from_dict(self.antitracer_config["eta_forcing"])
        except KeyError as e:
            raise KeyError(f"Missing parameter in 'eta_forcing' block: {e}")

        if not self.eta_info.file.exists():
            raise FileNotFoundError(f"eta_file not found: {self.eta_info.file}")

        # 4. Validate experiments list
        experiments = self.antitracer_config["experiments"]
        if not isinstance(experiments, list) or not experiments:
            raise ValueError("'experiments' must be a non-empty list.")

        for exp in experiments:
            if not isinstance(exp, dict):
                raise TypeError("Each experiment must be a dictionary.")

            required_exp = ["basin", "polygon", "forcing_file", "varname", "is_alk"]
            for k in required_exp:
                if k not in exp:
                    raise ValueError(
                        f"Each experiment must contain {required_exp}, missing '{k}'."
                    )
                    
    # -------------------------------------------------------------------------
    # MAIN TABLE BUILDER
    # -------------------------------------------------------------------------

    def set_experiments(self) -> None:
        """
        Build the master experiment table (`self.df`) describing all CDR simulations.

        Handles:
        - Non-ANTITRACER case: OAE, ERW, DOR
        - ANTITRACER grouped multi-polygon tracer release
        - Baseline reference case
        """

        # -------------------------
        # Static configuration
        # -------------------------
        basins = [
            "North_Atlantic_basin",
            "North_Pacific_basin",
            "South",
            "Southern_Ocean",
        ]

        npolygon = dict(
            North_Atlantic_basin=150,
            North_Pacific_basin=200,
            South=300,
            Southern_Ocean=40,
        )

        coastal_polygons = dict(
            North_Atlantic_basin=list(range(90)),
            North_Pacific_basin=list(range(100)),
            South=list(range(120)),
            Southern_Ocean=[],
        )

        start_dates = ["1999-01", "1999-04", "1999-07", "1999-10"]
        ref_dates = ["0347-01-01", "0347-04-01", "0347-07-01", "0347-10-01"]

        cdr_forcing_root = (
            "/global/cfs/projectdirs/m4746/Projects/OAE-Efficiency-Map/data/"
            "alk-forcing/OAE-Efficiency-Map"
        )

        def generic_file(b, p, d):
            return f"{cdr_forcing_root}/alk-forcing-{b}.{p:03d}-{d}.nc"

        # -------------------------
        # Time axes
        # -------------------------
        #nyear_case = 15
        nyear_case = 1
        nyear_baseline = 16
        
        periods_case = nyear_case * 12
        periods_baseline = nyear_baseline * 12

        self.time_cases = {
            k: xr.cftime_range(k, periods=periods_case, freq="ME", calendar="noleap")
            for k in ref_dates
        }

        self.time_baseline = xr.cftime_range(
            ref_dates[0], periods=periods_baseline, freq="ME", calendar="noleap"
        )

        # -------------------------
        # Build polygon master index
        # -------------------------
        polygon_master_map = {}
        idx = -1
        for b in basins:
            for p in range(npolygon[b]):
                idx += 1
                polygon_master_map[(b, p)] = idx

        # -------------------------
        # Begin table rows
        # -------------------------
        rows = []

        # ---------------------------------------------------------------------
        # BASELINE CASE
        # ---------------------------------------------------------------------
        if self.cdr_forcing != "ANTITRACER":
            rows.append(
                dict(
                    blueprint=self.blueprint,
                    polygon=None,
                    polygon_master=None,
                    basin=None,
                    start_date=start_dates[0],
                    cdr_forcing=None,
                    cdr_forcing_files=None,
                    cdr_forcing_varnames=None,
                    case=f"{self.blueprint}.{project_sname}.control.{self.vintage}",
                    simulation_key="baseline",
                    refdate=ref_dates[0],
                    stop_n=nyear_baseline,
                    wallclock="48:00:00",
                    curtail_output=False,
                )
            )

        # ---------------------------------------------------------------------
        # ANTITRACER GROUPED RELEASE
        # ---------------------------------------------------------------------
        if self.cdr_forcing == "ANTITRACER":

            cfg = self.antitracer_config

            master_indices = []
            forcing_files = []
            varnames = []
            is_alk_list = []
            coupled_alk_idx_list = []

            # Build lookup: (basin, polygon) -> 1-based tracer index for is_alk=True tracers,
            # so that paired DIC tracers can reference their ALK partner automatically.
            alk_idx_by_location = {}
            for i, exp in enumerate(cfg["experiments"]):
                if exp["is_alk"]:
                    alk_idx_by_location[(exp["basin"], exp["polygon"])] = i + 1

            for exp in cfg["experiments"]:
                b = exp["basin"]
                p = exp["polygon"]
                d = cfg["date"]

                midx = polygon_master_map.get((b, p))
                if midx is None:
                    raise ValueError(f"No master index for basin={b}, polygon={p}")
                master_indices.append(midx)

                forcing_files.append(exp["forcing_file"] if exp["forcing_file"] is not None else "unknown")
                varnames.append(exp["varname"] if exp["varname"] is not None else "unknown")
                is_alk_list.append(exp["is_alk"])

                if not exp["is_alk"]:
                    coupled_alk_idx_list.append(alk_idx_by_location.get((b, p), 0))
                else:
                    coupled_alk_idx_list.append(0)

            # Simulation name
            simname = f"{self.simulation_name}_{cfg['date']}_{cfg['suffix']}"
            case = f"{self.blueprint}.{project_sname}.{simname}.{self.vintage}"

            try:
                i = start_dates.index(cfg["date"])
                refdate = ref_dates[i]
            except ValueError:
                raise ValueError(f"Invalid ANTITRACER start_date {cfg['date']}")

            # Define the base row dictionary
            row_data = dict(
                blueprint=self.blueprint,
                polygon=None,
                basin=None,
                start_date=cfg["date"],
                cdr_forcing=self.cdr_forcing,
                cdr_forcing_files=forcing_files,
                cdr_forcing_varnames=varnames,
                antitracer_master_indices=master_indices,
                antitracer_is_alk_list=is_alk_list,
                antitracer_coupled_alk_idx_list=coupled_alk_idx_list,
                antitracer_year_first=cfg.get("antitracer_year_first", 1999),
                antitracer_year_last=cfg.get("antitracer_year_last", 2019),
                antitracer_year_align=cfg.get("antitracer_year_align", 347),
                antitracer_tintalgo=cfg.get("antitracer_tintalgo", "linear"),
                antitracer_taxMode=cfg.get("antitracer_taxMode", "cycle"),
                case=case,
                simulation_key=simname,
                refdate=refdate,
                stop_n=nyear_case,
                wallclock="48:00:00",
                #wallclock="00:30:00",
                curtail_output=True,
            )

            # Flatten beta and eta forcing fields into the row
            row_data.update(self.beta_info.to_namelist_dict("beta_"))
            row_data.update(self.eta_info.to_namelist_dict("eta_"))

            rows.append(row_data)

        # ---------------------------------------------------------------------
        # REGULAR SINGLE-POLYGON FORCING (OAE/ERW/DOR)
        # ---------------------------------------------------------------------
        else:
            index = 0
            for b in basins:
                for p in range(npolygon[b]):

                    # ERW excludes non-coastal polygons
                    if self.cdr_forcing == "ERW" and p not in coastal_polygons[b]:
                        continue

                    master_index = polygon_master_map[(b, p)]

                    for i, d in enumerate(start_dates):
                        fpath = generic_file(b, p, d)
                        if not os.path.exists(fpath):
                            raise FileNotFoundError(fpath)

                        loc = f"{b}_{p:03d}_{d}-01"
                        simname = f"{self.simulation_name}_{loc}_{index:05d}"
                        case = f"{self.blueprint}.{project_sname}.{simname}.{self.vintage}"

                        rows.append(
                            dict(
                                blueprint=self.blueprint,
                                polygon=p,
                                polygon_master=master_index,
                                basin=b,
                                start_date=d,
                                cdr_forcing=self.cdr_forcing,
                                cdr_forcing_files=[fpath],
                                beta_file=None,
                                case=case,
                                simulation_key=simname,
                                refdate=ref_dates[i],
                                stop_n=nyear_case,
                                wallclock="10:00:00",
                                curtail_output=True,
                            )
                        )
                        index += 1

        # -------------------------
        # Finalize DataFrame
        # -------------------------
        self.df = pd.DataFrame(rows).set_index("case")
        self.cases = list(self.df.index)

    def build(
        self,
        phase,
        run_local=False,
        clobber=False,
        clobber_list=[],
        just_these_cases=[],
        queue="regular",
    ):
        """build cases in SLURM script"""

        building_jobs = machine.building_jobids()
        if building_jobs:
            print(f"waiting on {len(building_jobs)} build(s)")

        while building_jobs:
            building_jobs = machine.building_jobids()
            print("...", end="")
            time.sleep(30)

        # build a subset or all
        if phase == "reproduce-reference":
            df_build = self.df.loc[self.cases[0] : self.cases[0]]

        elif phase == "test":
            df_build = self.df.iloc[1:2]
        elif phase == "deploy":
            df_build = self.df.iloc[:]

        else:
            raise ValueError("phase unrecognized")

        self._refresh_case_status()
        df_case_status = self.df_case_status

        for case, caseinfo in df_build.iterrows():

            if just_these_cases:
                if case not in just_these_cases:
                    continue

            built = False
            if df_case_status is not None:
                if case in df_case_status.index:
                    if clobber or case in clobber_list:
                        self.clobber_case(case)
                    else:
                        built = df_case_status.loc[case].build

            if not built:
                build_kwargs = {
                    "blueprint": caseinfo["blueprint"],
                    "case": case,
                    "cdr_forcing": caseinfo["cdr_forcing"],
                    "cdr_forcing_files": caseinfo["cdr_forcing_files"],
                    "refdate": caseinfo["refdate"],
                    "stop_n": caseinfo["stop_n"],
                    "wallclock": caseinfo["wallclock"],
                    "curtail_output": caseinfo["curtail_output"],
                    "queue": queue,
                    "clobber": clobber,
                    "run_local": run_local,
                }

                # If it's an ANTITRACER run, add beta/eta forcing and tracer metadata
                if caseinfo["cdr_forcing"] == "ANTITRACER":
                    build_kwargs.update(self.beta_info.to_namelist_dict("beta_"))
                    build_kwargs.update(self.eta_info.to_namelist_dict("eta_"))
                    build_kwargs["antitracer_master_indices"] = caseinfo["antitracer_master_indices"]
                    build_kwargs["cdr_forcing_varnames"] = caseinfo["cdr_forcing_varnames"]
                    build_kwargs["antitracer_year_first"] = caseinfo["antitracer_year_first"]
                    build_kwargs["antitracer_year_last"] = caseinfo["antitracer_year_last"]
                    build_kwargs["antitracer_year_align"] = caseinfo["antitracer_year_align"]
                    build_kwargs["antitracer_tintalgo"] = caseinfo["antitracer_tintalgo"]
                    build_kwargs["antitracer_taxMode"] = caseinfo["antitracer_taxMode"]
                    build_kwargs["antitracer_is_alk_list"] = caseinfo["antitracer_is_alk_list"]
                    build_kwargs["antitracer_coupled_alk_idx_list"] = caseinfo["antitracer_coupled_alk_idx_list"]

                # Call submit_build using the complete dictionary of arguments
                submit_build(**build_kwargs)

    def compute(self, n_bundle=0, bundle_queue="regular", just_these_cases=[]):
        """perform the computation"""

        building_jobs = machine.building_jobids()
        if building_jobs:
            print(f"waiting on {len(building_jobs)} build(s)")

        
        while building_jobs:
            building_jobs = machine.building_jobids()
            print("...", end="")
            time.sleep(30)

        self._refresh_case_status()

        caselist = self.df_case_status.loc[
            (self.df_case_status.build)
            #& ~(self.df_case_status.archive)
            & ~(self.df_case_status.Queued)
        ].index.to_list()

        if just_these_cases:
            for case in just_these_cases:
                assert case in caselist, f"{case} is not in the list"
            caselist = just_these_cases

        if n_bundle == 0:
            submit_cases(caselist)
        else:
            submit_bundle(
                caselist, n_bundle=n_bundle, nodes_per_case=7, queue=bundle_queue
            )

        return len(caselist)

    def check_cases(self):
        """identify cases in pathological state"""

        if self.df_case_status is None:
            return []

        caselist = self.df_case_status.loc[
            (self.df_case_status.build)
            & (self.df_case_status.submitted)
            & ~(self.df_case_status.run_completed)
            & ~(self.df_case_status.Queued)
        ].index.to_list()

        for case in self.df_case_status.loc[self.df_case_status.archive].index:
            # get case data files
            files = sorted(
                glob(
                    f"{archive_root}/{case}/ocn/hist/{case}.pop.h.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc"
                )
            )
            if not files:
                if case not in caselist:
                    caselist.append(case)

        if caselist:
            print("the following cases may have failed:")
            for case in caselist:
                print(f"  {case}")
        return caselist

    def clobber_case(self, case):
        """remove all case data from disk"""
        for key, path in self.paths_case(case).items():
            check_call(["rm", "-fr", path])

    def validate(self, clobber=False, n=None):
        """validate the model integrations"""

        self._refresh_case_status()

        caselist = self.df_case_status.loc[
            (self.df_case_status.archive)
        ].index.to_list()

        if n is not None:
            caselist = caselist[:n]

        zarr_stores_exist = [
            os.path.exists(self.paths_case(case)["validate"]) for case in caselist
        ]

        self.dask_cluster = None
        if not all(zarr_stores_exist) or clobber:
            self.dask_cluster = machine.dask_cluster(wallclock="06:00:00")

        for case in tqdm(caselist):
            ds_out = self._validate_case(case, clobber, no_load=True)

        rows = []
        for case in tqdm(caselist):
            ds_out = self._validate_case(case, clobber)

            if ds_out is None:
                continue

            is_cdr_run = self.df.loc[case]["cdr_forcing"] is not None
            row_data = dict(case=case, is_cdr_run=is_cdr_run)
            for v in ds_out.variables:
                if v[-5:] == "_rmse":
                    rmse_max = ds_out[v].max().values.item()
                    row_data[v] = rmse_max

            rows.append(row_data)

        self.df_validation = pd.DataFrame(rows).set_index("case")

        if self.dask_cluster is not None:
            self.dask_cluster.shutdown()
            self.dask_cluster = None

    @property
    def df_case_status(self):
        """
        Return DataFrame with case status info
        """
        if self._df_case_status is None:
            self._refresh_case_status()
        return self._df_case_status

    def _refresh_case_status(self):
        """
        Populate case status DataFrame
        """
        self._df_case_status = cesm.case_status(
            self.vintage, caselist=self.df.index.to_list()
        )

    def _path_reference_timeseries(self, variable):
        """
        return path to timeseries data — replace with data catalog API
        """
        # set root path
        fpath_smyle = (
            "/global/cfs/projectdirs/m4746/Datasets/SMYLE-FOSI/ocn/proc/tseries/month_1"
        )
        # open control dataset
        stream = "pop.h"
        datestr = "030601-036812"
        file = f"{fpath_smyle}/{self.reference_case}.{stream}.{variable}.{datestr}.nc"
        assert os.path.exists(file)
        return file

    def paths_case(self, case):
        return dict(
            build=f"{paths['cases']}/{case}",
            compute=f"{paths['scratch']}/{case}",
            archive=f"{paths['data']}/archive/{case}",
            validate=f"{path_validation_data}/{case}.validation.zarr",
            analyze=f"{path_analysis_data}/{case}.analysis.zarr",
        )


@click.command()
@click.option("--blueprint", required=True)
@click.option("--case", required=True)
@click.option("--kwargs", required=True)
@click.option("--clobber", type=click.BOOL, default=False)
def main(blueprint, case, kwargs, clobber):

    print("=" * 80)
    print("BUILDING")
    print(blueprint)
    print(case)
    print(kwargs)
    print("=" * 80)
    print()

    assert blueprint in build_blueprint, f"Undefined blueprint {blueprint}"

    try:
        kwargs = json.loads(kwargs)
    except json.JSONDecodeError:
        click.echo("Invalid dictionary format. Please pass valid JSON.")

    gen_case = build_blueprint[blueprint]
    gen_case(case, clobber=clobber, **kwargs)


if __name__ == "__main__":
    main()
