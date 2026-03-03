import os
from subprocess import check_call
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

from dataclasses import dataclass
import click
import papermill as pm
from papermill.engines import NBClientEngine

import machine
import cesm
import analysis
from config import paths, project_sname, account, kernel_name

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
        #SBATCH --time=00:30:00
        #SBATCH --exclusive
        #SBATCH --constraint=cpu

        set -e
        
        module load python
        
        """
    )

    bundle_id = str(uuid.uuid4())
    n_this_bundle = n_bundle if len(cases) > n_bundle else len(cases)
    n_nodes = n_this_bundle * nodes_per_case
    script = [header(bundle_id, n_nodes)]

    submitted = []
    submit_batch = []
    for n, case in enumerate(cases):

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

    source /opt/cray/pe/cpe/24.07/restore_lmod_system_defaults.sh
    module purge
    module restore
    module load conda
    conda activate cworthy
    
    {cmd}
    
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
class BetaForcing:
    file: Path
    varname: str
    year_first: int
    year_last: int
    year_align: int

    @classmethod
    def from_dict(cls, data: dict):
        """Creates an instance from the 'beta_forcing' section of the YAML."""
        return cls(
            file=Path(data["file"]),
            varname=data["varname"],
            year_first=int(data["year_first"]),
            year_last=int(data["year_last"]),
            year_align=int(data["year_align"])
        )

    def to_namelist_dict(self):
        """Returns a dictionary suitable for injecting into a master table or row."""
        return {
            "beta_file": str(self.file),
            "beta_varname": self.varname,
            "beta_year_first": self.year_first,
            "beta_year_last": self.year_last,
            "beta_year_align": self.year_align,
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
        - 'experiments' : list[{"basin": str, "polygon": int, "forcing_file": str | Path}, "varname": str]
        - 'beta_forcing' : dict with keys "file", "varname", "year_align", "year_first", "year_last"
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
        Validate ANTITRACER configuration and initialize BetaForcing object.
        """
        # 1. Basic type check
        if not isinstance(self.antitracer_config, dict) or not self.antitracer_config:
            raise ValueError("antitracer_config must be a non-empty dictionary.")

        # 2. Check top-level required keys
        # Note: 'beta_forcing' is now a required top-level key
        required_keys = ["suffix", "date", "experiments", "beta_forcing"]
        for key in required_keys:
            if key not in self.antitracer_config:
                raise ValueError(
                    f"ANTITRACER config missing required key '{key}'. "
                    f"Required keys: {required_keys}"
                )

        # 3. Validate and initialize BetaForcing object
        try:
            self.beta_info = BetaForcing.from_dict(self.antitracer_config["beta_forcing"])
        except KeyError as e:
            raise KeyError(f"Missing parameter in 'beta_forcing' block: {e}")

        if not self.beta_info.file.exists():
            raise FileNotFoundError(f"beta_file not found: {self.beta_info.file}")

        # 4. Validate experiments list
        experiments = self.antitracer_config["experiments"]
        if not isinstance(experiments, list) or not experiments:
            raise ValueError("'experiments' must be a non-empty list.")

        for exp in experiments:
            if not isinstance(exp, dict):
                raise TypeError("Each experiment must be a dictionary.")
            
            required_exp = ["basin", "polygon", "forcing_file", "varname"]
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

            for exp in cfg["experiments"]:
                b = exp["basin"]
                p = exp["polygon"]
                d = cfg["date"]

                midx = polygon_master_map.get((b, p))
                if midx is None:
                    raise ValueError(f"No master index for basin={b}, polygon={p}")
                master_indices.append(midx)

                forcing_files.append(exp["forcing_file"])
                varnames.append(exp["varname"])

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
                case=case,
                simulation_key=simname,
                refdate=refdate,
                stop_n=nyear_case,
                #wallclock="48:00:00",
                wallclock="00:30:00",
                curtail_output=True,
            )

            # This adds beta_file, beta_varname, beta_year_first, etc. to the row
            row_data.update(self.beta_info.to_namelist_dict())

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

                # If it's an ANTITRACER run, add some more stuff to the arguments
                if caseinfo["cdr_forcing"] == "ANTITRACER":
                    # This adds beta_file, beta_varname, beta_year_first, etc. 
                    # as individual keys to the dictionary sent to atlas.py
                    build_kwargs.update(self.beta_info.to_namelist_dict())
                    build_kwargs["antitracer_master_indices"] = caseinfo["antitracer_master_indices"]
                    build_kwargs["cdr_forcing_varnames"] = caseinfo["cdr_forcing_varnames"]

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

    def analyze(self, clobber=False):
        """perform analysis and generate output datasets"""

        caselist = self.df_case_status.loc[
            (self.df_case_status.archive)
        ].index.to_list()

        caselist = list(
            filter(lambda c: self.df.loc[c].cdr_forcing is not None, caselist)
        )

        n = 50
        groups = list(itertools.zip_longest(*(iter(caselist),) * n))

        rows = []
        for group in groups:
            caselist_i = [i for i in group if i is not None]
            zarr_stores_exist = [
                os.path.exists(self.paths_case(case)["analyze"]) for case in caselist_i
            ]

            self.dask_cluster = None
            if not all(zarr_stores_exist) or clobber:
                self.dask_cluster = machine.dask_cluster()

            for case in tqdm(caselist_i):
                if "control" in case:
                    continue
                zarr_path = self._analyze_case(case, clobber)
                rows.append(dict(case=case, zarr_path=zarr_path))

            if self.dask_cluster is not None:
                self.dask_cluster.shutdown()
                self.dask_cluster = None

        self.df_analysis = pd.DataFrame(rows).set_index("case")

    def visualize(self, clobber=False):
        """run visualization notebooks"""

        self._refresh_case_status()

        caselist = self.df_case_status.loc[
            (self.df_case_status.archive)
        ].index.to_list()

        for case in caselist:

            caseinfo = self.df.loc[case].to_dict()
            caseinfo["case"] = case

            zarr_store = self.paths_case(case)["validate"]
            if os.path.exists(zarr_store):
                nb_out = f"{path_validation_nb_out}/{case}.ipynb"
                if not os.path.exists(nb_out) or clobber:
                    print(f"executing: {nb_out}")
                    pm.execute_notebook(
                        "_plot_case_validation.ipynb",
                        nb_out,
                        parameters=dict(zarr_store=zarr_store),
                        kernel_name="python3",
                        engine_name="md_jinja",
                        jinja_data=caseinfo,
                    )

            zarr_store = self.paths_case(case)["analyze"]
            if os.path.exists(zarr_store):
                nb_out = f"{path_analysis_nb_out}/{case}.ipynb"
                if not os.path.exists(nb_out) or clobber:
                    print(f"executing: {nb_out}")
                    pm.execute_notebook(
                        "_plot_case_analysis.ipynb",
                        nb_out,
                        parameters=dict(zarr_store=zarr_store),
                        kernel_name="python3",
                        engine_name="md_jinja",
                        jinja_data=caseinfo,
                    )

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

    def _validate_case(self, case, clobber=False, no_load=False):
        """compute validation dataset and persist as Zarr store"""

        zarr_store = self.paths_case(case)["validate"]
        if os.path.exists(zarr_store) and not clobber:
            if no_load:
                return
            else:
                return xr.open_zarr(zarr_store)

        else:
            caseinfo = self.df.loc[case]
            is_cdr_run = caseinfo["cdr_forcing"] is not None

            # this stuff should be on a case object or in a DataFrame
            variable_dict = dict()
            if is_cdr_run:
                variable_dict["DIC_ALT_CO2"] = "DIC"
                variable_dict["ALK_ALT_CO2"] = "ALK"
                variable_dict["ECOSYS_IFRAC"] = "ECOSYS_IFRAC"
                variable_dict["FG_ALT_CO2"] = "FG_CO2"
            else:
                variable_dict = {v: v for v in self._vars_to_replicate}

            # get case data files
            files = sorted(
                glob(
                    f"{archive_root}/{case}/ocn/hist/{case}.pop.h.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc"
                )
            )
            if not files:
                print(f"{case}: no files")
                return

            if is_cdr_run:
                time_case = self.time_cases[caseinfo["refdate"]]
            else:
                time_case = self.time_baseline

            len_time = len(time_case)
            assert (
                len(files) == len_time
            ), f"{case}:\n{len(files)} found -- expected {len_time}"

            # read the data
            chunk_spec = {"nlat": -1, "nlon": -1, "z_t": 60}
            ds = xr.open_mfdataset(
                files,
                decode_times=False,
                combine="by_coords",
                coords="minimal",
                data_vars="minimal",
                compat="override",
                drop_variables=[
                    "transport_regions",
                    "transport_components",
                    "moc_components",
                ],  # xarray can't merge these for some reason
                chunks=chunk_spec,
            )

            # maybe add some variables if this case has them
            if is_cdr_run:
                for v in self._vars_to_replicate:
                    if (
                        (v in ds)
                        and (v not in variable_dict.keys())
                        and (v not in variable_dict.values())
                    ):
                        variable_dict[v] = v

            # get the right period of time from the control
            ndx0 = np.where(time_case[0] == self.time_reference)[0].item()
            tndx = np.arange(ndx0, ndx0 + len(time_case), 1)

            # loop over variables and compute difference metrics
            ds_out = xr.Dataset()
            for v_case, v_ref in variable_dict.items():
                if v_case not in ds:
                    print(f"{v_case} not found", end=", ")
                    continue

                with xr.open_dataset(
                    self._path_reference_timeseries(v_ref),
                    decode_times=False,
                    chunks=chunk_spec,
                ) as ds_ref:
                    assert len(ds_ref.time) == len(
                        self.time_reference
                    ), "mismatch in control run time axis"

                    # pluck time segment
                    ds_ref = ds_ref.isel(time=tndx)

                    # identify correct coordinates
                    if "z_t" in ds_ref[v_ref].dims:
                        isel_timeseries = dict(z_t=0, nlat=0, nlon=0)
                        isel_slab = dict(z_t=0, time=-1)
                        sum_dims = ["z_t", "nlat", "nlon"]

                    elif "z_w_top" in ds_ref[v_ref].dims:
                        isel_timeseries = dict(z_w_top=9, nlat=0, nlon=0)
                        isel_slab = dict(z_w_top=9, time=-1)
                        sum_dims = ["z_w_top", "nlat", "nlon"]

                    elif "z_t_150m" in ds_ref[v_ref].dims:
                        isel_timeseries = dict(z_t_150m=0, nlat=0, nlon=0)
                        isel_slab = dict(z_t_150m=0, time=-1)
                        sum_dims = ["z_t_150m", "nlat", "nlon"]
                    else:
                        isel_timeseries = dict(nlat=0, nlon=0)
                        isel_slab = dict(time=-1)
                        sum_dims = ["nlat", "nlon"]

                    # initialize variables
                    n = ds[v_case].isel(time=0).notnull().sum()
                    ds_out[f"{v_case}_rmse"] = xr.full_like(
                        ds[v_case].isel(**isel_timeseries), fill_value=np.nan
                    )
                    ds_out[f"{v_case}_diff"] = xr.full_like(
                        ds[v_case].isel(**isel_slab), fill_value=np.nan
                    )

                    # compute metrics
                    with xr.set_options(arithmetic_join="exact"):
                        ds_out[f"{v_case}_rmse"].data = np.sqrt(
                            ((ds[v_case] - ds_ref[v_ref]) ** 2 / n).sum(sum_dims)
                        )
                        ds_out[f"{v_case}_diff"].data = (
                            ds[v_case] - ds_ref[v_ref]
                        ).isel(**isel_slab)
            try:
                ds_out = ds_out.compute()
                ds_out.to_zarr(
                    zarr_store,
                    mode="w",
                )                
            except:
                print(f"FAILED!\n{case}")
                
            return ds_out

    def _analyze_case(self, case, clobber=False):
        """compute validation dataset and persist as Zarr store"""

        zarr_store = self.paths_case(case)["analyze"]
        if os.path.exists(zarr_store) and not clobber:
            return zarr_store

        ds = analysis.open_gx1v7_dataset(case, stream="pop.h")
        ds_out = analysis.reduction(ds).compute()

        with warnings.catch_warnings(action="ignore"):
            ds_out.to_zarr(
                zarr_store,
                mode="w",
            )

        return zarr_store

    @property
    def _vars_to_replicate(self):
        return [
            "TEMP",
            "SALT",
            "UVEL",
            "VVEL",
            "WVEL",
            "PO4",
            "NO3",
            "SiO3",
            "NH4",
            "Fe",
            "Lig",
            "O2",
            "DIC",
            "DIC_ALT_CO2",
            "ALK",
            "ALK_ALT_CO2",
            "DOC",
            "DON",
            "DOP",
            "DOPr",
            "DONr",
            "DOCr",
            "zooC",
            "spChl",
            "spC",
            "spP",
            "spFe",
            "spCaCO3",
            "diatChl",
            "diatC",
            "diatP",
            "diatFe",
            "diatSi",
            "diazChl",
            "diazC",
            "diazP",
            "diazFe",
            "ECOSYS_IFRAC",
            "FG_ALT_CO2",
        ]


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
