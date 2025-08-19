import os
from subprocess import check_call
from glob import glob

import itertools
import uuid
import time
import warnings

from tqdm.notebook import tqdm


import json
import textwrap

from jinja2 import Template

import numpy as np
import pandas as pd
import xarray as xr

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


class global_irf_map(object):

    def __init__(self, cdr_forcing, vintage, antitracer_config=None):
        # simulation details

        self.blueprint = "smyle"
        self.simulation_name = f"glb-{cdr_forcing.lower()}"
        self.cdr_forcing = cdr_forcing
        self.vintage = vintage
        self.antitracer_config = antitracer_config

        if self.cdr_forcing == "ANTITRACER":
            if not isinstance(self.antitracer_config, dict) or not self.antitracer_config:
                raise ValueError("When cdr_forcing is 'ANTITRACER', 'antitracer_config' must be a single, non-empty dictionary.")

            # Validate the required top-level keys for the single dictionary config
            if "suffix" not in self.antitracer_config or \
               "date" not in self.antitracer_config or \
               "experiments" not in self.antitracer_config:
               raise ValueError("The 'antitracer_config' dictionary must contain 'suffix', 'date', and 'experiments' keys.")

            # Validate the 'experiments' list itself
            if not isinstance(self.antitracer_config["experiments"], list) or \
               not self.antitracer_config["experiments"]:
               raise ValueError("The 'experiments' value in 'antitracer_config' must be a non-empty list of experiment dictionaries.")

            # Validate each experiment dictionary within the 'experiments' list
            for exp_dict in self.antitracer_config["experiments"]:
                if not isinstance(exp_dict, dict) or \
                   "basin" not in exp_dict or \
                   "polygon" not in exp_dict:
                   raise ValueError("Each experiment in 'antitracer_config.experiments' must be a dict with 'basin' and 'polygon' keys.")

        # reference case details
        self.reference_case = "g.e22.GOMIPECOIAF_JRA-1p4-2018.TL319_g17.SMYLE.005"
        self.time_reference = xr.cftime_range(
            "0306-01-01", "0368-12-31", freq="ME", calendar="noleap"
        )
        self._df_case_status = None
        self.df_validation = None
        self.df_analysis = None
        self.set_experiments()

    def set_experiments(self):
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
        cdr_forcing_root_path = "/global/cfs/projectdirs/m4746/Projects/OAE-Efficiency-Map/data/alk-forcing/OAE-Efficiency-Map"

        generic_cdr_files_template = lambda b, p, d: f"{cdr_forcing_root_path}/alk-forcing-{b}.{p:03d}-{d}.nc"

        nyear_case = 1
        nyear_baseline = 16
        periods = nyear_case * 12
        self.time_cases = {}
        for k in ref_dates:
            self.time_cases[k] = xr.cftime_range(
                k, periods=periods, freq="ME", calendar="noleap"
            )

        periods = nyear_baseline * 12
        self.time_baseline = xr.cftime_range(
            ref_dates[0], periods=periods, freq="ME", calendar="noleap"
        )

        # Initialize the list of rows for the DataFrame
        rows = []

        if self.cdr_forcing != "ANTITRACER":
            rows.append(
                dict(
                    blueprint=self.blueprint,
                    polygon=None, polygon_master=None, basin=None,
                    start_date=start_dates[0],
                    cdr_forcing=None, cdr_forcing_files=None,
                    case=f"{self.blueprint}.{project_sname}.control.{self.vintage}",
                    simulation_key="baseline",
                    refdate=ref_dates[0],
                    stop_n=nyear_baseline,
                    wallclock="12:00:00",
                    curtail_output=False,
                )
            )

        if self.cdr_forcing == "ANTITRACER":
            group_config = self.antitracer_config
            global_suffix = group_config["suffix"]
            common_date = group_config["date"]

            collected_cdr_files = []
            varname = "alk_forcing"

            locations_to_aggregate = group_config["experiments"] # Use 'experiments' key

            for location_dict in locations_to_aggregate:
                b = location_dict["basin"]
                p = location_dict["polygon"]
                d = common_date 

                assert b in basins, f"Configured basin '{b}' not in known basins."
                assert p < npolygon.get(b, 0), f"Configured polygon '{p}' invalid for basin '{b}'."
                assert d in start_dates, f"Configured start_date '{d}' not in known start_dates."

                base_file_path = generic_cdr_files_template(b, p, d)
                assert os.path.exists(base_file_path), f"Antitracer base forcing file not found: {base_file_path}"

                collected_cdr_files.append(base_file_path)

            # The case name reflects the overall aggregated run, not individual loc_id
            simname = f"{self.simulation_name}_{common_date}_{global_suffix}"
            case = f"{self.blueprint}.{project_sname}.{simname}.{self.vintage}"

            try:
                index = start_dates.index(common_date)
                ref_date = ref_dates[index]
            except ValueError:
                raise ValueError(f"'{start_date}' is not a valid start date.")

            # Add this SINGLE row to the DataFrame
            rows.append(
                dict(
                    blueprint=self.blueprint,
                    polygon=None, # Polygon is not meaningful for an aggregated run
                    basin=None,   # Basin is not meaningful for an aggregated run
                    start_date=common_date,
                    cdr_forcing=self.cdr_forcing,
                    cdr_forcing_files=collected_cdr_files, # The collected list of all files
                    case=case,
                    simulation_key=simname,
                    refdate=ref_date,
                    stop_n=nyear_case,
                    wallclock="10:00:00",
                    curtail_output=True,
                )
            )

        else: # self.cdr_forcing is not "ANTITRACER" (OAE, DOR, ERW, or None)
            index = 0
            polygon_master_index = -1
            for b in basins:
                n = npolygon[b]

                for p in range(0, n):
                    polygon_master_index += 1

                    # skip non-coastal polygons if ERW
                    if self.cdr_forcing == "ERW":
                        if p not in coastal_polygons[b]:
                            continue
                            
                    for i, d in enumerate(start_dates):

                        file = f"{cdr_forcing_path}/alk-forcing-{b}.{p:03d}-{d}.nc"
                        assert os.path.exists(file), file

                        loc = f"{b}_{p:03d}_{d}-01"
                        simname = f"{self.simulation_name}_{loc}_{index:05d}"
                        case = f"{self.blueprint}.{project_sname}.{simname}.{self.vintage}"

                        rows.append(
                            dict(
                                blueprint=self.blueprint,
                                polygon=p,
                                polygon_master=polygon_master_index,
                                basin=b,
                                start_date=d,
                                cdr_forcing=self.cdr_forcing,
                                cdr_forcing_files=[file],
                                case=case,
                                simulation_key=simname,
                                refdate=ref_dates[i],
                                stop_n=nyear_case,
                                wallclock="10:00:00",
                                curtail_output=True,
                            )
                        )
                        index += 1

        # Assign the built list of rows to the DataFrame
        self.df = pd.DataFrame(rows).set_index("case")
        self.cases = self.df.index.to_list()

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
                build_script = submit_build(
                    blueprint=caseinfo["blueprint"],
                    case=case,
                    cdr_forcing=caseinfo["cdr_forcing"],
                    cdr_forcing_files=caseinfo["cdr_forcing_files"],
                    refdate=caseinfo["refdate"],
                    stop_n=caseinfo["stop_n"],
                    wallclock=caseinfo["wallclock"],
                    curtail_output=caseinfo["curtail_output"],
                    queue=queue,
                    clobber=clobber,
                    run_local=run_local,
                )

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
