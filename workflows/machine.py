import os
from subprocess import check_output, check_call

import tempfile
import time
import textwrap

import dask
from dask.distributed import Client

from config import paths, account

USER = os.environ["USER"]
SCRATCH = paths["scratch"]

JUPYTERHUB_URL = "https://jupyter.nersc.gov"


class dask_cluster(object):
    """ad hoc script to launch a dask cluster"""

    def __init__(self, wallclock="04:00:00"):
        path_dask = f"{SCRATCH}/dask"
        os.makedirs(path_dask, exist_ok=True)

        scheduler_file = tempfile.mktemp(
            prefix="dask_scheduler_file.", suffix=".json", dir=path_dask
        )

        script = textwrap.dedent(
            f"""\
            #!/bin/bash
            #SBATCH --job-name dask-worker
            #SBATCH --account {account}
            #SBATCH --qos=regular
            #SBATCH --nodes=4
            #SBATCH --ntasks=64
            #SBATCH --time={wallclock}
            #SBATCH --constraint=cpu
            #SBATCH --error {path_dask}/dask-workers/dask-worker-%J.err
            #SBATCH --output {path_dask}/dask-workers/dask-worker-%J.out

            echo "Starting scheduler..."

            scheduler_file={scheduler_file}
            rm -f $scheduler_file

            module load python
            conda activate cworthy

            #start scheduler
            DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=3600s \
            DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=3600s \
            dask scheduler \
                --interface hsn0 \
                --scheduler-file $scheduler_file &

            dask_pid=$!

            # Wait for the scheduler to start
            sleep 5
            until [ -f $scheduler_file ]
            do
                 sleep 5
            done

            echo "Starting workers"

            #start scheduler
            DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=3600s \
            DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=3600s \
            srun dask-worker \
            --scheduler-file $scheduler_file \
                --interface hsn0 \
                --nworkers 1 

            echo "Killing scheduler"
            kill -9 $dask_pid
            """
        )

        script_file = tempfile.mktemp(prefix="launch-dask.", dir=path_dask)
        with open(script_file, "w") as fid:
            fid.write(script)

        print(f"spinning up dask cluster with scheduler:\n  {scheduler_file}")
        self.jobid = (
            check_output(f"sbatch {script_file} " + "awk '{print $1}'", shell=True)
            .decode("utf-8")
            .strip()
            .split(" ")[-1]
        )

        while not os.path.exists(scheduler_file):
            time.sleep(5)

        print("cluster running...")

        dask.config.config["distributed"]["dashboard"][
            "link"
        ] = "{JUPYTERHUB_SERVICE_PREFIX}proxy/{host}:{port}/status"

        self.client = Client(scheduler_file=scheduler_file)
        print(f"Dashboard:\n {JUPYTERHUB_URL}/{self.client.dashboard_link}")

    def shutdown(self):
        self.client.shutdown()
        check_call(f"scancel {self.jobid}", shell=True)


def running_jobids():
    """get list of running jobs"""
    return (
        check_output("squeue -u ${USER} | grep run. | awk '{print $1}'", shell=True)
        .decode("utf-8")
        .strip()
        .split("\n")
    )


def building_jobids():
    """get list of jobs running for model builds"""
    return [
        jid
        for jid in check_output(
            "squeue -u ${USER} | grep build. | awk '{print $1}'", shell=True
        )
        .decode("utf-8")
        .strip()
        .split("\n")
        if jid
    ]


def queue_info():
    """return a dictionary with a key for case name and value of JobId"""
    stdout = (
        check_output(
            "squeue -u ${USER} --Format=JobID:10,State:10,Name:256", shell=True
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )
    info_list = [
        {"JobId": s.split()[0], "JobState": s.split()[1], "JobName": s.split()[2]}
        for s in stdout[1:]
    ]

    info = {}
    for d in info_list:
        if d["JobName"][:4] == "run.":
            case = d["JobName"].replace("run.", "")
            info[case] = d
    return info


def JobState(JobId):
    stdout = (
        check_output(f"scontrol show JobId={JobId}", shell=True)
        .decode("utf-8")
        .strip()
        .replace("\n", "")
    )
    return [s.split("=")[1] for s in stdout.split(" ") if "JobState" in s][0]
