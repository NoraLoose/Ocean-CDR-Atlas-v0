import os
import subprocess


# config.yml
project_name = "Ocean-CDR-Atlas-v0"
project_sname = "cdr-atlas-v0"
branch = project_sname
codename = "cesm2.2.0"
remote = "git@github.com:CWorthy-ocean/cesm2.2.0.git"

machine_name = "perlmutter"
account = "m4746"

kernel_name = "python3"

# house keeping
USER = os.environ["USER"]
assert machine_name in ["perlmutter"]

# directories that might not exist
paths = dict()
paths["scratch"] = f"{os.environ['SCRATCH']}/{project_name}"
dir_project_root = f"/global/cfs/projectdirs/m4746/Projects/{project_name}"
paths["data"] = f"{dir_project_root}/data"
paths["codes"] = (
    f"{dir_project_root}/codes"  # does this need to be known outside config.py?
)
paths["cases"] = f"{dir_project_root}/cases"

# TODO: read-only give better performance, but requires staging data
paths["cesm_inputdata"] = f"/global/cfs/projectdirs/m4746/Datasets/cesm-inputdata"
paths["cesm_inputdata_ro"] = f"/dvs_ro/cfs/projectdirs/m4746/Datasets/cesm-inputdata"

for path in paths.values():
    os.makedirs(path, exist_ok=True)

# directories that exist
paths["workflow"] = os.path.dirname(os.path.realpath(__file__))

paths["src"] = os.path.join(paths["codes"], codename)

# check out the code
if not os.path.exists(paths["src"]):
    # clone the repo
    p = subprocess.Popen(
        f"git clone {remote}",
        shell=True,
        cwd=paths["codes"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()

    # crude error handling
    if stdout:
        print(stdout)
    if stderr:
        print(stderr)
    if p.returncode != 0:
        raise Exception("git error")

    if branch:
        # checkout branch
        p = subprocess.Popen(
            f"git checkout {branch}",
            shell=True,
            cwd=paths["codes"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()

        # crude error handling
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)
        if p.returncode != 0:
            raise Exception("git error")
