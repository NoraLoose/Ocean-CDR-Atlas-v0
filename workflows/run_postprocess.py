#!/usr/bin/env python3
from subprocess import check_call
import time
import click
import textwrap

import atlas
from config import paths, project_sname, account, kernel_name

scriptroot = paths["workflow"]

cdr_forcing = "DOR"
vintage = "001"
phase = "deploy"



@click.command()
@click.option("--clobber", required=False, type=click.BOOL, default=False)
def main(clobber):
    """
    Run Ocean CDR Atlas workflow

    """
    print("Running Atlas Workflow:")
    print(f"CDR Forcing: {cdr_forcing}")
    print(f"Phase: {phase}")
    print(f"Vintage: {vintage}")
    print(f"(clobber={clobber})")

    calc = atlas.global_irf_map(cdr_forcing, vintage)
    #calc.validate(clobber=clobber)
    calc.analyze(clobber=clobber)

if __name__ == "__main__":
    main()






