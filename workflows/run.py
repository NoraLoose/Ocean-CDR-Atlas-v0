#!/usr/bin/env python3
from subprocess import check_call
import time
import click

import atlas


@click.command()
@click.option("--cdr-forcing", required=True)
@click.option("--vintage", required=False, default="001")
@click.option("--phase", required=False, default="deploy")
@click.option("--clobber", required=False, type=click.BOOL, default=False)
def main(cdr_forcing, vintage, phase, clobber):
    """
    Run Ocean CDR Atlas workflow

    """
    print("Running Atlas Workflow:")
    print(f"CDR Forcing: {cdr_forcing}")
    print(f"Phase: {phase}")
    print(f"Vintage: {vintage}")
    print(f"(clobber={clobber})")

    calc = atlas.global_irf_map(cdr_forcing, vintage)
    clobber_list = calc.check_cases()

    print(f"{sum(calc.df_case_status.build.to_list())} built")
    print(f"{sum(calc.df_case_status.submitted.to_list())} submitted")
    print(f"{sum(calc.df_case_status.run_completed.to_list())} run completed")
    print(f"{sum(calc.df_case_status.archive.to_list())} archive")

    calc.build(
        phase=phase,
        clobber=clobber,
        clobber_list=clobber_list,
    )

    n_jobs = calc.compute()
    if n_jobs > 0:
        raise SystemExit("stop here to wait for computation to complete")

    # get the queue status
    running_jobs = machine.running_jobids()
    n_jobs = len(running_jobs)
    while n_jobs:
        running_jobs = machine.running_jobids()
        if n_jobs - len(running_jobs) >= 10:
            n_jobs = len(running_jobs)
            calc.validate(clobber=clobber)
            calc.analyze(clobber=clobber)
            calc.visualize(clobber=clobber)

        print(f"waiting on {len(running_jobs)}")
        time.sleep(30)

    calc.validate(clobber=clobber)
    calc.analyze(clobber=clobber)
    calc.visualize(clobber=clobber)


if __name__ == "__main__":
    main()
