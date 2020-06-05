"""We want all steps to be short so that we don't have to worry about
them influencing the timers firing too much.  Add metrics decorator to
all step functions, and we can check from the resulting file if this
assumption remains true.

"""
import functools
import logging
import re
import time

import click
import pandas as pd

metrics_logger = logging.getLogger("metrics")


def metrics(_func=None, *, tag=""):
    def wrap_fn(func):
        @functools.wraps(func)
        def measured_f(*args, **kwargs):
            start = time.monotonic()
            rv = func(*args, **kwargs)
            metrics_logger.info(f"{tag}, {func.__name__} - {time.monotonic() - start}")
            return rv

        return measured_f

    if _func:
        return wrap_fn(_func)
    else:
        return wrap_fn


def read_metrics(filename):
    with open(filename) as f:
        dat = f.readlines()
    r = re.compile(".* - ([a-z]*), ([a-z]*) - ([0-9.e-]*)")
    dat = pd.DataFrame(
        {"state": x.group(1), "fn": x.group(2), "time": float(x.group(3))}
        for x in [r.match(d) for d in dat]
    )
    print(dat.groupby("state").time.mean())


@click.command()
@click.option("--metrics-file", default="metrics.log")
def cli(metrics_file):
    read_metrics(metrics_file)


if __name__ == "__main__":
    cli()
