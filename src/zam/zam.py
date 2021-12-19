#!/bin/python
import argparse
import json
import logging
import os
import typing
import zam
from zam.snapshoter import snapshoter
from zam.replicator import replicator
from zam.pruner import pruner

# TODO: fetch from setup tools instead. Maybe something like this, assuming that
# there's a way to get the cfg file from the installed package?
#
# from setuptools.config import read_configuration
# conf_dict = read_configuration("path/to/setup.cfg")
# VERSION = conf_dict["metadata"]["version"]
VERSION = "0.3.0.dev2"

default_config_fname = "zam_config.json"

# from highest to lowest precedence
default_configs = [
    f"/etc/{default_config_fname}",
    # /usr/local is for sysadmin installed files; other /usr directorys are from the package manager
    # host-specific configuration
    f"/usr/local/etc/{default_config_fname}",
    f"/usr/etc/{default_config_fname}",
    # architecture-independent configuration
    f"/usr/local/share/{default_config_fname}",
    f"/usr/share/{default_config_fname}",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        "-c",
        dest="config_file_name",
        action="store",
        default=None,
        help="The location of the script's configuration file",
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error", "critical"],
        default="warning",
        help="Case insensitive log level",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Show the version number",
    )

    # args must be global so that, e.g., the log function can access the log level
    args = parser.parse_args()

    args_version: bool = args.version
    if args_version:
        print(VERSION)
        return

    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    logging.basicConfig(
        level=numeric_level,
        style="{",
        format="{asctime:>23} {levelname:>7} {filename:>15} {lineno:>4}: {message}",
    )

    logging.debug(f"Args are: {args}")

    assert args.config_file_name is None

    tasks: typing.List[zam.task.task] = []
    # tasks.append(???)

    zam.task.run_tasks(tasks)


if __name__ == "__main__":
    main()
