#!/bin/python
# Remove after https://bugs.python.org/issue38605 is resolved (python 3.11?)
from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import datetime
import json
import logging
import os
import subprocess
import sys
import textwrap
import time
import types
import typing

SECONDS_PER_SOLAR_YEAR = 31556925
VERSION = "0.2.1"


def timedelta_from_dict(d: typing.Dict[str, int]) -> datetime.timedelta:
    years: typing.Optional[int] = d.pop("years", None)
    months: typing.Optional[int] = d.pop("months", None)

    d.setdefault("seconds", 0)
    if years is not None:
        d["seconds"] += years * SECONDS_PER_SOLAR_YEAR
    if months is not None:
        d["seconds"] += int(months * SECONDS_PER_SOLAR_YEAR / 12)
    return datetime.timedelta(**d)


def pretty_check_returncode(
    returncode: typing.Optional[int], stderr: bytes, errmsg: str
) -> None:
    if returncode is not None and returncode != 0:
        print(f"{errmsg}. stderr:")
        string = stderr.decode("utf-8")
        print(textwrap.indent(string, "\t"))
        raise Exception(errmsg)


@dataclasses.dataclass(frozen=True, order=True)
class snapshot_t:
    datetime: datetime.datetime

    def __post_init__(self) -> None:
        now = datetime.datetime.utcnow()
        if self.datetime > now:
            raise ValueError("A snapshot with a future date exists")

    def __str__(self) -> str:
        return f"snapshot_t({self.datetime})"


@dataclasses.dataclass(frozen=True, order=True)
class window_t:
    # We want one snapshot per period going back until max_age in the past.
    # If max_age is None, keep one snapshot per period for all time.
    max_age: typing.Optional[datetime.timedelta] = dataclasses.field()
    period: datetime.timedelta = dataclasses.field(compare=False)


@dataclasses.dataclass(frozen=True)
class replica_t:
    remote_host: typing.Optional[str] = dataclasses.field()
    ssh_port: typing.Optional[int] = dataclasses.field()
    ssh_identity_file: typing.Optional[str] = dataclasses.field()
    pool: str = dataclasses.field()
    dataset: str = dataclasses.field()
    windows: typing.List[window_t] = dataclasses.field()
    snapshot_prefix: str = dataclasses.field(default="ZAM-")
    date_fstring: str = dataclasses.field(default="%Y-%m-%dT%H:%M:%S")

    def __post_init__(self) -> None:
        get_window_max_age: typing.Callable[[window_t], datetime.timedelta] = (
            lambda x: x.max_age or datetime.timedelta.max
        )
        if list(self.windows) != sorted(list(self.windows), key=get_window_max_age):
            raise ValueError(f"{self}'s windows are not sorted by max_age")
        get_window_period: typing.Callable[
            [window_t], datetime.timedelta
        ] = lambda x: x.period
        if list(self.windows) != sorted(list(self.windows), key=get_window_period):
            raise ValueError(
                f"{self}'s window periods are not monotonically increasing"
            )

    def __str__(self) -> str:
        return f"replica_t({self.remote_host}, {self.pool}, {self.dataset})"

    def get_snapshot_full_name(self, snapshot: snapshot_t) -> str:
        return f"{self.pool}/{self.dataset}@{self.snapshot_prefix}{snapshot.datetime.strftime(self.date_fstring)}"

    def get_ssh_cmd(self) -> typing.List[str]:
        if self.remote_host is None:
            return []

        ssh = ["ssh"]
        if self.ssh_port is not None:
            ssh += ["-p", str(self.ssh_port)]
        if self.ssh_identity_file is not None:
            ssh += ["-i", self.ssh_identity_file]
        ssh += [self.remote_host]
        return ssh

    def exists(self) -> bool:
        dataset_full_name: str = f"{self.pool}/{self.dataset}"
        cmd: subprocess.CompletedProcess[bytes] = subprocess.run(
            self.get_ssh_cmd() + ["zfs", "list", "-o", "name"], capture_output=True
        )
        pretty_check_returncode(
            cmd.returncode,
            cmd.stderr,
            "`zfs list` failed when checking if {self} exists",
        )
        output = cmd.stdout.decode("utf-8")
        lines = output.split("\n")
        assert lines[0] == "NAME"
        lines = lines[1:]
        return dataset_full_name in lines

    def list(self) -> typing.List[snapshot_t]:
        # step 1: call `zfs list -t snapshot {self}`
        dataset_full_name = f"{self.pool}/{self.dataset}"
        completed: subprocess.CompletedProcess[bytes] = subprocess.run(
            self.get_ssh_cmd()
            + ["zfs", "list", "-t", "snapshot", dataset_full_name, "-o", "name"],
            capture_output=True,
        )
        pretty_check_returncode(
            completed.returncode,
            completed.stderr,
            f"Failed to list snapshots of {self}",
        )
        if completed.stderr == b"no datasets available\n":
            return []
        output = completed.stdout.decode("utf-8")

        # step 2: parse output
        lines = output.split("\n")
        assert lines[0] == "NAME"
        lines = lines[1:]
        prefix = f"{dataset_full_name}@"
        ret = []
        for fullname in lines:
            if len(fullname) == 0:
                continue
            assert fullname.startswith(prefix)
            name = fullname.removeprefix(prefix)

            if not name.startswith(self.snapshot_prefix):
                # Ignore non-ZAM snapshots
                continue

            time_s = name.removeprefix(self.snapshot_prefix)
            dt = datetime.datetime.strptime(time_s, self.date_fstring)
            snapshot = snapshot_t(datetime=dt)
            ret.append(snapshot)
            assert self.get_snapshot_full_name(snapshot) == fullname
        ret.sort()
        return ret

    def clone_to(
        self,
        dest: replica_t,
        snapshot_old: typing.Optional[snapshot_t],
        snapshot_new: snapshot_t,
    ) -> None:
        args_incremental = []
        if snapshot_old is not None:
            args_incremental = ["-i", self.get_snapshot_full_name(snapshot_old)]

        # TODO: send with --replicate or maybe --backup? similarly update receive
        cmd_source: typing.List[str] = (
            ["zfs", "send"]
            + args_incremental
            + ["--raw", "--verbose", f"{self.get_snapshot_full_name(snapshot_new)}"]
        )
        with subprocess.Popen(
            self.get_ssh_cmd() + cmd_source,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as popen_source:
            cmd_dest: typing.List[str] = [
                "zfs",
                "recv",
                f"{dest.get_snapshot_full_name(snapshot_new)}",
            ]
            with subprocess.Popen(
                dest.get_ssh_cmd() + cmd_dest,
                stdin=popen_source.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as popen_dest:
                while True:  # do while unsuccessful
                    status_source = popen_source.poll()
                    stderr_source = (
                        None
                        if popen_source.stderr is None
                        else popen_source.stderr.read()
                    )
                    if stderr_source is None:
                        raise Exception()
                    pretty_check_returncode(
                        status_source, stderr_source, f"zfs send failed {cmd_source}"
                    )
                    status_dest = popen_dest.poll()
                    stderr_dest = (
                        None if popen_dest.stderr is None else popen_dest.stderr.read()
                    )
                    if stderr_dest is None:
                        raise Exception()
                    pretty_check_returncode(
                        status_dest, stderr_dest, f"zfs recv failed {cmd_dest}"
                    )
                    if status_source == 0 and status_dest == 0:
                        break
                    time.sleep(1)

    def delete(self, dest) -> None:
        raise Exception("not implemented")


@dataclasses.dataclass(frozen=True)  # , order=True)
class managed_dataset_t:
    source: replica_t = dataclasses.field()
    destinations: typing.List[replica_t] = dataclasses.field()

    snapshot_period: datetime.timedelta = dataclasses.field()
    replication_period: datetime.timedelta = dataclasses.field()
    prune_period: datetime.timedelta = dataclasses.field()

    """If true, not only will the source dataset be cloned but also all descendent datasets"""
    recursive: bool = dataclasses.field(default=True)

    def __post_init__(self) -> None:
        if (
            self.snapshot_period > self.replication_period
            or self.snapshot_period > self.prune_period
        ):
            raise ValueError(
                "There is no point in replicating/pruning more often than the rate at which they are created"
            )

    def take_snapshot(self) -> snapshot_t:
        now: datetime.datetime = datetime.datetime.utcnow()
        snapshot: snapshot_t = snapshot_t(datetime=now)
        snapshot_fullname: str = self.source.get_snapshot_full_name(snapshot)

        command: typing.List[str] = ["zfs", "snapshot", snapshot_fullname]
        if self.recursive:
            command.append("-r")
        completed: subprocess.CompletedProcess[bytes] = subprocess.run(
            self.source.get_ssh_cmd() + command, capture_output=True
        )
        returncode: int = completed.returncode
        stderr: bytes = completed.stderr
        pretty_check_returncode(
            returncode,
            stderr,
            f"Failed to take snapshot on {self.source}",
        )
        return snapshot


@dataclasses.dataclass(frozen=True)
class config_t:
    managed_datasets: typing.List[managed_dataset_t] = dataclasses.field()


# TODO: find the correct way of doing this
primitive_map = {
    datetime.timedelta: timedelta_from_dict,
    str: str,
}


def object_from_dict(type_, value):
    if type_ in primitive_map:
        # TODO: remove try/except?
        try:
            constructor = primitive_map[type_]
            return constructor(value)
        except Exception as e:
            pass
    if hasattr(type_, "__origin__"):
        if type_.__origin__ is typing.Union:
            # very crude check for optional.
            # TODO: add support for other types of unions
            assert len(type_.__args__) == 2
            assert type_.__args__[1] is type(None)
            return object_from_dict(type_.__args__[0], value)
        if type_.__origin__ is list:
            assert len(type_.__args__) == 1
            subtype = type_.__args__[0]
            return [object_from_dict(subtype, x) for x in value]

    assert type(value) == dict
    assert hasattr(type_, "__dataclass_fields__")
    dct = value
    # the args to pass to type_' constructor
    args = {}

    # VALIDATE FIELD NAMES
    expected_names = set(typing.get_type_hints(type_).keys())
    actual_names = set(dct.keys())
    extra_names = actual_names - expected_names
    if len(extra_names) > 0:
        raise Exception(f"Illegal keys in {type_.__name__}: {extra_names}")
    questionable_names = expected_names - actual_names
    missing_names = []
    for name in questionable_names:
        # if the field has a default value, ignore it
        if not isinstance(
            type_.__dataclass_fields__[name].default, dataclasses._MISSING_TYPE
        ):  # would it be possible / safer to do something like field.default.__class__.__module__ == dataclasses?
            continue

        # if the type hint indicates that it is
        hint = typing.get_type_hints(type_)[name]
        if hasattr(hint, "__origin__") and hint.__origin__ is typing.Union:
            assert len(hint.__args__) == 2
            assert hint.__args__[1] is type(None)
            args[name] = None
            continue
        missing_names.append(name)
    if len(missing_names) > 0:
        raise Exception(f"Missing keys for {type_.__name__}: {missing_names}")

    # PARSE FIELDS
    for name in actual_names:
        field_type = typing.get_type_hints(type_)[name]
        args[name] = object_from_dict(field_type, dct[name])

    # CONSTRUCT
    return type_(**args)


# TODO: constants should be moved to top of file
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

# TODO: why isn't this in main?
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
    "--version", action="store_true", default=False, help="Show the version number"
)


def do_snapshot(mds: managed_dataset_t) -> datetime.datetime:
    snapshots = mds.source.list()
    if (
        len(snapshots) == 0
        or datetime.datetime.utcnow() - snapshots[-1].datetime > mds.snapshot_period
    ):
        logging.info(f"Taking snapshot on {mds.source}")
        snapshots.append(mds.take_snapshot())
    return snapshots[-1].datetime + mds.snapshot_period


def do_replicate(mds: managed_dataset_t) -> datetime.datetime:
    src = mds.source
    snapshots_s = src.list()
    assert len(snapshots_s) > 0

    for dest in mds.destinations:
        if not dest.exists():
            snapshot = snapshots_s[0]
            logging.info(f"Initializing {dest} with {snapshot} from {src}")
            mds.source.clone_to(dest, None, snapshot)
        snapshots_d = dest.list()

        for previous, current in zip(snapshots_s, snapshots_s[1:]):
            assert previous in snapshots_d
            if not current in snapshots_d:
                logging.info(f"Cloning {current} from {src} to {dest}")
                mds.source.clone_to(dest, previous, current)
                snapshots_d.append(current)

    return datetime.datetime.utcnow() + mds.replication_period


def do_prune(mds: managed_dataset_t) -> datetime.datetime:
    return datetime.datetime.max


async def async_loop(
    func: typing.Callable[[managed_dataset_t], datetime.datetime],
    datasets: typing.List[managed_dataset_t],
) -> int:
    while True:
        next_action = min(map(func, datasets))
        num_sec = (next_action - datetime.datetime.utcnow()).total_seconds()
        await asyncio.sleep(max(1, num_sec))


async def main() -> None:
    # args must be global so that, e.g., the log function can access the log level
    args = parser.parse_args()

    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    logging.basicConfig(
        level=numeric_level,
        style="{",
        format="{asctime:>23} {levelname:>7} {filename:>15} {lineno:>4}: {message}",
    )

    logging.debug(f"Args are: {args}")

    args_version: bool = args.version
    if args_version:
        print(VERSION)
        return

    args_config_file_name: str = args.config_file_name
    if args_config_file_name is None:
        for fname in default_configs:
            if os.path.isfile(fname):
                args_config_file_name = fname
                break
        else:
            print(
                f"None of the default config files exist ({default_configs})",
                file=sys.stderr,
            )
            exit(1)

    with open(args_config_file_name) as json_file:
        foo: dict[object, object] = json.load(json_file)
        conf: config_t = object_from_dict(config_t, foo)
    logging.debug(f"config is: {conf}")

    # note that we don't have to worry about the do_* functions running
    # concurrently because they are not asyncronous; only async_loop and main
    # are async.

    loop_snapshot = async_loop(do_snapshot, conf.managed_datasets)
    loop_replicate = async_loop(do_replicate, conf.managed_datasets)
    loop_prune = async_loop(do_prune, conf.managed_datasets)
    tasks: typing.List[asyncio.Task[int]] = [
        asyncio.create_task(loop_snapshot, name="snapshot"),
        asyncio.create_task(loop_replicate, name="replicate"),
        asyncio.create_task(loop_prune, name="prune"),
    ]

    while True:
        for task in tasks:
            if task.done():
                exception = task.exception()
                if exception is not None:
                    print(f"A task raised an exception ({task}, {exception})")
                    raise exception
                else:
                    raise Exception(f"A task exited unexpectedly ({task})")
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
