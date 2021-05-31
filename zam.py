#!/bin/python
import argparse
import collections
import dataclasses
import datetime
import json
import subprocess
import sys
import time
import types
import typing

SECONDS_PER_SOLAR_YEAR=31556925

def datetime_from_json(obj):
    d = collections.defaultdict(int, obj)

    offset=0
    #timedelta doesn't support years or months because of ambiguity.
    offset+=d["years"]*SECONDS_PER_SOLAR_YEAR # arbitrary choice of seconds per year
    offset+=d["months"]*SECONDS_PER_SOLAR_YEAR/12 # I will arbitrarily say "month" means the average length of a month
    return datetime.timedelta(
        weeks=int(d["weeks"]),
        days=int(d["days"]),
        hours=int(d["hours"]),
        minutes=int(d["minutes"]),
        seconds=int(d["seconds"])+offset,
    )

@dataclasses.dataclass(frozen=True, order=True)
class window_t:
    # We want one snapshot per period going back until max_age in the past.
    # If max_age is None, keep one snapshot per period for all time.
    max_age: typing.Optional[datetime.timedelta] = dataclasses.field()
    period: datetime.timedelta = dataclasses.field(compare=False)

    @staticmethod
    def from_json(obj):
        try:
            max_age=datetime_from_json(obj["max-age"])
        except KeyError:
            max_age=None
        period=datetime_from_json(obj["period"])
        return window_t(
            max_age=max_age,
            period=period,
        )

@dataclasses.dataclass(frozen=True) #, order=True)
class replica_t:
    remote_host: typing.Optional[str] = dataclasses.field()
    pool:str = dataclasses.field()
    dataset:str = dataclasses.field()
    windows: typing.Tuple[window_t] = dataclasses.field()
    snapshot_prefix: str = dataclasses.field()
    date_fstring: str = dataclasses.field()
    def __post_init__(self):
        if list(self.windows) != sorted(list(self.windows), key=lambda x: x.max_age or datetime.timedelta.max):
            raise ValueError('Given windows are not sorted by max_age')
        if list(self.windows) != sorted(list(self.windows), key=lambda x: x.period):
            raise ValueError('Given window periods are not monotonically increasing')

    @staticmethod
    def from_json(obj):
        try:
            remote_host:str = obj["remote-host"]
        except KeyError:
            remote_host = None
        pool:str = obj["pool"]
        dataset:str=obj["dataset"]
        windows=[]
        for ele in obj["windows"]:
            windows.append(window_t.from_json(ele))
        try:
            snapshot_prefix: str = obj["snapshot-prefix"]
        except KeyError:
            snapshot_prefix = "ZAM-"
        try:
            date_fstring=obj["date-fstring"]
        except KeyError:
            date_fstring="%Y-%m-%dT%H:%M:%S"
        return replica_t(
            remote_host=remote_host,
            pool=pool,
            dataset=dataset,
            windows=windows,
            snapshot_prefix=snapshot_prefix,
            date_fstring=date_fstring,
        )



@dataclasses.dataclass(frozen=True) #, order=True)
class managed_dataset_t:
    source: replica_t
    destinations: typing.Tuple[replica_t, ...]

    snapshot_period: datetime.timedelta
    replication_period: datetime.timedelta
    prune_period: datetime.timedelta

    """If true, not only will the source dataset be cloned but also all descendent datasets"""
    recursive: bool = dataclasses.field()

    def __post_init__(self):
        if self.snapshot_period > self.replication_period or self.snapshot_period > self.prune_period:
            raise ValueError('There is no point in replicating/pruning more often than the rate at which they are created')

    @staticmethod
    def from_json(obj):
        source = replica_t.from_json(obj["source"])
        destinations=[]
        for ele in obj["destinations"]:
            destinations.append(replica_t.from_json(ele))
        snapshot_period=datetime_from_json(obj["snapshot-period"])
        replication_period=datetime_from_json(obj["replication-period"])
        prune_period=datetime_from_json(obj["prune-period"])
        try:
            recursive=obj["recursive"]
        except KeyError:
            recursive=True
        return managed_dataset_t(
            source=source,
            destinations=destinations,
            snapshot_period=snapshot_period,
            replication_period=replication_period,
            prune_period=prune_period,
            recursive=recursive,
        )

@dataclasses.dataclass(frozen=True)
class config:
    managed_datasets: typing.Tuple[replica_t]

    @staticmethod
    def from_json(obj):
        managed_datasets = []
        lst = obj["managed-datasets"]
        for ele in lst:
            managed_datasets.append(managed_dataset_t.from_json(ele))
        return config(tuple(managed_datasets))

@dataclasses.dataclass(frozen=True, order=True)
class snapshot_t:
    remote_host: typing.Optional[str]
    pool:str
    dataset:str
    datetime: datetime.datetime

    def delete():
        raise "not implemented"

    def clone_to(replica: replica_t):
        raise "not implemented"

    @staticmethod
    def new(mds: managed_dataset_t):
        replica: replica_t = mds.source
        if replica.remote_host is not None:
            raise Exception("not implemented")
        now=datetime.datetime.utcnow()
        s_now=now.strftime(replica.date_fstring)

        #zfs snapshot tank/home/i@foo
        dataset_full_name=f'{replica.pool}/{replica.dataset}'
        snapshot_full_name=f'{dataset_full_name}@{replica.snapshot_prefix}{s_now}'
        command=['zfs', 'snapshot', snapshot_full_name]
        if mds.recursive:
            command.append('-r')
        command = subprocess.run(command, capture_output=True)
        if command.returncode:
            print("Failed to create snapshot. stderr:")
            print("\t" + command.stderr.decode("utf-8").replace("\n", "\n\t").rstrip("\n\t"))
            raise Exception("Failed to create snapshot")
        output = command.stdout.decode("utf-8")
        lines = output.split('\n')
        return snapshot_t(
            remote_host=replica.remote_host,
            pool=replica.pool,
            dataset=replica.dataset,
            datetime=now)

    @staticmethod
    def list(replica: replica_t):
        if replica.remote_host is not None:
            raise Exception("not implemented")


        dataset_full_name=f'{replica.pool}/{replica.dataset}'
        command = subprocess.run(['zfs', 'list', '-t', 'snapshot',
                                  dataset_full_name, '-o',
                                  'name'], capture_output=True)
        command.check_returncode()
        output = command.stdout.decode("utf-8")
        lines = output.split('\n')
        assert(lines[0] == 'NAME')
        prefix=f'{dataset_full_name}@'

        ret=[]
        for line in lines[1:]:
            if len(line) == 0:
                continue
            assert(line.startswith(prefix))
            line=line.removeprefix(prefix)

            if not line.startswith(replica.snapshot_prefix):
                continue
            line=line.removeprefix(replica.snapshot_prefix)

            dt = datetime.datetime.strptime(line, replica.date_fstring)
            ret.append(snapshot_t(
                remote_host=replica.remote_host,
                pool=replica.pool,
                dataset=replica.dataset,
                datetime=dt))
        ret.sort()
        return ret




LOG_ERROR=1
LOG_WARNING=2
LOG_INFO=3
LOG_TRACE=4

parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', type=str, dest="config_file_name", help="The location of the script's configuration file", default='zam_config.json')
parser.add_argument('--verbose', '-v', default=LOG_ERROR, action='count', help="Increases verbosity. Can be used multiple times.")




# Instead of systemd timer units, use this?
# systemd-run --on-active=30 /bin/touch /tmp/foo

# On replication, replicate ALL of the local snapshots that are dated after the
# newest one on the remote server.

# simplified pruning algorithm:
# * for each snapshot s
#   * if s.successor.time - s.predecessor.time < curr_window.period:
#     * delete s
#
# Known issues / edge cases:
# * Must be careful on border between different windows?

def do_snapshot(mds):
    snapshots=snapshot_t.list(mds.source)
    now=datetime.datetime.utcnow()
    if now - snapshots[-1].datetime > mds.snapshot_period:
        print("Taking snapshot")
        new=snapshot_t.new(mds)
        snapshots.append(new)
    return snapshots[-1].datetime + mds.snapshot_period - now

def do_replicate(mds):
    return datetime.timedelta.max

def do_prune(mds):
    return datetime.timedelta.max

def main():
    args = parser.parse_args()

    if args.verbose >= LOG_TRACE:
        print(f'Args are: {args}')

    with open(args.config_file_name) as json_file:
        conf = config.from_json(json.load(json_file))
    if args.verbose >= LOG_TRACE:
        print(f'config is: {conf}')

    while True:
        next_action = datetime.timedelta.max

        for ele in conf.managed_datasets:
            next_action = min(next_action, do_snapshot(ele))
            next_action = min(next_action, do_replicate(ele))
            next_action = min(next_action, do_prune(ele))

        print(f"sleeping for {next_action.total_seconds()} seconds")
        time.sleep(next_action.total_seconds())

if __name__ == "__main__":
   main()
