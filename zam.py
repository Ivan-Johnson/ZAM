#!/bin/python
import argparse
import collections
import dataclasses
import datetime
import json
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
    snapshot_prefix: str = dataclasses.field(default="ZAM-")

    def __post_init__(self):
        if list(self.windows) != sorted(list(self.windows), key=lambda x: x.max_age or datetime.timedelta.max):
            raise ValueError('Windows must be sorted')

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
        snapshot_prefix: str = dataclasses.field(default="ZAM-")
        return replica_t(
            remote_host=remote_host,
            pool=pool,
            dataset=dataset,
            windows=windows,
            snapshot_prefix=snapshot_prefix,
        )



@dataclasses.dataclass(frozen=True) #, order=True)
class managed_dataset:
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
        return managed_dataset(
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
            managed_datasets.append(managed_dataset.from_json(ele))
        return config(tuple(managed_datasets))

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

def main():
    args = parser.parse_args()

    if args.verbose >= LOG_TRACE:
        print(f'Args are: {args}')

    with open(args.config_file_name) as json_file:
        conf = config.from_json(json.load(json_file))
    if args.verbose >= LOG_TRACE:
        print(f'config is: {conf}')

if __name__ == "__main__":
   main()
