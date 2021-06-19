#!/bin/python
import argparse
import asyncio
import collections
import dataclasses
import datetime
import json
import os
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

def pretty_check_returncode(returncode:typing.Optional[int], stderr:bytes, errmsg:str):
    if returncode is not None and returncode != 0:
        print(f'{errmsg}. stderr:')
        string = stderr.decode('utf-8')
        print('\t' + string.replace('\n', '\n\t').rstrip('\n\t'))
        raise Exception(errmsg)

@dataclasses.dataclass(frozen=True, order=True)
class snapshot_t:
    datetime:datetime.datetime

    def __post_init__(self):
        now=datetime.datetime.utcnow()
        if self.datetime > now:
            raise ValueError('A snapshot with a future date exists')
    def __str__(self):
        return f'snapshot_t({self.datetime})'

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

@dataclasses.dataclass(frozen=True)
class replica_t:
    remote_host: typing.Optional[str] = dataclasses.field()
    ssh_port:typing.Optional[int] = dataclasses.field()
    ssh_identity_file:typing.Optional[str] = dataclasses.field()
    pool:str = dataclasses.field()
    dataset:str = dataclasses.field()
    windows: typing.Tuple[window_t] = dataclasses.field()
    snapshot_prefix: str = dataclasses.field()
    date_fstring: str = dataclasses.field()
    def __post_init__(self):
        if list(self.windows) != sorted(list(self.windows), key=lambda x: x.max_age or datetime.timedelta.max):
            raise ValueError(f'{self}\'s windows are not sorted by max_age')
        if list(self.windows) != sorted(list(self.windows), key=lambda x: x.period):
            raise ValueError(f'{self}\'s window periods are not monotonically increasing')
    def __str__(self):
        return f'replica_t({self.remote_host}, {self.pool}, {self.dataset})'

    def get_snapshot_full_name(self, snapshot:snapshot_t):
        return f'{self.pool}/{self.dataset}@{self.snapshot_prefix}{snapshot.datetime.strftime(self.date_fstring)}'

    def __get_ssh_cmd(self):
        if self.remote_host is None:
            return []

        ssh=["ssh"]
        if self.ssh_port is not None:
            ssh += ["-p", str(self.ssh_port)]
        if self.ssh_identity_file is not None:
            ssh += ['-i', self.ssh_identity_file]
        ssh += [self.remote_host]
        return ssh

    def run(self, args, **kwargs):
        args = self.__get_ssh_cmd() + args
        log_t(f'Doing `run`: {args} with {kwargs}')
        return subprocess.run(args, **kwargs)

    def popen(self, args, **kwargs):
        args = self.__get_ssh_cmd() + args
        log_t(f'Doing `popen`: {args} with {kwargs}')
        return subprocess.Popen(args, **kwargs)

    def exists(self):
        dataset_full_name=f'{self.pool}/{self.dataset}'
        cmd = self.run(['zfs', 'list', '-o', 'name'], capture_output=True)
        pretty_check_returncode(cmd.returncode, cmd.stderr, '`zfs list` failed when checking if {self} exists')
        output = cmd.stdout.decode("utf-8")
        lines = output.split('\n')
        assert(lines[0] == 'NAME')
        lines = lines[1:]
        return dataset_full_name in lines

    def list(self):
        # step 1: call `zfs list -t snapshot {self}`
        dataset_full_name=f'{self.pool}/{self.dataset}'
        completed = self.run(['zfs', 'list', '-t', 'snapshot', dataset_full_name, '-o', 'name'], capture_output=True)
        pretty_check_returncode(completed.returncode, completed.stderr, f'Failed to list snapshots of {self}')
        if (completed.stderr == b'no datasets available\n'):
            return []
        output = completed.stdout.decode("utf-8")

        # step 2: parse output
        lines = output.split('\n')
        assert(lines[0] == 'NAME')
        lines = lines[1:]
        prefix=f'{dataset_full_name}@'
        ret=[]
        for fullname in lines:
            if len(fullname) == 0:
                continue
            assert(fullname.startswith(prefix))
            name=fullname.removeprefix(prefix)

            if not name.startswith(self.snapshot_prefix):
                # Ignore non-ZAM snapshots
                continue

            time_s=name.removeprefix(self.snapshot_prefix)
            dt = datetime.datetime.strptime(time_s, self.date_fstring)
            snapshot=snapshot_t(datetime=dt)
            ret.append(snapshot)
            assert(self.get_snapshot_full_name(snapshot) == fullname)
        ret.sort()
        return ret

    # TODO: add type hint dest:replica_t
    def clone_to(self, dest, snapshot_old:snapshot_t, snapshot_new:snapshot_t):
        args_incremental=[]
        if snapshot_old is not None:
            args_incremental=['-i', self.get_snapshot_full_name(snapshot_old)]

        #TODO: send with --replicate or maybe --backup? similarly update receive
        cmd_source = ['zfs', 'send'] + args_incremental + ['--raw', '--verbose', f'{self.get_snapshot_full_name(snapshot_new)}']
        with self.popen(cmd_source, stdout=subprocess.PIPE) as popen_source:
            cmd_dest = ['zfs', 'recv', f'{dest.get_snapshot_full_name(snapshot_new)}']
            with dest.popen(cmd_dest, stdin=popen_source.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as popen_dest:
                while True: #do while
                    time.sleep(1) # TODO: unnecessarily slow for incremental?

                    #TODO inline ternerary is ugly -_-
                    status_source = popen_source.poll()
                    pretty_check_returncode(status_source, None if popen_source.stderr is None else popen_source.stderr.read(), f'zfs send failed {cmd_source}')
                    status_dest = popen_dest.poll()
                    pretty_check_returncode(status_dest, None if popen_dest.stderr is None else popen_dest.stderr.read(), f'zfs recv failed {cmd_dest}')
                    if status_source == 0 and status_dest == 0:
                        break

    def delete(self, dest):
        raise Exception("not implemented")

    @staticmethod
    def from_json(obj):
        try:
            remote_host:str = obj["remote-host"]
        except KeyError:
            remote_host = None
        try:
            ssh_port:int = int(obj["ssh_port"])
        except KeyError:
            ssh_port:int = None
        try:
            ssh_identity_file:str = obj["identity-file"]
        except KeyError:
            ssh_identity_file=None
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
            ssh_port=ssh_port,
            ssh_identity_file=ssh_identity_file,
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

    def take_snapshot(self):
        now=datetime.datetime.utcnow()
        snapshot:snapshot_t = snapshot_t(datetime=now)
        snapshot_fullname:str=self.source.get_snapshot_full_name(snapshot)

        command=['zfs', 'snapshot', snapshot_fullname]
        if self.recursive:
            command.append('-r')
        completed = self.source.run(command, capture_output=True)
        pretty_check_returncode(completed.returncode, completed.stderr, f'Failed to take snapshot on {self.source}')
        return snapshot

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

# TODO: make a config framework?
#
# A: it's a pain to have to maintain from_json functions
#
# B: I'd like configs to be able to define default values. e.g. a "dataset"
# defined in the top level JSON object would be used as the default dataset for
# all replicas in all managed_datasets. With a custom library, this could be
# implemented in __getattr__/__getattribute__ where if the value is not defined
# locally it recurses up the config struct to find a default value.
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

LOG_ERROR=1
LOG_WARNING=2
LOG_INFO=3
LOG_TRACE=4

def log(level, message):
    if args.log_level >= level:
        print(message)

def log_e(message):
    log(LOG_ERROR, message)
def log_w(message):
    log(LOG_WARNING, message)
def log_i(message):
    log(LOG_INFO, message)
def log_t(message):
    log(LOG_TRACE, message)

default_config_fname="zam_config.json"

#from highest to lowest precedence
default_configs=[
    f'/etc/{default_config_fname}',

    # /usr/local is for sysadmin installed files; other /usr directorys are from the package manager

    # host-specific configuration
    f'/usr/local/etc/{default_config_fname}',
    f'/usr/etc/{default_config_fname}',

    # architecture-independent configuration
    f'/usr/local/share/{default_config_fname}',
    f'/usr/share/{default_config_fname}',
]

parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', dest="config_file_name", action="store", default=default_configs, help="The location of the script's configuration file")
parser.add_argument('--verbose', '-v', dest="log_level", action="append_const", const=1, default=[], help="Increases verbosity. Can be used multiple times.")
parser.add_argument('--quiet', '-q', dest="log_level", action="append_const", const=-1, help="Decreases verbosity. Can be used multiple times.")



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
    snapshots=mds.source.list()
    if len(snapshots) == 0 or datetime.datetime.utcnow() - snapshots[-1].datetime > mds.snapshot_period:
        log_i(f'Taking snapshot on {mds.source}')
        snapshots.append(mds.take_snapshot())
    return snapshots[-1].datetime + mds.snapshot_period

def do_replicate(mds):
    src=mds.source
    snapshots_s=src.list()
    assert(len(snapshots_s) > 0)

    for dest in mds.destinations:
        if not dest.exists():
            snapshot=snapshots_s[0]
            log_i(f'Initializing {dest} with {snapshot} from {src}')
            mds.source.clone_to(dest, None, snapshot)
        snapshots_d=dest.list()

        for previous, current in zip(snapshots_s, snapshots_s[1:]):
            assert(previous in snapshots_d)
            if not current in snapshots_d:
                log_i(f'Cloning {current} from {src} to {dest}')
                mds.source.clone_to(dest, previous, current)
                snapshots_d.append(current)

    return datetime.datetime.utcnow() + mds.replication_period

def do_prune(mds):
    return datetime.datetime.max

async def async_loop(func, datasets):
    while True:
        next_action = min(map(func, datasets))
        num_sec=(next_action-datetime.datetime.utcnow()).total_seconds()
        await asyncio.sleep(max(1, num_sec))

async def main():
    # args must be global so that, e.g., the log function can access the log level
    global args
    args = parser.parse_args()
    args.log_level = LOG_INFO + sum(args.log_level)

    log_t(f'Args are: {args}')

    if isinstance(args.config_file_name, list):
        for fname in args.config_file_name:
            if os.path.isfile(fname):
                args.config_file_name = fname
                break
        else:
            print(f'None of the default config files exist ({default_configs})', file=sys.stderr)
            exit(1)

    with open(args.config_file_name) as json_file:
        conf = config.from_json(json.load(json_file))
    log_t(f'config is: {conf}')

    # note that we don't have to worry about the do_* functions running
    # concurrently because they are not asyncronous; only async_loop and main
    # are async.
    tasks = [
        asyncio.create_task(async_loop(do_snapshot, conf.managed_datasets), name="snapshot"),
        asyncio.create_task(async_loop(do_replicate, conf.managed_datasets), name="replicate"),
        asyncio.create_task(async_loop(do_prune, conf.managed_datasets), name="prune"),
    ]

    while True:
        for task in tasks:
            if task.done():
                exception = task.exception()
                if exception is not None:
                    print(f'A task raised an exception ({task}, {exception})')
                    raise exception
                else:
                    raise Exception(f'A task exited unexpectedly ({task})')
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
