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

def timedelta_from_dict(d):
    years = d.pop('years', None)
    months = d.pop('months', None)

    d.setdefault('seconds', 0)
    if years is not None:
        d['seconds'] += years*SECONDS_PER_SOLAR_YEAR
    if months is not None:
        d['seconds'] += int(months*SECONDS_PER_SOLAR_YEAR/12)
    return datetime.timedelta(**d)

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

@dataclasses.dataclass(frozen=True)
class replica_t:
    remote_host: typing.Optional[str] = dataclasses.field()
    ssh_port:typing.Optional[int] = dataclasses.field()
    ssh_identity_file:typing.Optional[str] = dataclasses.field()
    pool:str = dataclasses.field()
    dataset:str = dataclasses.field()
    windows: typing.List[window_t] = dataclasses.field()
    snapshot_prefix: str = dataclasses.field(default='ZAM-')
    date_fstring: str = dataclasses.field(default='%Y-%m-%dT%H:%M:%S')
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

        ssh=['ssh']
        if self.ssh_port is not None:
            ssh += ['-p', str(self.ssh_port)]
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
        output = cmd.stdout.decode('utf-8')
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
        output = completed.stdout.decode('utf-8')

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
        raise Exception('not implemented')

@dataclasses.dataclass(frozen=True) #, order=True)
class managed_dataset_t:
    source: replica_t = dataclasses.field()
    destinations: typing.List[replica_t] = dataclasses.field()

    snapshot_period: datetime.timedelta = dataclasses.field()
    replication_period: datetime.timedelta = dataclasses.field()
    prune_period: datetime.timedelta = dataclasses.field()

    '''If true, not only will the source dataset be cloned but also all descendent datasets'''
    recursive: bool = dataclasses.field(default=True)

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

@dataclasses.dataclass(frozen=True)
class config_t:
    managed_datasets: typing.List[managed_dataset_t] = dataclasses.field()

primitive_map = {
    datetime.timedelta: timedelta_from_dict,
    str: str,
}

def object_from_dict(type_, value):
    if type_ in primitive_map:
        try:
            return primitive_map[type_](value)
        except Exception as e:
            import pdb; pdb.set_trace()
            pass
    if hasattr(type_, '__origin__'):
        if type_.__origin__ is typing.Union:
            assert(len(type_.__args__) == 2)
            assert(type_.__args__[1] is type(None))
            return object_from_dict(type_.__args__[0], value)
        if type_.__origin__ is list:
            assert(len(type_.__args__) == 1)
            subtype = type_.__args__[0]
            return [ object_from_dict(subtype, x) for x in value ]

    assert(type(value) == dict)
    assert(hasattr(type_, '__dataclass_fields__'))
    dct = value
    # the args to pass to type_' constructor
    args={}

    # VALIDATE FIELD NAMES
    expected_names = set(type_.__dataclass_fields__.keys())
    actual_names = set(dct.keys())
    extra_names = actual_names - expected_names
    if len(extra_names) > 0:
        raise Exception(f'Illegal keys in {type_.__name__}: {extra_names}')
    questionable_names = expected_names - actual_names
    missing_names = []
    for name in questionable_names:
        field = type_.__dataclass_fields__[name]
        # if field is an instance of typing.Optional, don't add it to missing_names
        if hasattr(field.type, '__origin__') and field.type.__origin__ is typing.Union:
            args[name] = None
            continue
        # if the field does not have a default value, add it to missing_names
        if isinstance(field.default, dataclasses._MISSING_TYPE): # would it be possible / safer to do something like field.default.__class__.__module__ == dataclasses?
            missing_names.append(name)
    if len(missing_names) > 0:
        raise Exception(f'Missing keys for {type_.__name__}: {missing_names}')

    # PARSE FIELDS
    for name in actual_names:
        field = type_.__dataclass_fields__[name]
        args[name] = object_from_dict(field.type, dct[name])

    # CONSTRUCT
    return type_(**args)

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

default_config_fname='zam_config.json'

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
parser.add_argument('--config', '-c', dest='config_file_name', action='store', default=default_configs, help='The location of the script\'s configuration file')
parser.add_argument('--verbose', '-v', dest='log_level', action='append_const', const=1, default=[], help='Increases verbosity. Can be used multiple times.')
parser.add_argument('--quiet', '-q', dest='log_level', action='append_const', const=-1, help='Decreases verbosity. Can be used multiple times.')



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
        conf = object_from_dict(config_t, json.load(json_file))
    log_t(f'config is: {conf}')

    # note that we don't have to worry about the do_* functions running
    # concurrently because they are not asyncronous; only async_loop and main
    # are async.
    tasks = [
        asyncio.create_task(async_loop(do_snapshot, conf.managed_datasets), name='snapshot'),
        asyncio.create_task(async_loop(do_replicate, conf.managed_datasets), name='replicate'),
        asyncio.create_task(async_loop(do_prune, conf.managed_datasets), name='prune'),
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

if __name__ == '__main__':
    asyncio.run(main())
