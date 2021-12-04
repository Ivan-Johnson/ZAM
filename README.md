# ZFS Automatic Manager

ZAM is a python-based command-line tool for maintaining a ZFS file system.

ZAM is very much a work in progress, and the name is certainly not final.

Currently the only feature that ZAM supports is periodically taking snapshots
and replicating them to remote servers. ZAM is not even able to delete old
snapshots, although that feature is a top priority.


## Development Environment Setup

Run these commands from the root of the ZAM repository:

    python3 -m venv ".venv" --prompt "ZAM"
    source ".venv/bin/activate"
    .venv/bin/python3 -m pip install --upgrade pip
    pip install -e '.'
    pip install -r requirements_dev.txt
