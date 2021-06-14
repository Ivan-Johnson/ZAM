# ZFS Automatic Manager

ZAM is a short (&lt;1k LOC) python script for maintaining a ZFS file system.

AFAIK, the official Arch repositories lack any tools for managing ZFS
(presumably due to ZFS and Linux having incompatible license), thus requiring
users to install unofficial packages for this purpose. ZAM is one such package,
with its primary selling point being that it is little more than a short script,
making it relatively easy for a developer to verify that it is not malicious.

ZAM is very much a work in progress, and the name is certainly not final.

Currently the only feature that ZAM supports is periodically taking snapshots
and replicating them to remote servers. ZAM is not even able to delete old
snapshots, although that feature is a top priority.
