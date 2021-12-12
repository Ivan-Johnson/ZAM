import datetime
import logging
from zam.config import managed_dataset_t


class snapshoter:
    def __init__(self, dataset: managed_dataset_t):
        self.dataset = dataset

    def run(self) -> datetime.datetime:
        snapshots = self.dataset.source.list()
        if (
            len(snapshots) == 0
            or datetime.datetime.utcnow() - snapshots[-1].datetime
            > self.dataset.snapshot_period
        ):
            logging.info(f"Taking snapshot on {self.dataset.source}")
            snapshots.append(self.dataset.take_snapshot())
        return snapshots[-1].datetime + self.dataset.snapshot_period
