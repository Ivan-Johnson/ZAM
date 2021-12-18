from zam.config import managed_dataset_t
import datetime
import logging
import typing


class snapshoter:
    def __init__(self, dataset: managed_dataset_t):
        self.dataset = dataset

    def get_next_runtime(self) -> typing.Optional[datetime.datetime]:
        snapshots = self.dataset.source.list()
        return snapshots[-1].datetime + self.dataset.snapshot_period

    def run(self) -> datetime.datetime:
        logging.info(f"Taking snapshot on {self.dataset.source}")
        self.dataset.take_snapshot()
