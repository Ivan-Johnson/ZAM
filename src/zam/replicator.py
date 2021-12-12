from zam.config import managed_dataset_t
import datetime
import logging


class replicator:
    def __init__(self, dataset: managed_dataset_t):
        self.dataset = dataset

    def run(self) -> datetime.datetime:
        src = self.dataset.source
        snapshots_s = src.list()
        assert len(snapshots_s) > 0

        for dest in self.dataset.destinations:
            if not dest.exists():
                snapshot = snapshots_s[0]
                logging.info(f"Initializing {dest} with {snapshot} from {src}")
                self.dataset.source.clone_to(dest, None, snapshot)
            snapshots_d = dest.list()

            for previous, current in zip(snapshots_s, snapshots_s[1:]):
                assert previous in snapshots_d
                if not current in snapshots_d:
                    logging.info(f"Cloning {current} from {src} to {dest}")
                    self.dataset.source.clone_to(dest, previous, current)
                    snapshots_d.append(current)

        return datetime.datetime.utcnow() + self.dataset.replication_period
