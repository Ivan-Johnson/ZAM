import typing
import datetime
from zam.config import managed_dataset_t


class pruner:
    def __init__(self, dataset: managed_dataset_t):
        pass

    def run(self) -> typing.Optional[datetime.datetime]:
        return None
