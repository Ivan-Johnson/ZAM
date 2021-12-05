#!/bin/python
# Remove after https://bugs.python.org/issue38605 is resolved (python 3.11?)
from __future__ import annotations

import dataclasses
import datetime
import heapq
import time
import typing


class task(typing.Protocol):
    """This protocol must be implemented by classes intended to be run by scheduler"""

    def run(self) -> typing.Optional[datetime.datetime]:
        """This function is called by the scheduler.

        If this function returns None then this task will not be called again.

        If this function returns a datetime, then it will be called again but
        not before that datetime. The returned datetime must be in the UTC
        timezone.
        """
        ...  # pragma: no cover


@dataclasses.dataclass(order=True)
class PrioritizedTask:
    start: datetime.datetime
    task: task = dataclasses.field(compare=False)


def run(tasks: typing.List[task]) -> None:
    heap = list(map(lambda task: PrioritizedTask(datetime.datetime.min, task), tasks))

    while len(heap) > 0:
        task = heap[0].task
        start = heap[0].start

        seconds_until_start = (start - datetime.datetime.utcnow()).total_seconds()
        time.sleep(max(0, seconds_until_start))

        now = datetime.datetime.utcnow()
        assert now >= start

        later = task.run()
        if later is None:
            heapq.heappop(heap)
        else:
            pt = PrioritizedTask(later, task)
            heapq.heapreplace(heap, pt)
