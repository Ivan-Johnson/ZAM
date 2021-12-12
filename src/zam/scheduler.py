#!/bin/python
# Remove after https://bugs.python.org/issue38605 is resolved (python 3.11?)
from __future__ import annotations

import dataclasses
import datetime
import heapq
import time
import typing


class task(typing.Protocol):
    """This protocol must be implemented by classes intended to be run by
    scheduler"""

    def run(self) -> None:
        """Run the task"""
        ...  # pragma: no cover

    def getNextRuntime(self) -> typing.Optional[datetime.datetime]:
        """Get the time to next run the task at.

        If this function returns None then this task will not be called again.

        If this function returns a datetime, then the task will be called again
        but not before that datetime. The returned datetime must be in the UTC
        timezone.
        """
        ...  # pragma: no cover


@dataclasses.dataclass(order=True)
class __PrioritizedTask:
    start: datetime.datetime
    task: task = dataclasses.field(compare=False)


def run(tasks: typing.List[task]) -> None:
    prioritized_tasks = list(
        map(lambda task: __PrioritizedTask(task.getNextRuntime(), task), tasks)
    )
    prioritized_tasks = list(
        filter(lambda pt: pt.start is not None, prioritized_tasks)
    )
    heapq.heapify(prioritized_tasks)
    heap = prioritized_tasks

    while len(heap) > 0:
        task = heap[0].task
        start = heap[0].start

        timedelta_until_start = start - datetime.datetime.utcnow()
        seconds_until_start = timedelta_until_start.total_seconds()
        time.sleep(max(0, seconds_until_start))

        now = datetime.datetime.utcnow()
        assert now >= start

        task.run()
        next_runtime = task.getNextRuntime()
        if next_runtime is None:
            heapq.heappop(heap)
        else:
            pt = __PrioritizedTask(next_runtime, task)
            heapq.heapreplace(heap, pt)
