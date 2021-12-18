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

    def get_next_runtime(self) -> typing.Optional[datetime.datetime]:
        """Get the time to next run the task at.

        If this function returns None then this task will not be called again.

        If this function returns a datetime, then the task will be called again
        but not before that datetime. The returned datetime must be in the UTC
        timezone.
        """
        ...  # pragma: no cover


@dataclasses.dataclass
class __PrioritizedTask:
    start: typing.Optional[datetime.datetime]
    task: task

    def __lt__(self, other: __PrioritizedTask) -> bool:
        if self.start is None and other.start is None:
            return False
        if self.start is None:
            return True
        if other.start is None:
            return False
        return bool(self.start < other.start)


def __pt_from_task(task: task) -> __PrioritizedTask:
    return __PrioritizedTask(task.get_next_runtime(), task)


def run_tasks(tasks: typing.List[task]) -> None:
    prioritized_tasks = list(map(__pt_from_task, tasks))
    heapq.heapify(prioritized_tasks)
    heap = prioritized_tasks

    while len(heap) > 0:
        task = heap[0].task
        start = heap[0].start

        if start is None:
            heapq.heappop(heap)
            continue

        timedelta_until_start = start - datetime.datetime.utcnow()
        seconds_until_start = timedelta_until_start.total_seconds()
        time.sleep(max(0, seconds_until_start))

        now = datetime.datetime.utcnow()
        assert now >= start

        task.run()
        heapq.heapreplace(heap, __pt_from_task(task))
