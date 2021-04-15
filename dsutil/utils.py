"""Miscellaneous utils.
"""
from typing import Any, Sized, Callable
import time


def to_bool(value: Any) -> bool:
    """Convert an object to a bool value (True or False).

    :param value: any object that can be converted to a bool value.
    :return: True or False.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("t", "true", "y", "yes"):
            return True
        if value.isdigit():
            return int(value) != 0
        return False
    if isinstance(value, int) and value != 0:
        return True
    if isinstance(value, Sized) and len(value) > 0:
        return True
    return False


def retry(task: Callable,
                  times: int = 3,
                  wait_seconds: float = 60):
    """Retry a Docker API on failure (for a few times).
    :param task: The task to run.
    :param times: The total number of times to retry.
    :param wait_seconds: The number of seconds to wait before retrying.
    :return: The return result of the task.
    """
    if times <= 1:
        return task()
    for _ in range(times):
        try:
            return task()
        except:
            time.sleep(wait_seconds)
    return task()
