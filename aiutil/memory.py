"""Memory related utils.
"""

import getpass
import sys
import math
from collections import deque
import time
from argparse import ArgumentParser, Namespace
import numpy as np
import psutil
from loguru import logger

USER = getpass.getuser()


def get_memory_usage(user: str = USER) -> int:
    """Get the memory usage of the specified user.

    :param user: The user whose memory usage to get.
    :return: The memory usage of the user in bytes.
    """
    STATUS = (
        psutil.STATUS_RUNNING,
        psutil.STATUS_SLEEPING,
        psutil.STATUS_DISK_SLEEP,
        psutil.STATUS_WAKING,
        psutil.STATUS_PARKED,
        psutil.STATUS_IDLE,
        psutil.STATUS_WAITING,
    )
    try:
        return sum(
            p.memory_info().rss
            for p in psutil.process_iter()
            if p.username() == USER and p.status() in STATUS
        )
    except:
        return get_memory_usage(user)


def monitor_memory_usage(seconds: float = 1, user: str = USER):
    """Log out the memory usage of the specified user in a specified frequency.

    :param seconds: The number of seconds to wait before the next logging.
    :param user: The user whose memory usage to monitor.
    """
    while True:
        time.sleep(seconds)
        logger.info("Memory used by {}: {:,}", user, get_memory_usage(user=user))


def match_memory_usage(
    target: float,
    arr_size: int = 1_000_000,
    sleep_min: float = 1,
    sleep_max: float = 30,
):
    """Match a user's memory usage to the specified value.
    The memory usage will gradually increase to the specified value
    if it is smaller than the specified value.
    Otherwise,
    the memory usage drops immediately to match the specified value.

    :param target: The target memory in bytes.
    :param arr_size: The size of integer arrays for consuming memory.
    :param sleep_min: The minimum time of sleeping.
    :param sleep_max: The maximum time of sleeping.
    """
    logger.info("Target memory: {:,.0f}", target)
    # define an template array
    arr = list(range(arr_size))
    size = sys.getsizeof(arr)
    # deque for consuming memory flexibly
    dq = deque()
    # define 2 points for linear interpolation of sleep seconds
    xp = (0, 10)
    yp = (sleep_max, sleep_min)
    while True:
        mem = get_memory_usage(USER)
        logger.info(
            "Current used memory by {}: {:,} out of which {:,} is contributed by the memory matcher",
            USER,
            mem,
            size * len(dq),
        )
        diff = (target - mem) / size
        if diff > 0:
            logger.info("Consuming more memory ...")
            dq.append(arr.copy())
            time.sleep(np.interp(diff, xp, yp))
        else:
            count = min(math.ceil(-diff), len(dq))
            logger.info("Releasing memory ...")
            for _ in range(count):
                dq.pop()
            time.sleep(np.interp(count, xp, yp))


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.

    :param args: The arguments to parse.
        If None, the arguments from command-line are parsed.
    :param namespace: An inital Namespace object.
    :return: A namespace object containing parsed options.
    """
    parser = ArgumentParser(
        description="Make memory consumption match the specified target."
    )
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument(
        "-g",
        dest="target",
        type=lambda s: int(s) * 1073741824,
        help="Specify target memory in gigabytes.",
    )
    mutex.add_argument(
        "-m",
        dest="target",
        type=lambda s: int(s) * 1048576,
        help="Specify target memory in megabytes.",
    )
    return parser.parse_args(args=args, namespace=namespace)


def main():
    """The main function for scripting usage."""
    args = parse_args()
    match_memory_usage(args.target)


if __name__ == "__main__":
    main()
