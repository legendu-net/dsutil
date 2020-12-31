#!/usr/bin/env python3
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


def get_memory_usage(user: str = USER):
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
            p.memory_info().rss for p in psutil.process_iter()
            if p.username() == USER and p.status() in STATUS
        )
    except:
        return get_memory_usage(user)


def monitor_memory_usage(seconds: float = 1, user: str = USER):
    while True:
        time.sleep(seconds)
        logger.info("Memory used by {}: {:,}", user, get_memory_usage(user=user))


def match_memory_usage(
    target: float,
    arr_size: int = 1_000_000,
    sleep_min: float = 1,
    sleep_max: float = 30
):
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
            USER, mem, size * len(dq)
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
    """
    parser = ArgumentParser(
        description="Make memory consumption match the specified target."
    )
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument(
        "-g",
        dest="target",
        type=lambda s: int(s) * 1073741824,
        help="Specify target memory in gigabytes."
    )
    mutex.add_argument(
        "-m",
        dest="target",
        type=lambda s: int(s) * 1048576,
        help="Specify target memory in megabytes."
    )
    return parser.parse_args(args=args, namespace=namespace)


def main():
    args = parse_args()
    match_memory_usage(args.target)


if __name__ == "__main__":
    main()
