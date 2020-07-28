import re
import os
from typing import Union, Iterable
import datetime
import pandas as pd
#import pysnooper

DATE_FORMAT_DASH = '%Y-%m-%d'
DATE_FORMAT_DIGIT = '%Y%m%d'
TIME_FORMAT_DASH = '%Y-%m-%d %H:%M:%S'


def range_str(
    start,
    stop,
    *,
    step=datetime.timedelta(days=1),
    format: str = TIME_FORMAT_DASH
) -> Iterable[str]:
    for ts in range(start=start, stop=stop, step=step):
        yield ts.strftime(format)


def range(start, stop, step=datetime.timedelta(days=1)) -> Iterable[datetime.datetime]:
    """Generate a range of datetime objects.
    :param start: A datetime object or a string that can be parsed into a datetime.
    :param stop: A datetime object or a string that can be parsed into a datetime.
    :param step: A timedelta object specifying how much the values in the sequence increase at each step. 
    """
    start = pd.to_datetime(start)
    stop = pd.to_datetime(stop)
    curr_dt = start
    while curr_dt < stop:
        yield curr_dt
        curr_dt += step


def last_weekday(weekday) -> datetime.date:
    """Get the date of latest occurrence of the specified weekday.
    """
    mapping = {
        "Monday": 0,
        "Mon": 0,
        "Tuesday": 1,
        "Tue": 1,
        "Wednesday": 2,
        "Wed": 2,
        "Thursday": 3,
        "Thu": 3,
        "Friday": 4,
        "Fri": 4,
        "Saturday": 5,
        "Sat": 5,
        "Sunday": 6,
        "Sun": 6
    }
    if isinstance(weekday, str):
        weekday = mapping[weekday]
    today = datetime.date.today()
    diff = today.weekday() - weekday
    if diff < 0:
        diff += 7
    return today - datetime.timedelta(days=diff)


def last_monday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Mon')


def last_tuesday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Tue')


def last_wednesday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Wed')


def last_thursday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Thu')


def last_friday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Fri')


def last_saturday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Sat')


def last_sunday() -> datetime.date:
    """Get the date of latest occurrence of Monday.
    """
    return last_weekday('Sun')


def today(days: int = 0) -> datetime.date:
    """Return the date of days after today as a date.
    """
    return datetime.date.today() + datetime.timedelta(days=days)


def today_dash(days: int = 0) -> str:
    """Return the date of days after today as a dash separated string.
    """
    return today(days).strftime(DATE_FORMAT_DASH)


def today_digit(days: int = 0) -> str:
    """Return the date of days after today as a 8-digit string.
    """
    return today(days).strftime(DATE_FORMAT_DIGIT)
