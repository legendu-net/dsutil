"""Date/time related utils.
"""
from typing import Iterable
import datetime
import pandas as pd

# import pysnooper
DATE_FORMAT_DASH = "%Y-%m-%d"
DATE_FORMAT_DIGIT = "%Y%m%d"
TIME_FORMAT_DASH = "%Y-%m-%d %H:%M:%S"
_MAPPING = {
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
    "Sun": 6,
}


def range_str(
    start, stop, *, step=datetime.timedelta(days=1), fmt: str = TIME_FORMAT_DASH
) -> Iterable[str]:
    """Generate datetime range as str.

    :param start: A datetime object or a string that can be parsed into a datetime.
    :param stop: A datetime object or a string that can be parsed into a datetime.
    :param step: A timedelta object specifying how much the values in the sequence increase at each step.
    :param fmt: The format of date/time (defaults to TIME_FORMAT_DASH)
    :yield: A generator of datetime in string format.
    """
    for ts in range(start=start, stop=stop, step=step):
        yield ts.strftime(fmt)


def range(start, stop, step=datetime.timedelta(days=1)) -> Iterable[datetime.datetime]:
    """Generate a range of datetime objects.

    :param start: A datetime object or a string that can be parsed into a datetime.
    :param stop: A datetime object or a string that can be parsed into a datetime.
    :param step: A timedelta object specifying how much the values in the sequence increase at each step.
    :yield: A generator of datetime objects.
    """
    start = pd.to_datetime(start)
    stop = pd.to_datetime(stop)
    curr_dt = start
    while curr_dt < stop:
        yield curr_dt
        curr_dt += step


def _format_weekday(weekday: int | str) -> int:
    if isinstance(weekday, str):
        weekday = _MAPPING[weekday]
    return weekday


def last_weekday(
    date: datetime.date, weekday: int | str, exclude_date: bool = False
) -> datetime.date:
    """Get the date of the last occurrence of the specified weekday.

    :param date: The date from when to date back.
    :param weekday: An integer (0 stands for Monday)
        or the name (full or 3-letter abbreviation) of a weekday/weekend.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of specified last weekday.
    """
    diff = date.weekday() - _format_weekday(weekday)
    if diff < 0:
        diff += 7
    if exclude_date and diff == 0:
        diff = 7
    return date - datetime.timedelta(days=diff)


def last_monday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Monday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Monday.
    """
    return last_weekday(date=date, weekday="Mon", exclude_date=exclude_date)


def last_tuesday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Tuesday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Tuesday.
    """
    return last_weekday(date=date, weekday="Tue", exclude_date=exclude_date)


def last_wednesday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Wednesday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Wednesday.
    """
    return last_weekday(date=date, weekday="Wed", exclude_date=exclude_date)


def last_thursday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Thursday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Thursday.
    """
    return last_weekday(date=date, weekday="Thu", exclude_date=exclude_date)


def last_friday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Friday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Friday.
    """
    return last_weekday(date=date, weekday="Fri", exclude_date=exclude_date)


def last_saturday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Saturday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Saturday.
    """
    return last_weekday(date=date, weekday="Sat", exclude_date=exclude_date)


def last_sunday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the last occurrence of Sunday.

    :param date: The date from when to date back.
    :param exclude_date: Whether to exclude `date` when dating back.
    :return: The date of last Sunday.
    """
    return last_weekday(date=date, weekday="Sun", exclude_date=exclude_date)


def next_weekday(
    date: datetime.date, weekday: int | str, exclude_date: bool = False
) -> datetime.date:
    """Get the date of the next specified weekday.

    :param date: The date from when to date forward.
    :param weekday: An integer (0 stands for Monday)
        or the name (full or 3-letter abbreviation) of a weekday/weekend.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next weekday.
    """
    diff = _format_weekday(weekday) - date.weekday()
    if diff < 0:
        diff += 7
    if exclude_date and diff == 0:
        diff = 7
    return date + datetime.timedelta(days=diff)


def next_monday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Monday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Monday.
    """
    return next_weekday(date=date, weekday="Mon", exclude_date=exclude_date)


def next_tuesday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Tuesday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Tuesday.
    """
    return next_weekday(date=date, weekday="Tue", exclude_date=exclude_date)


def next_wednesday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Wednesday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Wednesday.
    """
    return next_weekday(date=date, weekday="Wed", exclude_date=exclude_date)


def next_thursday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Thursday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Thursday.
    """
    return next_weekday(date=date, weekday="Thu", exclude_date=exclude_date)


def next_friday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Friday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Friday.
    """
    return next_weekday(date=date, weekday="Fri", exclude_date=exclude_date)


def next_saturday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Saturday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Saturday.
    """
    return next_weekday(date=date, weekday="Sat", exclude_date=exclude_date)


def next_sunday(date: datetime.date, exclude_date: bool = False) -> datetime.date:
    """Get the date of the next Sunday.

    :param date: The date from when to date forward.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Sunday.
    """
    return next_weekday(date=date, weekday="Sun", exclude_date=exclude_date)


def this_weekday(date: datetime.date, weekday: int | str) -> datetime.date:
    """Get the date of the specified weekday in the week of date.

    :param date: The date whose week to look for the specified weekday.
    :param weekday: An integer (0 stands for Monday)
        or the name (full or 3-letter abbreviation) of a weekday/weekend.
    :return: The date of the next weekday.
    """
    diff = _format_weekday(weekday) - date.weekday()
    return date + datetime.timedelta(days=diff)


def this_monday(date: datetime.date) -> datetime.date:
    """Get the date of Monday in the week of date.

    :param date: The date whose week to look for Monday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Monday.
    """
    return this_weekday(date=date, weekday="Mon")


def this_tuesday(date: datetime.date) -> datetime.date:
    """Get the date of Tuesday in the week of date.

    :param date: The date whose week to look for Tuesday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Tuesday.
    """
    return this_weekday(date=date, weekday="Tue")


def this_wednesday(date: datetime.date) -> datetime.date:
    """Get the date of Wednesday in the week of date.

    :param date: The date whose week to look for Wednesday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Wednesday.
    """
    return this_weekday(date=date, weekday="Wed")


def this_thursday(date: datetime.date) -> datetime.date:
    """Get the date of Thursday in the week of date.

    :param date: The date whose week to look for Thursday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Thursday.
    """
    return this_weekday(date=date, weekday="Thu")


def this_friday(date: datetime.date) -> datetime.date:
    """Get the date of Friday in the week of date.

    :param date: The date whose week to look for Friday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Friday.
    """
    return this_weekday(date=date, weekday="Fri")


def this_saturday(date: datetime.date) -> datetime.date:
    """Get the date of Saturday in the week of date.

    :param date: The date whose week to look for Saturday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Saturday.
    """
    return this_weekday(date=date, weekday="Sat")


def this_sunday(date: datetime.date) -> datetime.date:
    """Get the date of Sunday in the week of date.

    :param date: The date whose week to look for Sunday.
    :param exclude_date: Whether to exclude `date` when dating forward.
    :return: The date of the next Sunday.
    """
    return this_weekday(date=date, weekday="Sun")
