"""Miscellaneous utils.
"""
from typing import Any, Sized


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
