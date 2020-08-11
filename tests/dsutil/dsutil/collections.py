from typing import Callable, Any


def format_item_html(key: Any, value: Any) -> str:
    """Format a (key, value) pair of a dictionary in HTML format.

    :param key: An key of the dictionary.
    :param value: The corresponding value of the key.
    """
    return '&nbsp;' * 4 + f'{key}: {value}'


def format_item_plain(key: Any, value: Any) -> str:
    """Format a (key, value) pair of a dictionary in HTML format.

    :param key: An key of the dictionary.
    :param value: The corresponding value of the key.
    """
    return ' ' * 4 + f'{key}: {value}'


def format_dict_html(
    dict_: dict,
    fmt: Callable[[Any, Any], str] = format_item_html,
    filter_: Callable[[Any, Any], bool] = lambda key, value: True
):
    """Format a dict in HTML format for pretty printing.

    :param dict_: The dictionary to format.
    :param fmt: A function to format a (key, value) pair.
    :param filter_: A filtering function to select items from the dictionary.
    :is_html: whether to format the dictionary in HTML format or in plain text format.
    """
    lines = (fmt(k, v) for k, v in dict_.items() if filter_(k, v))
    return '{<br>' + '<br>'.join(lines) + '<br>}'


def format_dict_plain(
    dict_: dict,
    fmt: Callable[[Any, Any], str] = format_item_plain,
    filter_: Callable[[Any, Any], bool] = lambda key, value: True
):
    """Format a dict for pretty printing.

    :param dict_: The dictionary to format.
    :param fmt: A function to format a (key, value) pair.
    :param filter_: A filtering function to select items from the dictionary.
    :is_html: whether to format the dictionary in HTML format or in plain text format.
    """
    lines = (fmt(k, v) for k, v in dict_.items() if filter_(k, v))
    return '{\n' + '\n'.join(lines) + '\n}'
