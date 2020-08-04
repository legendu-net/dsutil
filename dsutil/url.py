"""URL related utility functions.
"""


def format(url: str, text: str = "", type_="html") -> str:
    """Convert an URL to be a HTML hyperlink or a hyperlink formula in Excel so that it can be opened by clicking.
    :param url: The url to be converted to a hyperlink.
    :param text: The text to display. 
    :param type_: The type (html or excel) of hyperlink.
    """
    if not text:
        text = url
    type_ = type_.strip().lower()
    if type_ == "html":
        return f'<a href="{url}" target="_blank"> {text} </a>'
    if type_ == "excel":
        return f'=HYPERLINK("{url}", "{text}")'
    return url
