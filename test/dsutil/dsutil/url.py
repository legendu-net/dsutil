def format(url: str, text: str = '', type='html'):
    """Convert an URL to be a HTML hyperlink or a hyperlink formula in Excel so that it can be opened by clicking.
    :param url: The url to be converted to a hyperlink.
    :param text: The text to display. 
    :param type: The type (html or excel) of hyperlink.
    """
    if not text:
        text = url
    type = type.strip().lower()
    if type == 'html':
        return f'<a href="{url}" target="_blank"> {text} </a>'
    if type == 'excel':
        return f'=HYPERLINK("{url}", "{text}")'
