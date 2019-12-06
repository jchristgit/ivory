import shlex


def quote(value: str) -> str:
    """Always quote the given value.

    Example:
        >>> quote('foo')
        "'foo'"
        >>> quote('foo bar')
        "'foo bar'"
    """

    maybe_quoted = shlex.quote(value)
    if "'" not in maybe_quoted:
        return f"'{value}'"
    return maybe_quoted
