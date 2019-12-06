import os.path
import shlex
from typing import Callable, Optional, Type, TypeVar


T = TypeVar('T')


def expanded_value(
    type_: Type[T] = str, default: Optional[T] = None
) -> Callable[[str], T]:
    """Expand the given value, if possible.

    Examples:

        >>> expander = expanded_value(type_=int, default=5432)
        >>> expander('$SOME_UNKNOWN_VARIABLE')
        5432
        >>> expander('5433')
        5433
    """

    def expander(value: str) -> T:
        if os.path.expandvars(value) == value and '$' in value:
            return default
        return type_(os.path.expandvars(value))  # type: ignore

    return expander


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
