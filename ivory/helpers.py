import os.path
import shlex
from typing import Callable, Optional, Type, TypeVar


T = TypeVar('T')


def expanded_value(
    type_: Type[T] = str, default: Optional[T] = None
) -> Callable[[str], T]:
    def expander(value: str) -> T:
        if os.path.expandvars(value) == value:
            return default
        return type_(os.path.expandvars(value))  # type: ignore

    return expander


def quote(value: str) -> str:
    maybe_quoted = shlex.quote(value)
    if "'" not in maybe_quoted:
        return f"'{value}'"
    return maybe_quoted
