"""Manages PostgreSQL logical replication."""

import asyncio
import logging
import sys
from typing import cast, Optional, List

from . import cli


def main(cmdline: Optional[List[str]] = None) -> int:
    parser = cli.make_parser(description=__doc__)
    args = parser.parse_args(cmdline)
    logging.basicConfig(
        format='%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s',
        level=getattr(logging, args.log_level),
    )
    coroutine = args.func(args)
    return cast(int, asyncio.run(coroutine))


if __name__ == '__main__':
    sys.exit(main())
