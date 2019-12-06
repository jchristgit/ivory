"""Manages PostgreSQL upgrades via logical replication."""

import asyncio
import logging
import os.path
import sys

from . import cli


def main() -> int:
    parser = cli.make_parser(description=__doc__)
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s',
        level=getattr(logging, args.log_level),
    )
    coroutine = args.func(args)
    return asyncio.run(coroutine)


if __name__ == '__main__':
    sys.exit(main())
