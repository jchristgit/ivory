"""Manages PostgreSQL upgrades via logical replication."""

import asyncio
import logging
import os.path
import sys

from . import cli


def main() -> int:
    logging.basicConfig(
        format='%(asctime)s | %(levelname)-5s | %(name)-20s | %(message)s',
        level=logging.INFO,
    )
    parser = cli.make_parser(description=__doc__)
    args = parser.parse_args()
    coroutine = args.func(args)
    return asyncio.run(coroutine)


if __name__ == '__main__':
    sys.exit(main())
