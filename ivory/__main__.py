"""Manages PostgreSQL upgrades via logical replication."""

import asyncio
import sys

from . import cli


def main() -> int:
    parser = cli.make_parser(description=__doc__)
    args = parser.parse_args()
    coroutine = args.func(args)
    return asyncio.run(coroutine)


if __name__ == '__main__':
    sys.exit(main())
