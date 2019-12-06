"""Check whether databases are ready."""

import argparse
import logging

from ivory import check
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add check command-specific arguments."""


async def run(args: argparse.Namespace) -> int:
    """Verify that databases are ready for the replication process.

    This runs a collection of checks that report any issues. By default,
    passed checks are not reported, use logging level DEBUG to view the
    output of passed checks.

    Exits with code 0 if all checks passed. Otherwise, exits with code 1.
    """

    rc = 0

    (source_db, target_db) = await db.connect(
        source_dsn=args.source_dsn, target_dsn=args.target_dsn
    )

    async for result in check.find_problems(source_db=source_db, target_db=target_db):
        if result.error is None:
            log.debug(result.description)
        else:
            log.error("%s: %s.", result.checker, result.error)
            rc = 1

    return rc
