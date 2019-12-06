"""Check whether databases are ready."""

import argparse
import ast
import logging
import sys
import webbrowser

from ivory import check
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add check command-specific arguments."""

    parser.add_argument(
        '--no-webbrowser',
        help=(
            "Do not open a webbrowser displaying schema differences "
            "if present. By default, ivory will only open a webbrowser "
            "when standard output is a terminal."
        ),
        action='store_true',
        default=not sys.stdout.isatty(),
    )


async def run(args: argparse.Namespace) -> int:
    """Verify that databases are ready for the replication process.

    This runs a collection of checks that report any issues. By default,
    passed checks are not reported, use logging level DEBUG to view the
    output of passed checks.

    If a difference is found in the Schema, a webbrowser will be opened
    to inspect it. See the `--no-webbrowser` flag for details.

    Exits with code 0 if all checks passed. Otherwise, exits with code 1.
    """

    rc = 0

    (source_db, target_db) = await db.connect(args)

    async for result in check.find_problems(
        source_db=source_db, target_db=target_db, args=args
    ):
        if result.error is None:
            log.debug(result.description)
        else:
            if result.checker == 'check_schema_sync' and not args.no_webbrowser:
                *_, quoted_filename = result.error.split()
                filename = ast.literal_eval(quoted_filename)
                webbrowser.open(filename)

            log.error("%s: %s.", result.checker, result.error)
            rc = 1

    return rc
