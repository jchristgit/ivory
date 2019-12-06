"""Drop logical replication from the source to the target database."""

import argparse
import shlex
import logging

from ivory import constants
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add command-specific arguments."""

    parser.add_argument(
        '--publication-name',
        help="The name of the publication on the source database.",
        default=constants.DEFAULT_PUBLICATION_NAME,
    )
    parser.add_argument(
        '--subscription-name',
        help="The name of the subscription on the target database.",
        default=constants.DEFAULT_SUBSCRIPTION_NAME,
    )


async def run(args: argparse.Namespace) -> int:
    """Drop logical replication between the source and target database.

    If no active replication is found, nothing is done.
    """

    (source_db, target_db) = await db.connect(args)

    subscription = await target_db.fetchrow(
        "SELECT * FROM pg_catalog.pg_subscription WHERE subname = $1",
        args.subscription_name,
    )
    publication = await source_db.fetchrow(
        "SELECT * FROM pg_catalog.pg_publication WHERE pubname = $1",
        args.publication_name,
    )

    if subscription is None and publication is None:
        log.info("Replication already disabled.")

    if subscription is not None:
        await target_db.execute(
            f"DROP SUBSCRIPTION {shlex.quote(args.subscription_name)}"
        )
        log.info("Dropped subscription on target database.")

    if publication is not None:
        await source_db.execute(
            f"DROP PUBLICATION {shlex.quote(args.publication_name)}"
        )
        log.info("Dropped publication on source database.")

    replication_user = await source_db.fetchrow(
        "SELECT * FROM pg_catalog.pg_user WHERE usename = $1",
        constants.REPLICATION_USERNAME,
    )

    if replication_user is not None:
        await source_db.execute(
            f"DROP USER {shlex.quote(constants.REPLICATION_USERNAME)}"
        )

        log.info(
            "Dropped replication user %r on source database.",
            constants.REPLICATION_USERNAME,
        )

    return 0
