"""Stop logical replication."""

import argparse
import logging
import shlex

from ivory import constants
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add stop command-specific arguments."""

    parser.add_argument(
        '--fail-on-already-stopped',
        help="Exit with code 1 if the subscription is already stopped.",
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--subscription-name',
        help="The name of the subscription on the target database.",
        default=constants.DEFAULT_SUBSCRIPTION_NAME,
    )


async def run(args: argparse.Namespace) -> int:
    """Stop logical replication if it is not already stopped."""

    target_db = await db.connect_single(args, kind='target')

    subscription = await target_db.fetchrow(
        "SELECT * FROM pg_subscription WHERE subname = $1", args.subscription_name
    )

    if subscription is None:
        log.error("No subscription with name %r found.", args.subscription_name)
        return 1

    elif not subscription['subenabled']:
        log.info("Subscription is already stopped.")
        if args.fail_on_already_stopped:
            return 1

    else:
        await target_db.execute(
            f"ALTER SUBSCRIPTION {shlex.quote(args.subscription_name)} DISABLE"
        )
        log.info("Subscription stopped.")

    return 0
