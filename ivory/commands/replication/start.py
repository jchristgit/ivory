"""Start logical replication."""

import argparse
import logging
import shlex

from ivory import constants
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add start command-specific arguments."""

    parser.add_argument(
        '--fail-on-already-started',
        help="Exit with code 1 if the subscription is already started.",
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--subscription-name',
        help="The name of the subscription on the target database.",
        default=constants.DEFAULT_SUBSCRIPTION_NAME,
    )
    parser.add_argument(
        '--no-refresh',
        help=(
            "By default, ivory will run `REFRESH PUBLICATION` after enabling "
            "the subscription (except if the subscription is already active). "
            "This switch will skip running it."
        ),
        default=False,
        action='store_true',
    )


async def run(args: argparse.Namespace) -> int:
    """Start logical replication if it is not already started."""

    target_db = await db.connect_single(args, kind='target')

    subscription = await target_db.fetchrow(
        "SELECT * FROM pg_subscription WHERE subname = $1", args.subscription_name
    )

    if subscription is None:
        log.error("No subscription with name %r found.", args.subscription_name)
        return 1

    elif subscription['subenabled']:
        log.info("Subscription is already started.")
        if args.fail_on_already_started:
            return 1

    else:
        await target_db.execute(
            f"ALTER SUBSCRIPTION {shlex.quote(args.subscription_name)} ENABLE"
        )
        if not args.no_refresh:
            await target_db.execute(
                f"ALTER SUBSCRIPTION {shlex.quote(args.subscription_name)} REFRESH PUBLICATION"
            )
        log.info("Subscription started.")

    return 0
