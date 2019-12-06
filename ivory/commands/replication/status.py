"""Display replication status."""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Literal

from ivory import constants
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add status command-specific arguments."""

    parser.add_argument(
        '--subscription-name',
        help="The name of the subscription on the target database.",
        default=constants.DEFAULT_SUBSCRIPTION_NAME,
    )


async def run(args: argparse.Namespace) -> int:
    """Display the current status of replication."""

    rc = 0
    (source_db, target_db) = await db.connect(args)
    replication_stats = await source_db.fetchrow(
        """
        SELECT
            *
        FROM
            pg_stat_replication
        WHERE
            application_name = $1
            AND state IN ('catchup', 'streaming')
        """,
        constants.REPLICATION_APPLICATION_NAME,
    )

    if replication_stats is None:
        log.error("No active replication found.")
        return 1

    (my_lsn,) = await source_db.fetchrow('SELECT pg_current_wal_lsn()')

    # "The `dt` argument is ignored." But if you don't pass it, it crashes.
    reply_utcoffset = replication_stats['reply_time'].tzinfo.utcoffset(None)

    # timezones and unicode
    # horror
    sane_reply_time = replication_stats['reply_time'] - reply_utcoffset
    last_reply_delta = datetime.now(timezone.utc) - sane_reply_time
    if last_reply_delta > timedelta(minutes=5):
        log.error(
            "Last reply from standby received more than 5 minutes ago: %r.",
            last_reply_delta,
        )
        rc = 1
    else:
        log.info(
            "Last reply from standby received %d seconds ago.",
            last_reply_delta.total_seconds(),
        )

    if my_lsn == replication_stats['flush_lsn']:
        log.info("LSN matches.")
    else:
        log.warning(
            "Source is at LSN %r, standby at %r (diff %d).",
            my_lsn,
            replication_stats['flush_lsn'],
            my_lsn - replication_stats['flush_lsn'],
        )

    slot = await source_db.fetchrow(
        "SELECT * FROM pg_replication_slots WHERE slot_name = $1",
        args.subscription_name,
    )

    if slot is None:
        log.error("Missing replication slot with name %r.", args.subscription_name)
        rc = 1
    elif not slot['active']:
        log.error("Replication slot %r is not active.", args.subscription_name)
        rc = 1
    else:
        log.info("Replication slot is active.")

    state_sql = """
    SELECT
        pc.relname AS "name",
        psr.srsubstate AS "state"
    FROM
        pg_catalog.pg_subscription_rel AS psr
        JOIN pg_catalog.pg_subscription AS ps ON (psr.srsubid = ps.oid)
        JOIN pg_catalog.pg_class AS pc ON (psr.srrelid = pc.oid)
    WHERE
        ps.subname = $1
    """

    states = await target_db.fetch(state_sql, args.subscription_name)

    for (name, state) in states:
        if state != b'r':
            log.error(
                "Relation %r is not ready: %s (srsubstate=%r).",
                name,
                substate_to_human(state),
                state.decode(),
            )
            rc = 1

    return rc


def substate_to_human(value: Literal[b'i', b'd', b's', b'r']) -> str:
    """Convert PostgreSQL `srsubstate` to a human-readable value.

    Example:

        >>> substate_to_human(b'd')
        'data is being copied'
        >>> substate_to_human(b'x')  # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        ValueError: unknown value: b'x'
    """

    mapping = {
        b'i': 'initializing',
        b'd': 'data is being copied',
        b's': 'synchronized',
        b'r': 'ready (normal replication)',
    }
    if value in mapping:
        return mapping[value]
    raise ValueError(f"unknown value: {value!r}")
