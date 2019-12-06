"""Display replication status."""

import argparse
import logging
from datetime import datetime, timedelta, timezone

from ivory import constants
from ivory import db


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add status command-specific arguments."""


async def run(args: argparse.Namespace) -> int:
    """Display the current status of replication."""

    rc = 0
    source_db = await db.connect_single(args, kind='source')
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
        log.error(
            "Source is at LSN %r, standby at %r.",
            my_lsn,
            replication_stats['flush_lsn'],
        )
        rc = 1

    return rc
