"""Synchronize sequence values."""

import asyncio
import argparse
import logging
from typing import Tuple

from ivory import db


log = logging.getLogger(__name__)


def sequence_offset(value: str) -> Tuple[str, int]:
    if ':' not in value:
        raise ValueError("expected colon-separated `sequence:offset` value")

    (sequence, offset) = value.split(':')
    return (sequence, int(offset))


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add syncsequences command-specific arguments."""

    offset_group = parser.add_argument_group('offset options')
    offset_group.add_argument(
        '--fixed-offset',
        help="Fixed offset to apply for all sequences.",
        type=int,
        default=100,
    )
    offset_group.add_argument(
        '--sequence-offset',
        help=(
            "Specify a fixed offset to use on a per-sequence basis. "
            "Offsets are specified in the form `sequence:offset`, for "
            "example `foo_id_seq:30`. By default, sequence offsets "
            "are determined automatically by taking a second sample "
            "after the first."
        ),
        action='append',
        type=sequence_offset,
        default=[],
        dest='fixed_offsets',
    )
    offset_group.add_argument(
        '--sample-pause',
        help="Sleep this many seconds before taking the second sample.",
        type=int,
        default=1,
    )

    parser.add_argument(
        '--equal',
        help=(
            "Instead of creating an offset between the sequences on "
            "the source and target databases, assume that none of their "
            "values will change anymore, and set them to equal values."
        ),
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '-n',
        '--dry-run',
        help=(
            "Only print values being set, do not set any sequence "
            "values. Note that the source database sequences will "
            "still be incremented since the current value needs to "
            "be fetched via `nextval`."
        ),
        action='store_true',
        default=False,
    )


async def run(args: argparse.Namespace) -> int:
    """Synchronize sequence values from the source to the target database.

    One of the limitations of logical replication is that changes to sequence
    values are not replicated over. This means that if you were to perform a
    switchover to the target database whilst making use of columns that
    retrieve a default value via a sequence, you would likely retrieve
    conflicts for a while.

    This command aims to alleviate that behaviour and copies the sequence
    values on the source database to the target database, plus a fixed offset.

    Note that running this command will always consume at least one sequence
    item on both databases via the `nextval` function of PostgreSQL.

    Using log level DEBUG here will allow you to see current and target
    sequence values.
    """

    (source_db, target_db) = await db.connect(args)

    sequences = await source_db.fetch(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'S'"
    )

    source_sequence_values = {}
    sequence_offsets = dict(args.fixed_offsets)

    for (relname,) in sequences:
        (nextval,) = await source_db.fetchrow("SELECT nextval($1)", relname)
        log.debug("Last value of sequence %r on first sample is %r.", relname, nextval)
        source_sequence_values[relname] = nextval

    if args.equal:
        for sequence, lastval in source_sequence_values.items():
            if args.dry_run:
                log.debug(
                    "Would set target sequence %r value to %r.", sequence, lastval
                )
            else:
                await target_db.execute(
                    "SELECT setval($1::regclass, $2, false)", sequence, lastval
                )
                log.debug("Set target sequence %r value to %r.", sequence, lastval)

    else:
        await asyncio.sleep(args.sample_pause)

        for sequence, lastval in source_sequence_values.items():
            if sequence not in sequence_offsets:
                (nextval,) = await source_db.fetchrow("SELECT nextval($1)", sequence)

                offset = nextval - source_sequence_values[sequence]
                log.debug(
                    "Last value of sequence %r on second sample is %r, offset at %r.",
                    sequence,
                    nextval,
                    offset,
                )
                sequence_offsets[sequence] = offset
                source_sequence_values[sequence] = lastval

        sequence_values = {
            sequence: lastval + sequence_offsets[sequence] + args.fixed_offset
            for sequence, lastval in source_sequence_values.items()
        }

        for sequence, lastval in sequence_values.items():
            if args.dry_run:
                log.debug(
                    "Would set target sequence %r value to %r.", sequence, lastval
                )
            else:
                await target_db.execute(
                    "SELECT setval($1::regclass, $2)", sequence, lastval
                )
                log.debug("Set target sequence %r value to %r.", sequence, lastval)

    if not sequences:
        log.warning("No sequences found.")

    return 0
