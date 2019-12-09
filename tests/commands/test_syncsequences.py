import asyncio
import argparse

import asyncpg  # type: ignore
import pytest  # type: ignore

from ivory.commands import syncsequences


@pytest.mark.asyncio
@pytest.mark.parametrize('increment', (3,))
async def test_sequence_synchronization_equal(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
    increment: int,
) -> None:
    try:
        await source_db.execute("CREATE SEQUENCE IF NOT EXISTS testseq")
        await target_db.execute("CREATE SEQUENCE IF NOT EXISTS testseq")

        for _ in range(increment):
            await source_db.execute("SELECT nextval('testseq')")

        (final_value,) = await source_db.fetchrow("SELECT nextval('testseq')")

        args = cli_parser.parse_args(['syncsequences', '--equal'])
        rc = await syncsequences.run(args)
        assert rc == 0

        (target_value,) = await target_db.fetchrow("SELECT nextval('testseq') - 1")
        assert target_value == final_value
    finally:
        await source_db.execute("DROP SEQUENCE IF EXISTS testseq")
        await target_db.execute("DROP SEQUENCE IF EXISTS testseq")


@pytest.mark.asyncio
@pytest.mark.parametrize('increment', (3,))
async def test_sequence_synchronization_with_offsets(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
    increment: int,
) -> None:
    try:
        await source_db.execute("CREATE SEQUENCE IF NOT EXISTS testseq")
        await target_db.execute("CREATE SEQUENCE IF NOT EXISTS testseq")

        (final_value,) = await source_db.fetchrow("SELECT nextval('testseq')")

        args = cli_parser.parse_args(['syncsequences', '--fixed-offset', '0'])

        async def increment_sequence(by: int) -> None:
            # just enough to let the command enter `asyncio.sleep(1)`
            await asyncio.sleep(0.5)
            for _ in range(by):
                await source_db.execute("SELECT nextval('testseq')")

        (rc, _) = await asyncio.gather(
            syncsequences.run(args), increment_sequence(by=increment)
        )
        assert rc == 0

        (target_value,) = await target_db.fetchrow("SELECT nextval('testseq') - 1")
        # two nextval selections
        assert target_value == final_value + increment + 2

    finally:
        await source_db.execute("DROP SEQUENCE IF EXISTS testseq")
        await target_db.execute("DROP SEQUENCE IF EXISTS testseq")
