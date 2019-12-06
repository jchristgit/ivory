import argparse
import contextlib
import os
import shlex

import asyncpg
import pytest

from ivory.commands.replication import create
from ivory.commands.replication import start
from ivory.commands.replication import status
from ivory.commands.replication import stop
from ivory.commands.replication import drop


@pytest.mark.asyncio
@pytest.mark.parametrize('database', ('ivory_replication_test',))
@pytest.mark.skipif(
    os.getenv('CI') == 'true',
    reason="postgres docker images do not support replication",
)
async def test_full_lifecycle(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
    database: str,
) -> None:

    base_params = ['--source-dbname', database, '--target-dbname', database]
    try:
        await source_db.execute(f"CREATE DATABASE {shlex.quote(database)}")
        await target_db.execute(f"CREATE DATABASE {shlex.quote(database)}")

        args = cli_parser.parse_args(base_params + ['replication', 'create'])
        assert await create.run(args) == 0
        assert await create.run(args) == 0  # idempotence

        args = cli_parser.parse_args(
            base_params + ['replication', 'create', '--skip-checks']
        )
        assert await create.run(args) == 0  # insanity

        args = cli_parser.parse_args(
            base_params + ['replication', 'create', '--drop-replication-user']
        )
        assert await create.run(args) == 0  # replacement

        args = cli_parser.parse_args(base_params + ['replication', 'start'])
        assert await start.run(args) == 0
        assert await start.run(args) == 0  # idempotence

        # verify --fail-on-already-started flag
        args = cli_parser.parse_args(
            base_params + ['replication', 'start', '--fail-on-already-started']
        )
        assert await start.run(args) == 1
        assert await status.run(args) == 0

        args = cli_parser.parse_args(base_params + ['replication', 'stop'])
        assert await stop.run(args) == 0
        assert await stop.run(args) == 0  # idempotence

        # verify --fail-on-already-stopped flag
        args = cli_parser.parse_args(
            base_params + ['replication', 'stop', '--fail-on-already-stopped']
        )
        assert await stop.run(args) == 1

        args = cli_parser.parse_args(base_params + ['replication', 'start'])
        assert await start.run(args) == 0  # restart

        args = cli_parser.parse_args(base_params + ['replication', 'drop'])
        assert await drop.run(args) == 0
        assert await drop.run(args) == 0  # idempotence
    finally:
        # try to clean up...
        with contextlib.suppress(Exception):
            args = cli_parser.parse_args(base_params + ['replication', 'drop'])
            await drop.run(args)

        # kill -9 remaining conns
        await target_db.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            database,
        )
        await source_db.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            database,
        )
        await target_db.execute(f"DROP DATABASE IF EXISTS {shlex.quote(database)}")
        await source_db.execute(f"DROP DATABASE IF EXISTS {shlex.quote(database)}")
