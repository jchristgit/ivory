import argparse
import os
from unittest.mock import AsyncMock, MagicMock

import asyncpg  # type: ignore
import pytest  # type: ignore

from ivory import check


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="docker images disallow replication connections"
)
async def test_find_problems_finds_nothing_on_empty_database(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
) -> None:
    args = cli_parser.parse_args(["check"])
    async for result in check.find_problems(source_db, target_db, args):
        assert result.error is None


@pytest.mark.asyncio
async def test_complains_about_wal_level_not_logical(
    target_db: asyncpg.Connection, cli_parser: argparse.ArgumentParser
) -> None:
    source_db = MagicMock(spec=asyncpg.Connection)
    source_db.fetchrow = AsyncMock(return_value=('not-logical',))

    args = cli_parser.parse_args(["check"])
    result = await check.check_has_correct_wal_level(
        source_db=source_db, target_db=target_db, args=args
    )
    assert result is not None
    assert result.endswith("needs `wal_level = logical`")


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="docker images disallow replication connections"
)
async def test_complains_about_denied_replication_connection(
    target_db: asyncpg.Connection, cli_parser: argparse.ArgumentParser
) -> None:
    source_db = MagicMock(spec=asyncpg.Connection)
    source_db.fetch = AsyncMock(return_value=())

    args = cli_parser.parse_args(["check"])
    result = await check.check_allows_replication_connections(
        source_db=source_db, target_db=target_db, args=args
    )
    assert result is not None
    assert result.startswith("no pg_hba conf entry allows replication connections")


@pytest.mark.asyncio
async def test_complains_about_missing_replica_identity(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
) -> None:
    tx = source_db.transaction()
    try:
        await tx.start()
        await source_db.execute("CREATE TABLE without_identity (foo INT)")
        args = cli_parser.parse_args(["check"])
        result = await check.check_replica_identity_set(
            source_db=source_db, target_db=target_db, args=args
        )
        assert (
            result
            == "missing primary key / REPLICA IDENTITY on table public.without_identity"
        )
    finally:
        await tx.rollback()


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="pg_dump complains about major version mismatch"
)
async def test_complains_about_out_of_sync_schemas(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
) -> None:
    try:
        await source_db.execute("CREATE TABLE unsynced_table (bar INT PRIMARY KEY)")
        args = cli_parser.parse_args(["check"])
        result = await check.check_schema_sync(
            source_db=source_db, target_db=target_db, args=args
        )
        assert result is not None
        assert result.startswith("relation schemas out of sync, see ")
    finally:
        await source_db.execute("DROP TABLE unsynced_table")


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', (3,))
async def test_complains_about_mismatched_database_options(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
    limit: int,
) -> None:
    (dbname,) = await source_db.fetchrow("SELECT current_database()")
    (target_limit,) = await target_db.fetchrow(
        "SELECT datconnlimit FROM pg_database WHERE datname = current_database()"
    )

    if limit == target_limit:
        limit += 1

    (old_limit,) = await source_db.fetchrow(
        "SELECT datconnlimit FROM pg_database WHERE datname = $1", dbname
    )

    try:
        await source_db.execute(f"ALTER DATABASE {dbname} CONNECTION LIMIT {limit}")
        args = cli_parser.parse_args(["check"])
        result = await check.check_database_options(
            source_db=source_db, target_db=target_db, args=args
        )
        assert result == (
            f"database connection limit is {limit} "
            f"on source, but {target_limit} on target"
        )
    finally:
        await source_db.execute(f"ALTER DATABASE {dbname} CONNECTION LIMIT {old_limit}")
