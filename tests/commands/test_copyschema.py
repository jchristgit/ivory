import argparse
import os
import shlex

import asyncpg  # type: ignore
import pytest  # type: ignore

from ivory.commands import copyschema


@pytest.mark.asyncio
@pytest.mark.parametrize('database', ('copyschema_testdb',))
@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="pg_dump complains about major version mismatch"
)
async def test_run_reports_rc_0(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    cli_parser: argparse.ArgumentParser,
    database: str,
) -> None:
    try:
        await source_db.execute(f"CREATE DATABASE {shlex.quote(database)}")

        # for hitting log.warning path
        await source_db.execute(
            f"GRANT CONNECT ON DATABASE {shlex.quote(database)} TO current_user"
        )
        args = cli_parser.parse_args(
            ["--source-dbname", database, "--target-dbname", database, 'copyschema']
        )
        rc = await copyschema.run(args)
        assert rc == 0
    finally:
        await source_db.execute(f"DROP DATABASE IF EXISTS {shlex.quote(database)}")
        await target_db.execute(f"DROP DATABASE IF EXISTS {shlex.quote(database)}")
