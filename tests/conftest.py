import argparse
import os

import asyncpg  # type: ignore
import pytest  # type: ignore
import pytest_asyncio  # type: ignore

from ivory import cli


@pytest_asyncio.fixture
async def source_db() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=os.getenv('SOURCE_HOST'),
        port=os.getenv('SOURCE_PORT'),
        user=os.getenv('SOURCE_USER'),
        password=os.getenv('SOURCE_PASSWORD'),
        database=os.getenv('SOURCE_DBNAME'),
    )


@pytest_asyncio.fixture
async def target_db() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=os.getenv('TARGET_HOST'),
        port=os.getenv('TARGET_PORT'),
        user=os.getenv('TARGET_USER'),
        password=os.getenv('TARGET_PASSWORD'),
        database=os.getenv('TARGET_DBNAME'),
    )


@pytest.fixture
def cli_parser() -> argparse.ArgumentParser:
    return cli.make_parser()
