import asyncio
from typing import Tuple

import asyncpg


async def connect(source_dsn: str, dest_dsn: str) -> Tuple[asyncpg.Connection, asyncpg.Connection]:
    source = await asyncpg.connect(dsn=source_dsn)
    dest = await asyncpg.connect(dsn=dest_dsn)
    return (source, dest)
