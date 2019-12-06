import asyncio
from typing import Tuple

import asyncpg


async def connect(
    source_dsn: str, target_dsn: str
) -> Tuple[asyncpg.Connection, asyncpg.Connection]:
    source = await asyncpg.connect(dsn=source_dsn)
    target = await asyncpg.connect(dsn=target_dsn)
    return (source, target)
