import argparse
from typing import Any, Dict, Tuple

import asyncpg  # type: ignore


async def connect_single(
    args: argparse.Namespace, kind: str, override: Dict[str, Any] = {},
) -> asyncpg.Connection:
    options = {
        'host': getattr(args, f'{kind}_host'),
        'port': getattr(args, f'{kind}_port'),
        'database': getattr(args, f'{kind}_dbname'),
        'user': getattr(args, f'{kind}_user'),
        'password': getattr(args, f'{kind}_password'),
        # The default may not work on SSL-enforcing databases.
        # 'ssl': True,
        **override,
    }
    return await asyncpg.connect(**options)


async def connect(
    args: argparse.Namespace,
    source_override: Dict[str, Any] = {},
    target_override: Dict[str, Any] = {},
) -> Tuple[asyncpg.Connection, asyncpg.Connection]:

    source = await connect_single(args=args, kind='source', override=source_override)
    await source.execute("SET application_name = 'ivory'")
    target = await connect_single(args=args, kind='target', override=target_override)
    await target.execute("SET application_name = 'ivory'")

    return (source, target)
