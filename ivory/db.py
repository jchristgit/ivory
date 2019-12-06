import argparse
from typing import Any, Dict, Literal, Tuple

import asyncpg  # type: ignore


async def connect_single(
    args: argparse.Namespace,
    kind: Literal['source', 'target'],
    override: Dict[str, Any] = {},
) -> asyncpg.Connection:
    options = {
        'host': getattr(args, f'{kind}_host'),
        'port': getattr(args, f'{kind}_port'),
        'database': getattr(args, f'{kind}_dbname'),
        'user': getattr(args, f'{kind}_user'),
        'password': getattr(args, f'{kind}_password'),
        **override,
    }
    return await asyncpg.connect(**options)


async def connect(
    args: argparse.Namespace,
    source_override: Dict[str, Any] = {},
    target_override: Dict[str, Any] = {},
) -> Tuple[asyncpg.Connection, asyncpg.Connection]:

    source = await connect_single(args=args, kind='source', override=source_override)
    target = await connect_single(args=args, kind='target', override=target_override)

    return (source, target)
