import argparse
from typing import Any, Dict, Tuple

import asyncpg  # type: ignore


async def connect(
    args: argparse.ArgumentParser,
    source_override: Dict[str, Any] = {},
    target_override: Dict[str, Any] = {},
) -> Tuple[asyncpg.Connection, asyncpg.Connection]:

    source_options = {
        'host': args.source_host,
        'port': args.source_port,
        'database': args.source_dbname,
        'user': args.source_user,
        'password': args.source_password,
        **source_override,
    }

    target_options = {
        'host': args.target_host,
        'port': args.target_port,
        'database': args.target_dbname,
        'user': args.target_user,
        'password': args.target_password,
        **target_override,
    }

    source = await asyncpg.connect(**source_options)
    target = await asyncpg.connect(**target_options)

    return (source, target)
