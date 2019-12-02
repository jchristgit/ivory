"""Synchronize database schemas."""

import argparse
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

from ivory import db


log = logging.getLogger(__name__)


def port_from_addr(addr: str) -> str:
    matcher = re.compile(r'(\.s\.PGSQL\.|:)(\d+)')
    match = next(matcher.finditer(addr))
    return match.group(2)


async def run(args: argparse.Namespace) -> int:
    (source_db, target_db) = await db.connect(
        source_dsn=args.source_dsn, target_dsn=args.target_dsn
    )

    if target_db._addr.startswith('/'):
        host = str(Path(target_db._addr).parent)
        port = port_from_addr(source_db._addr)
    elif ':' in target_db._addr:
        host, port = target_db._addr.split(':')
    else:
        host = target_db._addr
        port = '5432'

    os.environ['PGPASSWORD'] = source_db._params.password or ''

    log.debug("Retrieving database schema.")

    try:
        schema = subprocess.check_output(
            (
                'pg_dump',
                '--host',
                host,
                '--port',
                port,
                '--user',
                source_db._params.user,
                '--dbname',
                source_db._params.database,
                '--schema-only',
            )
        )

        with tempfile.NamedTemporaryFile(
            prefix='ivory-schema', suffix='.sql', delete=False
        ) as f:
            f.write(schema)

        log.info("Schema SQL copied to %r.", f.name)

    except Exception as err:
        log.exception("Unable to dump database schema:", exc_info=err)
        return 1

    finally:
        os.unsetenv('PGPASSWORD')
