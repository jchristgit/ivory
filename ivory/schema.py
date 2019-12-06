import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

import asyncpg


log = logging.getLogger(__name__)


def port_from_addr(addr: str) -> str:
    matcher = re.compile(r'(\.s\.PGSQL\.|:)(\d+)')
    match = next(matcher.finditer(addr))
    return match.group(2)


async def dump(db: asyncpg.Connection) -> str:
    log.debug("Retrieving database schema.")

    if db._addr.startswith('/'):
        host = str(Path(db._addr).parent)
        port = port_from_addr(db._addr)
    elif ':' in db._addr:
        host, port = db._addr.split(':')
    else:
        host = db._addr
        port = '5432'

    os.environ['PGPASSWORD'] = db._params.password or ''

    try:
        schema = subprocess.check_output(
            (
                'pg_dump',
                '--host',
                host,
                '--port',
                port,
                '--user',
                db._params.user,
                '--dbname',
                db._params.database,
                '--schema-only',
                '--no-publications',
                '--no-subscriptions',
            ),
            text=True,
        )

        with tempfile.NamedTemporaryFile(
            prefix='ivory-schema', mode='w+', suffix='.sql', delete=False
        ) as f:
            f.write(schema)

        log.debug(
            "Schema SQL statements for %r on port %s copied to %r.", host, port, f.name
        )
        return schema

    finally:
        os.unsetenv('PGPASSWORD')
