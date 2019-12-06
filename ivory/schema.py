import logging
import os
import re
import subprocess
import tempfile
from typing import Optional


log = logging.getLogger(__name__)


def port_from_addr(addr: str) -> str:
    matcher = re.compile(r'(\.s\.PGSQL\.|:)(\d+)')
    match = next(matcher.finditer(addr))
    return match.group(2)


async def dump(
    host: str, port: int, dbname: str, user: str, password: Optional[str]
) -> str:
    log.debug("Retrieving database schema.")

    os.environ['PGPASSWORD'] = password or ''

    try:
        schema = subprocess.check_output(
            (
                'pg_dump',
                '--host',
                host,
                '--port',
                str(port),
                '--user',
                user,
                '--dbname',
                dbname,
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
