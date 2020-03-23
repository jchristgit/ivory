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
    host: Optional[str],
    port: Optional[int],
    dbname: Optional[str],
    user: Optional[str],
    password: Optional[str],
) -> str:
    log.debug("Retrieving database schema.")

    os.environ['PGPASSWORD'] = password or ''

    try:
        cmdline = [
            'pg_dump',
            '--schema-only',
            '--no-publications',
            '--no-subscriptions',
        ]

        if host:
            cmdline.extend(['--host', host])
        if port:
            cmdline.extend(['--port', str(port)])
        if user:
            cmdline.extend(['--user', user])
        if dbname:
            cmdline.extend(['--dbname', dbname])

        schema = subprocess.check_output(cmdline, text=True,)

        with tempfile.NamedTemporaryFile(
            prefix='ivory-schema-', mode='w+', suffix='.sql', delete=False
        ) as f:
            f.write(schema)

        log.debug(
            "Schema SQL statements for %r on port %s copied to %r.", host, port, f.name
        )
        return schema

    finally:
        os.unsetenv('PGPASSWORD')
