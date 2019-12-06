"""Synchronize database schemas."""

import argparse
import logging
import os
import re
import shlex
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict

import asyncpg  # type: ignore

from ivory import db


log = logging.getLogger(__name__)


def port_from_addr(addr: str) -> str:
    matcher = re.compile(r'(\.s\.PGSQL\.|:)(\d+)')
    match = next(matcher.finditer(addr))
    return match.group(2)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add copyschema command-specific arguments."""

    parser.add_argument(
        '-d',
        '--maintenance-db',
        help=(
            "The database to connect to when dropping the "
            "regular database as specified in the target options."
        ),
        default='postgres',
    )


async def get_database_create_options(source_db: asyncpg.Connection) -> Dict[str, str]:
    dbinfo = await source_db.fetchrow(
        'SELECT * FROM pg_database WHERE datname = current_database()'
    )

    (owner_name,) = await source_db.fetchrow(
        'SELECT usename FROM pg_catalog.pg_user WHERE usesysid = $1', dbinfo['datdba']
    )
    (encoding_name,) = await source_db.fetchrow(
        'SELECT pg_encoding_to_char($1)', dbinfo['encoding']
    )

    if dbinfo['datacl'] is not None:
        log.warning("Unable to copy database ACLs (unsupported): %r.", dbinfo['datacl'])

    collate = dbinfo['datcollate']
    ctype = dbinfo['datctype']

    return {
        'CONNECTION LIMIT': dbinfo['datconnlimit'],
        'ENCODING': f'"{encoding_name}"',
        'LC_COLLATE': f'"{collate}"',
        'LC_CTYPE': f'"{ctype}"',
        'OWNER': f'"{owner_name}"',
    }


async def run(args: argparse.Namespace) -> int:
    """Copy the schema from the source database to the target database.

    For the target database, this is a destructive action. The selected
    database in the target schema will be
    """

    (source_db, maintenance_db) = await db.connect(
        args, target_override=dict(database=args.maintenance_db)
    )

    schema = await load_schema(source_db=source_db, target_db=maintenance_db)
    try:
        await maintenance_db.execute(
            f"DROP DATABASE IF EXISTS {shlex.quote(args.target_dbname)}"
        )

    except asyncpg.exceptions.PostgresError as err:
        log.exception(
            "Unable to drop database %r on target:", args.target_dbname, exc_info=err
        )
        return 1
    else:
        log.info("Dropped database %r from target.", args.target_dbname)

    create_opts = await get_database_create_options(source_db)
    joined_opts = ' '.join(f'{key} = {value}' for key, value in create_opts.items())
    await maintenance_db.execute(
        f"CREATE DATABASE {shlex.quote(args.target_dbname)} WITH {joined_opts}"
    )
    await maintenance_db.execute(
        f"""
        COMMENT ON DATABASE {shlex.quote(args.target_dbname)}
        IS 'Created via ivory on {datetime.utcnow().isoformat()}'
        """
    )
    log.info("Created database %r on target.", args.target_dbname)

    log.debug("Applying schema on target (%d lines in SQL).", schema.count('\n'))
    target_db = await asyncpg.connect(
        host=args.target_host,
        port=args.target_port,
        database=args.target_dbname,
        user=args.target_user,
        password=args.target_password,
    )
    await target_db.execute(schema)
    log.info("Applied schema on target.")

    return 0


async def load_schema(
    source_db: asyncpg.Connection, target_db: asyncpg.Connection
) -> str:
    log.debug("Retrieving database schema.")

    if target_db._addr.startswith('/'):
        host = str(Path(target_db._addr).parent)
        port = port_from_addr(source_db._addr)
    elif ':' in target_db._addr:
        host, port = target_db._addr.split(':')
    else:
        host = target_db._addr
        port = '5432'

    os.environ['PGPASSWORD'] = source_db._params.password or ''

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
            ),
            text=True,
        )

        with tempfile.NamedTemporaryFile(
            prefix='ivory-schema', mode='w+', suffix='.sql', delete=False
        ) as f:
            f.write(schema)

        log.info("Schema SQL statements copied to %r.", f.name)
        return schema

    finally:
        os.unsetenv('PGPASSWORD')
