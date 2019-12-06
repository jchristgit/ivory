"""Synchronize database schemas."""

import argparse
import logging
import os
import re
import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import Dict

import asyncpg

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
            "regular database as specified in the target DSN."
        ),
        default='postgres',
    )


def database_from_dsn(dsn: str) -> str:
    _, current_target = dsn.rsplit('/', maxsplit=1)
    return current_target


def maintenance_dsn(regular_dsn: str, maintenance_db: str) -> str:
    current_target = database_from_dsn(regular_dsn)
    return regular_dsn.replace(f'/{current_target}', f'/{maintenance_db}')


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

    maintenance_target = maintenance_dsn(
        regular_dsn=args.target_dsn, maintenance_db=args.maintenance_db
    )
    (source_db, maintenance_db) = await db.connect(
        source_dsn=args.source_dsn, target_dsn=maintenance_target
    )

    schema = await load_schema(source_db=source_db, target_db=maintenance_db)
    target_database = database_from_dsn(args.target_dsn)
    await maintenance_db.execute(
        f"DROP DATABASE IF EXISTS {shlex.quote(target_database)}"
    )

    log.info("Dropped database %r from target.", target_database)

    create_opts = await get_database_create_options(source_db)
    joined_opts = ' '.join(f'{key} = {value}' for key, value in create_opts.items())
    await maintenance_db.execute(
        f"CREATE DATABASE {shlex.quote(target_database)} WITH {joined_opts}"
    )
    log.info("Created database %r on target.", target_database)

    log.debug("Applying schema on target (%d lines in SQL).", schema.count('\n'))
    target_db = await asyncpg.connect(dsn=args.target_dsn)
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
