"""Synchronize database schemas."""

import argparse
import logging
from datetime import datetime
from typing import Dict

import asyncpg  # type: ignore

from ivory import db
from ivory import schema


log = logging.getLogger(__name__)


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

    sql = await schema.dump(
        host=args.source_host,
        port=args.source_port,
        dbname=args.source_dbname,
        user=args.source_user,
        password=args.source_password,
    )

    try:
        await maintenance_db.execute(f'DROP DATABASE IF EXISTS "{args.target_dbname}"')

    except asyncpg.exceptions.PostgresError as err:
        log.exception(
            "Unable to drop database %r on target:", args.target_dbname, exc_info=err
        )
        return 1
    else:
        log.info("Dropped database %r from target.", args.target_dbname)

    create_opts = await get_database_create_options(source_db)

    if (
        create_opts.get('ENCODING') == 'SQL_ASCII'
        or create_opts.get('LC_COLLATE') == '"C"'
    ):
        create_opts['TEMPLATE'] = 'template0'

    # This can be added later behind a "migrate encoding" flag,
    # although handling further up in the stack (`get_database_create_options`)
    # would definitely be the cleaner approach.
    #
    # del create_opts['LC_COLLATE']
    # del create_opts['TEMPLATE']
    # del create_opts['LC_CTYPE']

    joined_opts = ' '.join(f'{key} = {value}' for key, value in create_opts.items())
    await maintenance_db.execute(
        f'CREATE DATABASE "{args.target_dbname}" WITH {joined_opts}'
    )
    await maintenance_db.execute(
        f"""
        COMMENT ON DATABASE "{args.target_dbname}"
        IS 'Created via ivory on {datetime.utcnow().isoformat()}'
        """
    )
    log.info("Created database %r on target.", args.target_dbname)

    log.debug("Applying schema on target (%d lines in SQL).", sql.count('\n'))
    target_db = await asyncpg.connect(
        host=args.target_host,
        port=args.target_port,
        database=args.target_dbname,
        user=args.target_user,
        password=args.target_password,
    )
    await target_db.execute(sql)
    log.info("Applied schema on target.")

    await source_db.close()
    await target_db.close()

    return 0
