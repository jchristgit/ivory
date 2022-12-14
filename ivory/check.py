"""Pre-flight checks."""

import argparse
import difflib
import tempfile
from gettext import ngettext
from ipaddress import ip_network
from typing import AsyncGenerator, Optional, NamedTuple

import asyncpg  # type: ignore

from ivory.constants import REPLICATION_USERNAME
from ivory import schema


__all__ = ('find_problems',)


class CheckResult(NamedTuple):
    checker: str
    description: str
    error: Optional[str]


async def find_problems(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> AsyncGenerator[CheckResult, None]:
    # https://www.cybertec-postgresql.com/en/upgrading-postgres-major-versions-using-logical-replication/
    checks = (
        check_has_correct_wal_level,
        check_allows_replication_connections,
        check_replica_identity_set,
        check_schema_sync,
        check_database_options,
    )

    for check in checks:
        assert check.__doc__ is not None

        error = await check(source_db=source_db, target_db=target_db, args=args)
        yield CheckResult(
            checker=check.__name__, description=check.__doc__, error=error
        )


async def check_has_correct_wal_level(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> Optional[str]:
    """The master has the correct WAL level set."""

    (level,) = await source_db.fetchrow(
        "SELECT setting FROM pg_settings WHERE name = 'wal_level'"
    )
    if level == 'logical':
        return None
    return f"master (source database) has `wal_level = {level}`, needs `wal_level = logical`"


async def check_allows_replication_connections(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> Optional[str]:
    """The master allows replication connections from the slave."""

    (target_ip,) = await target_db.fetchrow(
        "SELECT COALESCE(inet_server_addr(), '127.0.0.1')"
    )
    accepted_connections = await source_db.fetch(
        """
        SELECT
            address,
            netmask
        FROM
            pg_hba_file_rules
        WHERE
            database @> $1
            AND address IS NOT NULL
            AND (user_name @> $2 OR user_name @> $3)
        """,
        ['replication'],
        [REPLICATION_USERNAME],
        ['all'],
    )

    if any(
        target_ip in ip_network(address) for (address, netmask) in accepted_connections
    ):
        return None

    return (
        "no pg_hba conf entry allows replication "
        f"connections from {target_ip} via user {REPLICATION_USERNAME!r} "
        "on master (source)"
    )


async def check_replica_identity_set(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> Optional[str]:
    """REPLICA IDENTITY is set for all tables."""

    # From https://www.cybertec-postgresql.com/en/upgrading-postgres-major-versions-using-logical-replication/  # noqa
    # Slightly altered to account for missing columns.
    problematic_tables = await source_db.fetch(
        """
        SELECT
            quote_ident(nspname) || '.' || quote_ident(relname) AS tbl
        FROM
            pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE
            relkind = 'r'
            AND NOT nspname LIKE ANY (ARRAY[E'pg\\_%', 'information_schema'])
            AND NOT EXISTS (SELECT * FROM pg_index WHERE indrelid = c.oid
                    AND indisunique AND indisvalid AND indisready AND indislive AND indisprimary)
            AND c.relreplident != 'f'  -- REPLICA IDENTITY FULL, all columns.
        """
    )

    if problematic_tables:
        names = ', '.join(name for (name,) in problematic_tables)
        return (
            "missing primary key / REPLICA IDENTITY on table"
            f"{ngettext('', 's', len(problematic_tables))} {names}"
        )
    return None


async def check_schema_sync(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> Optional[str]:
    """Source and target database schemas are in sync."""

    source_schema = await schema.dump(
        host=args.source_host,
        port=args.source_port,
        dbname=args.source_dbname,
        user=args.source_user,
        password=args.source_password,
    )
    target_schema = await schema.dump(
        host=args.target_host,
        port=args.target_port,
        dbname=args.target_dbname,
        user=args.target_user,
        password=args.target_password,
    )

    if source_schema != target_schema:
        differ = difflib.HtmlDiff(tabsize=4)
        with tempfile.NamedTemporaryFile(
            prefix='ivory-schema-diff-', suffix='.html', delete=False, mode='w+'
        ) as f:
            content = differ.make_file(
                fromlines=source_schema.splitlines(),
                tolines=target_schema.splitlines(),
                fromdesc="Source schema",
                todesc="Target schema",
                context=True,
            )
            f.write(content)
        return f"relation schemas out of sync, see {f.name!r}"

    return None


async def check_database_options(
    source_db: asyncpg.Connection,
    target_db: asyncpg.Connection,
    args: argparse.Namespace,
) -> Optional[str]:
    """Source and target databases have the same options set."""

    query = """
    SELECT
        datconnlimit AS "connection limit",
        pg_encoding_to_char(encoding) AS "encoding",
        (
            SELECT
                usename
            FROM
                pg_catalog.pg_user
            WHERE
                usesysid = datdba
        ) AS "database owner",
        datcollate AS "collation",
        datctype AS "ctype"
    FROM
        pg_database
    WHERE
        datname = current_database()
    """

    source_options = await source_db.fetchrow(query)
    target_options = await target_db.fetchrow(query)

    for key in source_options.keys():
        source_value = source_options[key]
        target_value = target_options[key]

        if source_value != target_value:
            return f"database {key} is {source_value!r} on source, but {target_value!r} on target"
    return None
