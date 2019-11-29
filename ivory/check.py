"""Pre-flight checks."""

from ipaddress import IPv4Address, IPv4Network
from typing import AsyncGenerator, Optional, NamedTuple

import asyncpg

from . import db


__all__ = ('find_problems',)


class CheckResult(NamedTuple):
    description: str
    error: Optional[str]


async def find_problems(
    source_dsn: str, target_dsn: str
) -> AsyncGenerator[CheckResult, None]:
    (source_db, target_db) = await db.connect(
        source_dsn=source_dsn, target_dsn=target_dsn
    )

    # https://www.cybertec-postgresql.com/en/upgrading-postgres-major-versions-using-logical-replication/
    checks = (
        check_has_correct_wal_level,
        check_allows_replication_connections,
        check_replica_identity_set,
    )

    for check in checks:
        error = await check(source_db=source_db, target_db=target_db)
        yield CheckResult(description=check.__doc__, error=error)


async def check_has_correct_wal_level(
    source_db: asyncpg.Connection, target_db: asyncpg.Connection
) -> Optional[str]:
    """The master has the correct WAL level set."""

    (level,) = await source_db.fetchrow(
        "SELECT setting FROM pg_settings WHERE name = 'wal_level'"
    )
    if level == 'logical':
        return None
    return f"master has `wal_level = {level}`, needs `wal_level = logical`"


async def check_allows_replication_connections(
    source_db: asyncpg.Connection, target_db: asyncpg.Connection
) -> Optional[str]:
    """The master allows replication connections from the slave."""

    (target_ip,) = await target_db.fetchrow(
        "SELECT COALESCE(inet_server_addr(), '127.0.0.1')"
    )
    accepted_connections = await source_db.fetch(
        "SELECT address, netmask FROM pg_hba_file_rules WHERE database @> $1 AND address IS NOT NULL",
        ['replication'],
    )

    if any(
        target_ip in IPv4Network(address, netmask)
        for (address, netmask) in accepted_connections
    ):
        return None
    return f"no pg_hba conf entry allows replication connections from {target_ip}"


async def check_replica_identity_set(
    source_db: asyncpg.Connection, target_db: asyncpg.Connection
) -> Optional[str]:
    """REPLICA IDENTITY is set for all tables."""

    # From https://www.cybertec-postgresql.com/en/upgrading-postgres-major-versions-using-logical-replication/
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
        """
    )

    if problematic_tables:
        names = ', '.join(name for (name,) in problematic_tables)
        return f"missing primary key / REPLICA IDENTITY on tables {names}"
    return None
