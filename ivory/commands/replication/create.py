"""Set up logical replication from the source to the target database."""

import argparse
import logging
import os.path
import random
import secrets
import shlex
from datetime import datetime

import asyncpg

from ivory import constants
from ivory import check
from ivory import db
from ivory import helpers


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add command-specific arguments."""

    parser.add_argument(
        '--skip-checks',
        help=(
            "Skip pre-flight check execution. "
            "Only use this switch if you know what you are doing."
        ),
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '--drop-replication-user',
        help="Drop the replication user if it exists before doing anything else.",
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '--publication-name',
        help="The name of the publication created on the source database.",
        default=constants.DEFAULT_PUBLICATION_NAME,
    )
    parser.add_argument(
        '--subscription-name',
        help="The name of the subscription created on the target database.",
        default=constants.DEFAULT_SUBSCRIPTION_NAME,
    )
    parser.add_argument(
        '--replication-password',
        help=(
            "Specific password to use for the replication user. By default, "
            "ivory will generate a password and print it to standard output. "
            "Password altering is not performed if the password was updated."
        ),
        default='$REPLICATION_PASSWORD',
        type=helpers.expanded_value(),
    )


async def run(args: argparse.Namespace) -> int:
    """Create logical replication between the source and target database.

    If an active replication is found, nothing is done.
    """

    (source_db, target_db) = await db.connect(args)

    # not specified = your loss
    if not args.skip_checks:
        async for result in check.find_problems(
            source_db=source_db, target_db=target_db
        ):
            if result.error is not None:
                log.error("%s: %s.", result.checker, result.error)
                return 1

        log.debug("Pre-flight checks successful.")
    else:
        log.warning("Pre-flight checks skipped.")

    replication_password = args.replication_password or secrets.token_hex(
        nbytes=random.randrange(40, 80)
    )

    # if err != nil
    if args.drop_replication_user:
        await source_db.execute(
            f"DROP USER IF EXISTS {shlex.quote(constants.REPLICATION_USERNAME)}"
        )
        log.info("Dropped replication user.")
    rc = await create_replication_user(
        source_db=source_db, password=replication_password
    )
    if rc != 0:
        return rc

    rc = await create_publication(
        source_db=source_db, publication_name=args.publication_name
    )
    if rc != 0:
        return rc

    rc = await create_subscription(
        target_db=target_db,
        publication_name=args.publication_name,
        subscription_name=args.subscription_name,
        password=replication_password,
        source_host=args.source_host,
        source_port=args.source_port,
        source_dbname=args.source_dbname,
    )
    if rc != 0:
        return rc

    return 0


async def create_replication_user(source_db: asyncpg.Connection, password: str) -> int:
    """Create the replication user on the source database."""

    existing_user = await source_db.fetchrow(
        "SELECT * FROM pg_user WHERE usename = $1", constants.REPLICATION_USERNAME
    )

    if existing_user is None:
        sql = f"""
        CREATE USER
            {shlex.quote(constants.REPLICATION_USERNAME)}
        WITH
            REPLICATION
            PASSWORD {helpers.quote(password)};
        """

        try:
            async with source_db.transaction():
                await source_db.execute(sql)
                await source_db.execute(
                    f"""
                    COMMENT ON ROLE {shlex.quote(constants.REPLICATION_USERNAME)}
                        IS 'ivory: replication user (created on {datetime.utcnow().isoformat()})'
                    """
                )
        except asyncpg.exceptions.PostgresError as err:
            log.exception("Unable to create user:", exc_info=err)
            return 1
        else:
            log.info(
                "Created replication user %r with password %r.",
                constants.REPLICATION_USERNAME,
                password,
            )

    else:
        log.debug("Existing user found.")
        if not existing_user['userepl']:
            log.error("Existing user cannot use replication.")
            return 1

    return 0


async def create_publication(
    source_db: asyncpg.Connection, publication_name: str
) -> int:
    """Create a publication from the source to the target database."""

    active_publication = await source_db.fetchrow(
        "SELECT * FROM pg_catalog.pg_publication WHERE pubname = $1", publication_name
    )

    if active_publication is None:
        await source_db.execute(
            f"CREATE PUBLICATION {shlex.quote(publication_name)} FOR ALL TABLES"
        )
        await source_db.execute(
            f"COMMENT ON PUBLICATION {shlex.quote(publication_name)} "
            f"IS 'ivory managed (created on {datetime.utcnow().isoformat()})'"
        )
        log.info("Created publication %r.", publication_name)

    else:
        log.debug("Active publication found.")

        for field in (
            'puballtables',
            'pubinsert',
            'pubupdate',
            'pubdelete',
            'pubtruncate',
        ):
            if not active_publication[field]:
                log.error(
                    "Expected setting %r of active publication to be true, but it is false.",
                    field,
                )
                return 1
    return 0


async def create_subscription(
    target_db: asyncpg.Connection,
    publication_name: str,
    subscription_name: str,
    password: str,
    source_host: str,
    source_port: int,
    source_dbname: str,
) -> int:
    """Create a subscription from the target database to the source database."""

    active_subscription = await target_db.fetchrow(
        "SELECT * FROM pg_catalog.pg_subscription WHERE subname = $1", subscription_name
    )
    conninfo = (
        f"host={shlex.quote(remove_socket(source_host))} "
        f"port={source_port} "
        f"dbname={shlex.quote(source_dbname)} "
        f"application_name=ivory_replicator "
        f"user={shlex.quote(constants.REPLICATION_USERNAME)} "
        f"password={shlex.quote(password)}"
    )

    if active_subscription is None:
        try:
            await target_db.execute(
                f"""
                CREATE SUBSCRIPTION {shlex.quote(subscription_name)}
                    CONNECTION {shlex.quote(conninfo)}
                    PUBLICATION {shlex.quote(publication_name)}
                """
            )

            await target_db.execute(
                f"COMMENT ON SUBSCRIPTION {shlex.quote(subscription_name)} "
                f"IS 'ivory managed (created on {datetime.utcnow().isoformat()})'"
            )
        except asyncpg.exceptions.PostgresError as err:
            log.exception("Unable to create subscription:", exc_info=err)
            return 1
        else:
            log.info("Subscription %r created.", subscription_name)

    else:
        await target_db.execute(
            f"""
            ALTER SUBSCRIPTION {shlex.quote(subscription_name)} CONNECTION {shlex.quote(conninfo)};
            ALTER SUBSCRIPTION {shlex.quote(subscription_name)} SET PUBLICATION {shlex.quote(publication_name)};
            """
        )
        log.info("Existing subscription updated.")
        if not active_subscription['subenabled']:
            log.warning("Active subscription is not enabled.")

    return 0


def remove_socket(host: str) -> str:
    """Remove the socket value, if applicable."""

    if os.path.sep in host and '.s.PGSQL' in host:
        return os.path.dirname(host)
    return host