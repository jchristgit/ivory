"""Set up logical replication from the source to the target database."""

import argparse
import logging
import os
import os.path
import shlex
from datetime import datetime

import asyncpg  # type: ignore

from ivory import constants
from ivory import check
from ivory import db
from ivory import helpers
from ivory import secrets


log = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add command-specific arguments."""

    parser.add_argument(
        '--skip-checks',
        help=(
            "Skip pre-flight check execution. "
            "Only use this switch if you know what you are doing. For instance, "
            "check failure on the REPLICA IDENTITY check being ignored will "
            "result in queries on affected tables resulting in errors."
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
            "Password altering is not performed if the password was updated. "
            "Read from $REPLICATION_PASSWORD."
        ),
        default=os.getenv('REPLICATION_PASSWORD'),
    )


async def run(args: argparse.Namespace) -> int:
    """Create logical replication between the source and target database.

    If an active replication is found, nothing is done.
    """

    (source_db, target_db) = await db.connect(args)

    # not specified = your loss
    if not args.skip_checks:
        async for result in check.find_problems(
            source_db=source_db, target_db=target_db, args=args
        ):
            if result.error is not None:
                log.error("%s: %s.", result.checker, result.error)
                return 1

        log.debug("Pre-flight checks successful.")
    else:
        log.warning("Pre-flight checks skipped.")

    replication_password = secrets.get_replication_password(
        source_hostname=args.source_host, from_args=args.replication_password
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
            PASSWORD {helpers.quote(password)}
            SUPERUSER
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
            log.info("Created replication user %r.", constants.REPLICATION_USERNAME)

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
        f"host={shlex.quote(source_host)} "
        f"port={source_port} "
        f"dbname={shlex.quote(source_dbname)} "
        f"application_name={shlex.quote(constants.REPLICATION_APPLICATION_NAME)} "
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
        if active_subscription['subconninfo'] != conninfo:
            await target_db.execute(
                f"""
            ALTER SUBSCRIPTION
                {shlex.quote(subscription_name)}
                CONNECTION {shlex.quote(conninfo)};
            """
            )
            log.info("Updated connection info of existing subscription.")

        if publication_name not in active_subscription['subpublications']:
            await target_db.execute(
                f"""
                ALTER SUBSCRIPTION
                    {shlex.quote(subscription_name)}
                    SET PUBLICATION {shlex.quote(publication_name)};
                """
            )
            log.info("Updated publication name of existing subscription.")

        if not active_subscription['subenabled']:
            log.warning("Active subscription is not enabled.")

    return 0
