import argparse
import os
from typing import Any

from ivory.commands import check
from ivory.commands import copyschema
from ivory.commands import replication
from ivory.commands import syncsequences


def add_database_options(group: argparse._ArgumentGroup, kind: str) -> None:
    env_key = kind.upper()
    description_key = kind.title()
    group.add_argument(
        f'--{kind}-host',
        help=(
            f"{description_key} database host to connect to. Read "
            f"from ${env_key}_HOST."
        ),
        default=os.getenv(f"{env_key}_HOST"),
    )
    group.add_argument(
        f'--{kind}-port',
        help=(
            f"{description_key} database port to connect to. Read "
            f"from ${env_key}_PORT."
        ),
        default=os.getenv(f'{env_key}_PORT', 5432),
        type=int,
    )
    group.add_argument(
        f'--{kind}-user',
        help=(
            f"{description_key} database user to use for operations. Read "
            f"from ${env_key}_USER."
        ),
        default=os.getenv(f'{env_key}_USER'),
    )
    group.add_argument(
        f'--{kind}-password',
        help=(
            f"Matching password for the {kind} database user. Read "
            f"from ${env_key}_PASSWORD."
        ),
        default=os.getenv(f'{env_key}_PASSWORD'),
    )
    group.add_argument(
        f'--{kind}-dbname',
        help=(
            f"{description_key} database name to connect to. Read "
            f"from ${env_key}_DBNAME."
        ),
        default=os.getenv(f'{env_key}_DBNAME'),
    )


def make_parser(**kwargs: Any) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='ivory', formatter_class=argparse.ArgumentDefaultsHelpFormatter, **kwargs
    )
    parser.add_argument(
        '-l',
        '--log-level',
        default='INFO',
        choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
        help="Level to log at.",
    )

    source_group = parser.add_argument_group(
        title='source database options',
        description=(
            "Options for the source database, which will be the "
            "'publisher' in logical replication terminology. In "
            "classic streaming replication setups, this would "
            "be the master."
        ),
    )
    add_database_options(group=source_group, kind='source')

    target_group = parser.add_argument_group(
        title='target database options',
        description=(
            "Options for the target database, which will be the "
            "'subscriber' in logical replication terminology. In "
            "classic streaming replication setups, this would be "
            "the slave. Ivory can manage multiple targets in "
            "subsequent invocations, however, a different "
            "`--subscription-name` needs to be used."
        ),
    )
    add_database_options(group=target_group, kind='target')

    subparsers = parser.add_subparsers(required=True, dest='subcommand')

    parser_check = subparsers.add_parser('check', help=check.__doc__)
    parser_check.description = check.run.__doc__
    parser_check.set_defaults(func=check.run)
    check.add_arguments(parser_check)

    parser_copyschema = subparsers.add_parser('copyschema', help=copyschema.__doc__)
    parser_copyschema.description = copyschema.run.__doc__
    parser_copyschema.set_defaults(func=copyschema.run)
    copyschema.add_arguments(parser_copyschema)

    parser_replication = subparsers.add_parser(
        'replication', help="Manage logical replication."
    )
    replication.configure_parser(parser_replication)

    parser_syncsequences = subparsers.add_parser(
        'syncsequences', help=syncsequences.__doc__
    )
    parser_syncsequences.description = syncsequences.run.__doc__
    parser_syncsequences.set_defaults(func=syncsequences.run)
    syncsequences.add_arguments(parser_syncsequences)

    return parser
