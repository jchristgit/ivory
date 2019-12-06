import argparse
from typing import Literal

from ivory.commands import check
from ivory.commands import copyschema
from ivory.commands import replication
from ivory.helpers import expanded_value


def add_database_options(
    group: argparse._ArgumentGroup, kind: Literal['source', 'target']
) -> None:
    env_key = kind.upper()
    description_key = kind.title()
    group.add_argument(
        f'--{kind}-host',
        help=f"{description_key} database host to connect to.",
        default=f'${env_key}_HOST',
        type=expanded_value(),
    )
    group.add_argument(
        f'--{kind}-port',
        help=f"{description_key} database port to connect to.",
        default=f'${env_key}_PORT',
        type=expanded_value(type_=int, default=5432),
    )
    group.add_argument(
        f'--{kind}-user',
        help=f"{description_key} database user to use for operations.",
        default=f'${env_key}_USER',
        type=expanded_value(),
    )
    group.add_argument(
        f'--{kind}-password',
        help=f"Matching password for the {kind} database user.",
        default=f'${env_key}_PASSWORD',
        type=expanded_value(),
    )
    group.add_argument(
        f'--{kind}-dbname',
        help=f"{description_key} database name to connect to.",
        default=f'${env_key}_DBNAME',
        type=expanded_value(),
    )


def make_parser(**kwargs) -> argparse.ArgumentParser:
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

    source_group = parser.add_argument_group(title='source database options')
    add_database_options(group=source_group, kind='source')

    target_group = parser.add_argument_group(title='target database options')
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

    return parser
