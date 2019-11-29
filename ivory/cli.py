import argparse
import os

from .commands import check


def make_parser(**kwargs) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='ivory', formatter_class=argparse.ArgumentDefaultsHelpFormatter, **kwargs
    )

    source_group = parser.add_argument_group(title='source database options')
    source_group.add_argument(
        '--source-dsn', help="Source database DSN.", default='$SOURCE_DSN'
    )

    target_group = parser.add_argument_group(title='target database options')
    target_group.add_argument(
        '--target-dsn', help="Target database DSN.", default='$TARGET_DSN'
    )

    subparsers = parser.add_subparsers(required=True, dest='subcommand')
    parser_check = subparsers.add_parser(
        'check', help='check whether databases are ready'
    )
    parser_check.set_defaults(func=check.run)

    return parser
