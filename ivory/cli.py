import argparse
import os
import os.path

from .commands import check
from .commands import copyschema


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
    source_group.add_argument(
        '--source-dsn',
        help="Source database DSN.",
        default='$SOURCE_DSN',
        type=os.path.expandvars,
    )

    target_group = parser.add_argument_group(title='target database options')
    target_group.add_argument(
        '--target-dsn',
        help="Target database DSN.",
        default='$TARGET_DSN',
        type=os.path.expandvars,
    )

    subparsers = parser.add_subparsers(required=True, dest='subcommand')
    parser_check = subparsers.add_parser('check', help=check.__doc__)
    parser_check.set_defaults(func=check.run)

    parser_copyschema = subparsers.add_parser('copyschema', help=copyschema.__doc__)
    parser_copyschema.set_defaults(func=copyschema.run)

    return parser
