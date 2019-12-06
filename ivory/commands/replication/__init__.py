import argparse

from ivory.commands.replication import create


def configure_parser(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(required=True, dest='subsubcommand')

    parser_create = subparsers.add_parser('create', help=create.__doc__)
    parser_create.description = create.run.__doc__
    parser_create.set_defaults(func=create.run)
    create.add_arguments(parser_create)
