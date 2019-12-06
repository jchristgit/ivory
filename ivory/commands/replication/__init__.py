import argparse

from ivory.commands.replication import create
from ivory.commands.replication import start
from ivory.commands.replication import stop


def configure_parser(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(required=True, dest='subsubcommand')

    parser_create = subparsers.add_parser('create', help=create.__doc__)
    parser_create.description = create.run.__doc__
    parser_create.set_defaults(func=create.run)
    create.add_arguments(parser_create)

    parser_start = subparsers.add_parser('start', help=start.__doc__)
    parser_start.description = start.run.__doc__
    parser_start.set_defaults(func=start.run)
    start.add_arguments(parser_start)

    parser_stop = subparsers.add_parser('stop', help=stop.__doc__)
    parser_stop.description = stop.run.__doc__
    parser_stop.set_defaults(func=stop.run)
    stop.add_arguments(parser_stop)
