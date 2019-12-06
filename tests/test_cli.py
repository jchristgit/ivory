import argparse

from ivory import cli


def test_parser_creation():
    assert isinstance(cli.make_parser(), argparse.ArgumentParser)
