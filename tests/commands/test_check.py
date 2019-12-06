import argparse

import pytest

from ivory.commands import check


@pytest.mark.asyncio
async def test_run_reports_rc_0(cli_parser: argparse.ArgumentParser) -> None:
    args = cli_parser.parse_args(['check'])
    rc = await check.run(args)
    assert rc == 0
