import argparse

import pytest

from ivory.commands import check


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="docker images disallow replication connections"
)
async def test_run_reports_rc_0(cli_parser: argparse.ArgumentParser) -> None:
    args = cli_parser.parse_args(['check'])
    rc = await check.run(args)
    assert rc == 0
