import argparse
import logging

from ivory import check


log = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> int:
    rc = 0

    async for result in check.find_problems(source_dsn=args.source_dsn, target_dsn=args.target_dsn):
        if result.error is None:
            log.debug(result.description)
        else:
            log.error("%s: %s.", result.checker, result.error)
            rc = 1

    return rc
