import argparse

from ivory import db


async def run(args: argparse.Namespace) -> int:
    (source_db, dest_db) = await db.connect(args.source_dsn, args.dest_dsn)
