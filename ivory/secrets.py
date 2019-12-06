import contextlib
import logging
import os
import os.path
import random
import secrets
from pathlib import Path


log = logging.getLogger(__name__)


def get_replication_password(source_hostname: str, from_args: str) -> str:
    """Get a replication password for the given hostname.

    Examples:

        >>> get_replication_password('example.com', 'abc')
        'abc'
    """

    if from_args:
        return from_args

    if os.path.sep in source_hostname:
        # UDS, don't try to be smart
        prefix = 'replication'
    else:
        prefix = source_hostname

    secret_dir = Path('secrets')
    secret_dir.mkdir(mode=0o700, exist_ok=True)
    secret_path = secret_dir / f'{prefix}-password.txt'

    with contextlib.suppress(FileNotFoundError):
        return secret_path.read_text()

    password = secrets.token_hex(nbytes=random.randrange(40, 80))
    old_umask = os.umask(0o077)
    with secret_path.open(mode='w+') as f:
        f.write(password)
        log.info(
            "Replication user password for %r written to %r.",
            source_hostname,
            str(secret_path),
        )
    os.umask(old_umask)

    return password
