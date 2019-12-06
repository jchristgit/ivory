import os
import subprocess

import pytest


@pytest.mark.skipif(
    os.getenv('CI') == 'true', reason="docker images disallow replication connections"
)
def test_invoke_via_entry_point():
    # how the hell does coverage parsing work here?
    # this is pure magic
    subprocess.check_call(['python', '-m', 'ivory', 'check'])
