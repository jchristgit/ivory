import subprocess


def test_invoke_via_entry_point():
    # how the hell does coverage parsing work here?
    # this is pure magic
    subprocess.check_call(['python', '-m', 'ivory', 'check'])
