# ivory

Manages PostgreSQL upgrades via logical replication.

## Usage

```sh
usage: ivory [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
             [--source-dsn SOURCE_DSN] [--target-dsn TARGET_DSN]
             {check,copyschema} ...

Manages PostgreSQL upgrades via logical replication.

positional arguments:
  {check,copyschema}
    check               Check whether databases are ready.
    copyschema          Synchronize database schemas.

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Level to log at. (default: INFO)

source database options:
  --source-dsn SOURCE_DSN
                        Source database DSN. (default: $SOURCE_DSN)

target database options:
  --target-dsn TARGET_DSN
                        Target database DSN. (default: $TARGET_DSN)
```
