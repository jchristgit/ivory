# ivory

Manages PostgreSQL upgrades via logical replication.

## Usage

```sh
usage: ivory [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
             [--source-host SOURCE_HOST] [--source-port SOURCE_PORT]
             [--source-user SOURCE_USER] [--source-password SOURCE_PASSWORD]
             [--source-dbname SOURCE_DBNAME] [--target-host TARGET_HOST]
             [--target-port TARGET_PORT] [--target-user TARGET_USER]
             [--target-password TARGET_PASSWORD]
             [--target-dbname TARGET_DBNAME]
             {check,copyschema,createreplication} ...

Manages PostgreSQL upgrades via logical replication.

positional arguments:
  {check,copyschema,createreplication}
    check               Check whether databases are ready.
    copyschema          Synchronize database schemas.
    createreplication   Set up logical replication from the source to the
                        target database.

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Level to log at. (default: INFO)

source database options:
  --source-host SOURCE_HOST
                        Source database host to connect to. (default:
                        $SOURCE_HOST)
  --source-port SOURCE_PORT
                        Source database port to connect to. (default:
                        $SOURCE_PORT)
  --source-user SOURCE_USER
                        Source database user to use for operations. (default:
                        $SOURCE_USER)
  --source-password SOURCE_PASSWORD
                        Matching password for the source database user.
                        (default: $SOURCE_PASSWORD)
  --source-dbname SOURCE_DBNAME
                        Source database name to connect to. (default:
                        $SOURCE_DBNAME)

target database options:
  --target-host TARGET_HOST
                        Target database host to connect to. (default:
                        $TARGET_HOST)
  --target-port TARGET_PORT
                        Target database port to connect to. (default:
                        $TARGET_PORT)
  --target-user TARGET_USER
                        Target database user to use for operations. (default:
                        $TARGET_USER)
  --target-password TARGET_PASSWORD
                        Matching password for the target database user.
                        (default: $TARGET_PASSWORD)
  --target-dbname TARGET_DBNAME
                        Target database name to connect to. (default:
                        $TARGET_DBNAME)
```
