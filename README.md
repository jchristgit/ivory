# ivory

Manages PostgreSQL logical replication.

## Description

`ivory` is a tool which manages logical replication of a source database to a
target database. Management of the following is performed:

- Checking for discrepancies between source and target database
- Copying the source database schema to the target database
- Setting up a logical replication connection

## Usage

```sh
usage: ivory [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
             [--source-host SOURCE_HOST] [--source-port SOURCE_PORT]
             [--source-user SOURCE_USER] [--source-password SOURCE_PASSWORD]
             [--source-dbname SOURCE_DBNAME] [--target-host TARGET_HOST]
             [--target-port TARGET_PORT] [--target-user TARGET_USER]
             [--target-password TARGET_PASSWORD]
             [--target-dbname TARGET_DBNAME]
             {check,copyschema,replication} ...

Manages PostgreSQL logical replication.

positional arguments:
  {check,copyschema,replication}
    check               Check whether databases are ready.
    copyschema          Synchronize database schemas.
    replication         Manage logical replication.

optional arguments:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Level to log at. (default: INFO)

source database options:
  --source-host SOURCE_HOST
                        Source database host to connect to. Read from from
                        $SOURCE_HOST. (default: None)
  --source-port SOURCE_PORT
                        Source database port to connect to. Read from from
                        $SOURCE_PORT. (default: None)
  --source-user SOURCE_USER
                        Source database user to use for operations. Read from
                        $SOURCE_USER. (default: None)
  --source-password SOURCE_PASSWORD
                        Matching password for the source database user. Read
                        from $SOURCE_PASSWORD. (default: None)
  --source-dbname SOURCE_DBNAME
                        Source database name to connect to. Read from
                        $SOURCE_DBNAME. (default: None)

target database options:
  --target-host TARGET_HOST
                        Target database host to connect to. Read from from
                        $TARGET_HOST. (default: None)
  --target-port TARGET_PORT
                        Target database port to connect to. Read from from
                        $TARGET_PORT. (default: None)
  --target-user TARGET_USER
                        Target database user to use for operations. Read from
                        $TARGET_USER. (default: None)
  --target-password TARGET_PASSWORD
                        Matching password for the target database user. Read
                        from $TARGET_PASSWORD. (default: None)
  --target-dbname TARGET_DBNAME
                        Target database name to connect to. Read from
                        $TARGET_DBNAME. (default: None)
```

Replication subcommand:

```sh
usage: ivory replication [-h] {create,start,status,stop,drop} ...

positional arguments:
  {create,start,status,stop,drop}
    create              Set up logical replication from the source to the
                        target database.
    start               Start logical replication.
    status              Display replication status.
    stop                Stop logical replication.
    drop                Drop logical replication from the source to the target
                        database.

optional arguments:
  -h, --help            show this help message and exit
```

<!-- vim: set ts=2 sw=2 textwidth=80: -->
