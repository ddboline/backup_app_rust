# backup_app_rust
This is meant to automate the process of backing up files, directories, and postgres tables.

```bash
USAGE:
    backup-app-rust [OPTIONS] <command> --config-file <config-file>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --config-file <config-file>
    -k, --key <key>
    -n, --num-workers <num-workers>

ARGS:
    <command>    list, backup, restore
```

Configuration is done via toml files, an example can be found in `tests/data/test_config.toml`.

There are three types of backups that can be configured:
1. local backups, which have the form:
```toml
[<entry name>]
destination = 'PATH WHERE BACKUP SHOULD GO'
backup_paths = ['LIST OF PATHS TO BE BACKED UP (inputs to tar)']
exclude = ['LIST OF PATHS TO EXCLUDE (the --exclude option in tar)]
2. Full backup of postgres, i.e:
```toml
[<entry name>]
full_postgres_backup = true
destination = 'PATH WHERE BACKUP SHOULD GO'
```
3. Backup of individual postgres database, i.e.:
```toml
[<entry name>]
database_url = 'postgresql://user:password@hostname:port/database'
destination = 'PATH WHERE BACKUP SHOULD GO'
tables = ['LIST OF TABLES TO BACKUP']
sequences = {sequence_name=['table-name', 'index column']]}
dependencies = {table_name=['list of foreign tables']}
```
