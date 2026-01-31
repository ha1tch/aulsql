# iaul

Interactive aul - a modern SQL client for aul and SQL Server compatible databases.

## Overview

iaul provides a psql-like interactive experience for working with aul and TDS-compatible databases. It features readline support, intelligent tab completion, multiple output formats, and a comprehensive set of commands for database exploration.

## Installation

```bash
go install github.com/ha1tch/aul/cmd/iaul@latest
```

Or build from source:

```bash
cd cmd/iaul
go build .
```

## Quick Start

```bash
# Interactive mode
iaul --host localhost --user sa --password secret --database mydb

# Execute SQL and exit
iaul -e "SELECT * FROM users"

# Run a script
iaul -f migrations.sql

# Export to CSV
iaul -e "SELECT * FROM orders" --format csv --quiet > orders.csv
```

## Command Line Options

| Flag | Description |
|------|-------------|
| `--host` | Server hostname |
| `--port` | Server port (default: 1433) |
| `--user` | Username |
| `--password` | Password |
| `--database` | Database name |
| `--encrypt` | Encryption: disable, false, true, strict |
| `--config` | Path to config.json |
| `-e "SQL"` | Execute SQL and exit |
| `-f file.sql` | Execute file and exit |
| `--format` | Output format: default, ascii, unicode, csv, json |
| `--silent` | Errors only |
| `--quiet` | Results only, no banner/stats |
| `--verbose` | Extra timing info |
| `--color` | Force colour output |
| `--no-color` | Disable colour output |
| `--dark` | Dark terminal background (bright headers) |
| `--light` | Light terminal background (dark headers) |
| `--history` | History file path |

## Interactive Commands

### Basic Commands

| Command | Description |
|---------|-------------|
| `<SQL>` | Execute SQL (end with `;` or `GO`) |
| `help`, `?` | Show help |
| `exit`, `quit`, `\q` | Exit |
| `clear` | Clear screen |
| `history` | Show command history |

### Macros

| Command | Description |
|---------|-------------|
| `macros`, `m` | List available macros |
| `m1`, `m2`, ... | Execute macro by number |

Macros are configurable in `config.json`.

### Display Formats

| Command | Description |
|---------|-------------|
| `format` | Show current format |
| `format default` | Simple tabular |
| `format ascii` | ASCII bordered table |
| `format unicode` | Unicode box-drawing table |
| `format csv` | CSV output |
| `format json` | JSON array output |

### Backslash Commands (psql-style)

**Schema exploration:**

| Command | Description |
|---------|-------------|
| `\d`, `\dt` | List tables |
| `\d TABLE` | Describe table columns |
| `\di` | List indexes |
| `\dv` | List views |
| `\dp`, `\df` | List procedures/functions |
| `\ds` | List schemas |
| `\dn` | List databases |

**Input/Output:**

| Command | Description |
|---------|-------------|
| `\i FILE` | Execute SQL file |
| `\o FILE` | Redirect output to file |
| `\o` | Redirect output to stdout |

**Settings:**

| Command | Description |
|---------|-------------|
| `\timing`, `\t` | Toggle timing display |
| `\pager`, `\p` | Toggle pager |
| `\pager CMD` | Set pager command |
| `\r`, `\reset` | Reload schema |

**Variables:**

| Command | Description |
|---------|-------------|
| `\set` | List all variables |
| `\set VAR VALUE` | Set variable |
| `\unset VAR` | Unset variable |

## Variables

Use `:varname` or `$(varname)` in SQL for substitution:

```
sql> \set customer_id 42
sql> \set region EMEA
sql> SELECT * FROM customers WHERE id = :customer_id AND region = '$(region)'
```

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Up/Down` | Navigate history |
| `Ctrl+R` | Reverse search history |
| `Tab` | Autocomplete |
| `Ctrl+A` | Move to line start |
| `Ctrl+E` | Move to line end |
| `Ctrl+W` | Delete word backward |
| `Ctrl+U` | Clear line |
| `Ctrl+L` | Clear screen |
| `Ctrl+C` | Cancel input |
| `Ctrl+D` | Exit |

## Tab Completion

iaul provides intelligent completion for:

- SQL keywords (`SELECT`, `FROM`, `WHERE`, etc.)
- Table names
- Column names (context-aware)
- Backslash commands
- Macro shortcuts

Column completion is context-aware:
- After `FROM tablename`, columns from that table are offered
- `tablename.<TAB>` completes to `tablename.column`

## Output Formats

**default** - Simple tabular:
```
name     age
-------  ---
Alice    30
Bob      25
```

**ascii** - ASCII borders:
```
+---------+-----+
| name    | age |
+=========+=====+
| Alice   | 30  |
| Bob     | 25  |
+---------+-----+
```

**unicode** - Box drawing:
```
┌─────────┬─────┐
│ name    │ age │
├═════════┼═════┤
│ Alice   │ 30  │
│ Bob     │ 25  │
└─────────┴─────┘
```

**csv** - Comma-separated:
```
name,age
Alice,30
Bob,25
```

**json** - JSON array:
```json
[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25}
]
```

## Configuration File

Create `config.json`:

```json
{
  "host": "localhost",
  "port": 1433,
  "user": "sa",
  "password": "secret",
  "database": "mydb",
  "encrypt": "disable",
  "trust_server_cert": true,
  "query_timeout_s": 30,
  "history_file": "~/.iaul_history",
  "max_history": 500,
  "macros": [
    {"name": "List tables", "sql": "SELECT * FROM sys.tables"},
    {"name": "Active users", "sql": "SELECT * FROM users WHERE active = 1"}
  ]
}
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MSSQL_HOST` | Server hostname |
| `MSSQL_PORT` | Server port |
| `MSSQL_USER` | Username |
| `MSSQL_PASSWORD` | Password |
| `MSSQL_DATABASE` | Database name |
| `MSSQL_ENCRYPT` | Encryption mode |
| `IAUL_HISTORY_FILE` | History file path |
| `NO_COLOR` | Disable colours |
| `FORCE_COLOR` | Force colours |

## Verbosity Levels

| Level | Banner | Results | Stats | Errors |
|-------|--------|---------|-------|--------|
| `--silent` | ✗ | ✗ | ✗ | ✓ |
| `--quiet` | ✗ | ✓ | ✗ | ✓ |
| (default) | ✓ | ✓ | ✓ | ✓ |
| `--verbose` | ✓ | ✓ | ✓+ | ✓ |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (SQL error, connection failure, etc.) |

## Comparison with Other Tools

iaul is comparable to:
- **psql** (PostgreSQL) - Similar feature set
- **mysql** (MySQL) - More features than mysql CLI
- **sqlcmd** (SQL Server) - Significantly better UX
- **usql** (Universal) - Similar, but iaul is TDS-focused

## License

Apache 2.0
