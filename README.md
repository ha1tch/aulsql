# aul

A multi-protocol database server with pluggable network protocols and JIT-compiled stored procedure execution.

## STATUS: VERY BETA, USE AT YOUR OWN RISK

## Many features still in being feverishly developed, may not work right away

## Overview

aul is a database server that:

1. **Accepts connections via multiple protocols** — TDS (SQL Server), PostgreSQL wire protocol, MySQL protocol, HTTP REST, and gRPC
2. **Loads stored procedures from SQL files** — Supporting T-SQL initially, with planned support for other dialects
3. **Executes procedures dynamically** — Using tgpiler's runtime interpreter for flexible execution including dynamic SQL and transactions
4. **JIT-compiles hot procedures** — Automatically transpiles frequently-executed procedures to Go and loads them as plugins for optimised performance

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         aul Server                              │
├─────────────────────────────────────────────────────────────────┤
│  Protocol Listeners                                             │
│  ┌─────┐ ┌──────────┐ ┌───────┐ ┌──────┐ ┌──────┐              │
│  │ TDS │ │ Postgres │ │ MySQL │ │ HTTP │ │ gRPC │              │
│  └──┬──┘ └────┬─────┘ └───┬───┘ └──┬───┘ └──┬───┘              │
│     │         │           │        │        │                   │
│     └─────────┴─────┬─────┴────────┴────────┘                   │
│                     ▼                                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Connection Handler                          │   │
│  │  - Session management                                    │   │
│  │  - Transaction tracking                                  │   │
│  │  - Request routing                                       │   │
│  └─────────────────────────┬───────────────────────────────┘   │
│                            ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Procedure Registry                          │   │
│  │  - Load from .sql files                                  │   │
│  │  - Hot-reload on changes                                 │   │
│  │  - Metadata extraction                                   │   │
│  └─────────────────────────┬───────────────────────────────┘   │
│                            ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Runtime                               │   │
│  │  ┌──────────────────┐    ┌──────────────────┐           │   │
│  │  │   Interpreter    │    │   JIT Manager    │           │   │
│  │  │  (tsqlruntime)   │    │  (tgpiler + Go)  │           │   │
│  │  └────────┬─────────┘    └────────┬─────────┘           │   │
│  │           │                       │                      │   │
│  │           └───────────┬───────────┘                      │   │
│  │                       ▼                                  │   │
│  │  ┌──────────────────────────────────────────────────┐   │   │
│  │  │              Storage Backend                      │   │   │
│  │  │  - In-memory (testing)                            │   │   │
│  │  │  - PostgreSQL, MySQL, SQLite                      │   │   │
│  │  └──────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Installation

```bash
go install github.com/ha1tch/aul/cmd/aul@latest
```

Or build from source:

```bash
git clone https://github.com/ha1tch/aul.git
cd aul
go build -o aul ./cmd/aul
```

## Usage

```bash
# Start with HTTP API (default)
aul --http-port 8080

# Start with SQL Server compatible TDS protocol
aul --tds-port 1433

# Multiple protocols
aul --tds-port 1433 --pg-port 5432 --http-port 8080

# Custom procedure directory with hot-reload
aul -d ./my_procedures -w

# Disable JIT compilation
aul --jit=false
```

## Configuration

### Command Line Options

```
Server Options:
  -c, --config <file>      Configuration file path
  -d, --proc-dir <path>    Directory containing stored procedures
  -w, --watch              Watch for file changes and hot-reload

Protocol Listeners:
  --tds-port <port>        TDS protocol port (SQL Server compatible)
  --pg-port <port>         PostgreSQL wire protocol port
  --mysql-port <port>      MySQL wire protocol port
  --http-port <port>       HTTP REST API port (default: 8080)
  --grpc-port <port>       gRPC port

Runtime Options:
  --dialect <n>         Default SQL dialect: tsql, postgres, mysql
  --jit                    Enable JIT compilation (default: true)
  --jit-threshold <n>      Executions before JIT (default: 100)
  --max-conns <n>          Max concurrent connections (default: 1000)
  --exec-timeout <dur>     Execution timeout (default: 30s)
```

### Configuration File

```yaml
# /etc/aul/config.yaml
server:
  name: my-aul-server
  proc_dir: /var/lib/aul/procedures
  watch_changes: true

listeners:
  - protocol: tds
    port: 1433
  - protocol: http
    port: 8080
    tls:
      enabled: true
      cert_file: /etc/aul/server.crt
      key_file: /etc/aul/server.key

runtime:
  dialect: tsql
  jit_enabled: true
  jit_threshold: 100
  max_concurrency: 500
  exec_timeout: 30s

storage:
  type: postgres
  host: localhost
  port: 5432
  database: aul_data
  username: aul
  password: ${AUL_DB_PASSWORD}
```

## Stored Procedures

Place SQL files in the procedures directory:

```sql
-- procedures/usp_GetCustomer.sql
CREATE PROCEDURE usp_GetCustomer
    @CustomerID INT
AS
BEGIN
    SELECT CustomerID, Name, Email, CreatedAt
    FROM Customers
    WHERE CustomerID = @CustomerID;
END
```

Procedures are automatically loaded at startup and can be hot-reloaded when files change (with `-w` flag).

## JIT Compilation

aul automatically JIT-compiles procedures that are executed frequently:

1. **Execution tracking** — Each procedure call increments an execution counter
2. **Threshold check** — When counter exceeds `--jit-threshold`, compilation is triggered
3. **Transpilation** — tgpiler converts T-SQL to Go code
4. **Compilation** — Go compiler builds a plugin (.so)
5. **Hot-swap** — Plugin is loaded and future calls use compiled code

The interpreter is always available as fallback for:
- Dynamic SQL (`EXEC(@sql)`)
- Procedures below the JIT threshold
- Compilation failures

## Project Structure

```
aul/
├── cmd/aul/           # CLI entry point
├── server/            # Core server orchestration
├── protocol/          # Protocol listener implementations
│   ├── tds/           # TDS (SQL Server) protocol
│   ├── postgres/      # PostgreSQL wire protocol
│   ├── mysql/         # MySQL protocol
│   ├── http/          # HTTP REST API
│   └── grpc/          # gRPC
├── procedure/         # Procedure loading and registry
├── runtime/           # Execution runtime
├── jit/               # JIT compilation manager
├── storage/           # Storage backend implementations
├── pkg/
│   ├── version/       # Version information
│   ├── log/           # Structured logging system
│   └── errors/        # Rich error types with codes
├── docs/              # Documentation
└── tests/             # Integration tests
```

## Logging

aul provides a comprehensive structured logging system with multiple categories:

- **system** — Server lifecycle, configuration, resource management
- **execution** — Procedure calls, query execution, JIT compilation
- **application** — Business logic, procedure loading, protocol handling
- **audit** — Security-relevant events (authentication, authorisation)
- **performance** — Timing, throughput, resource utilisation

Configure logging via CLI:

```bash
# Set log level (debug, info, warn, error)
aul --log-level debug

# Set output format (text, json)
aul --log-format json
```

Example log output (text format):

```
2025-01-28 10:30:15.123 INFO  [system] server starting
2025-01-28 10:30:15.125 DEBUG [application] procedure loaded name=dbo.usp_GetCustomer
2025-01-28 10:30:15.200 INFO  [system] server started state=running procedures=5 listeners=1
```

Example log output (JSON format):

```json
{"time":"2025-01-28T10:30:15.123Z","level":"INFO","category":"system","message":"server started","fields":{"procedures":5,"listeners":1}}
```

## Error Handling

aul uses structured errors with codes for programmatic handling:

| Range | Category | Example Codes |
|-------|----------|---------------|
| 1xxx | Configuration | E1001 (invalid), E1002 (missing) |
| 2xxx | Connection | E2001 (failed), E2003 (timeout), E2005 (handshake) |
| 3xxx | Procedure | E3001 (not found), E3003 (parse error) |
| 4xxx | Execution | E4001 (failed), E4002 (timeout), E4004 (nesting limit) |
| 5xxx | Storage | E5001 (connect), E5002 (query), E5004 (transaction) |
| 6xxx | JIT | E6002 (queue full), E6003 (transpile), E6004 (compile) |
| 9xxx | Internal | E9001 (internal), E9002 (not implemented) |

Errors include context fields for debugging:

```
E3001: procedure not found: usp_Missing
  Operation: ConnectionHandler.handleExec
  Context:
    procedure: usp_Missing
```

## Dependencies

- [jackc/pgx/v5](https://github.com/jackc/pgx) — PostgreSQL wire protocol (pgproto3)
- [shopspring/decimal](https://github.com/shopspring/decimal) — Arbitrary-precision decimals

**Vendored from tgpiler/tsqlparser:**
- tsqlparser v0.5.2 — Full T-SQL parser (AST generation)
- tsqlruntime — T-SQL runtime interpreter

## Implementation Status

| Component | Status |
|-----------|--------|
| HTTP REST API | ✓ Working |
| PostgreSQL wire protocol | ✓ Working (accepts connections, basic handshake) |
| TDS (SQL Server) protocol | ✓ Working (full query execution cycle) |
| MySQL protocol | Not implemented |
| gRPC | Not implemented |
| Procedure loading | ✓ Full AST parsing via tsqlparser |
| T-SQL Parser | ✓ Integrated (tsqlparser v0.5.2) |
| Interpreter (tsqlruntime) | ✓ Integrated (from tgpiler v0.5.2) |
| JIT compilation | ✓ Architecture complete, pending tgpiler integration |
| Storage backends | ✓ SQLite (default), In-memory |
| Structured logging | ✓ Working |
| Error codes | ✓ Working |

## Documentation

See [docs/INDEX.md](docs/INDEX.md) for the full documentation index.

Key documents:
- [001 - Stored Procedure Architecture](docs/001-STORED_PROCEDURE_ARCHITECTURE.md) — Execution paths and dialect handling
- [002 - Storage and Translation](docs/002-PROCEDURE_STORAGE_AND_TRANSLATION.md) — Hierarchy, tenancy, delegation, ACL design
- [003 - Development Plan](docs/003-STORED_PROCEDURE_DEVELOPMENT_PLAN.md) — Phased roadmap to v1.0
- [004 - JIT Architecture](docs/004-JIT_ARCHITECTURE.md) — JIT pipeline design and implementation

## License

GPL v3.0
https://github.com/ha1tch/aulsql/blob/main/LICENSE

## Author

Copyright (c) 2026 haitch
