# aul Examples

This directory contains example clients and tools for working with aul.

> **Note:** The interactive SQL client `iaul` has been promoted to an official command and is now located at `cmd/iaul`. See [cmd/iaul/README.md](../cmd/iaul/README.md) for documentation.

## Directory Structure

```
examples/
├── goclient/       # Go TDS client for testing connectivity
└── tdsproxy/       # TDS traffic proxy for debugging
```

## goclient

A simple Go client that connects to aul via TDS protocol and lists tables and stored procedures.

### Usage

```bash
cd examples/goclient
go run goclient.go --host localhost --user sa --password test --database master
```

### Options

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--host` | `MSSQL_HOST` | Server hostname |
| `--port` | `MSSQL_PORT` | Server port (default: 1433) |
| `--user` | `MSSQL_USER` | Username |
| `--password` | `MSSQL_PASSWORD` | Password |
| `--database` | `MSSQL_DATABASE` | Database name |
| `--encrypt` | `MSSQL_ENCRYPT` | Encryption mode: `disable`, `false`, `true`, `strict` |
| `--trust-server-cert` | `MSSQL_TRUST_SERVER_CERT` | Trust server certificate |
| `--config` | - | Path to JSON config file |
| `-v` | - | Verbose output (show connection string) |

### Configuration File

Create a `config.json`:

```json
{
  "host": "localhost",
  "port": 1433,
  "user": "sa",
  "password": "test",
  "database": "master",
  "encrypt": "disable",
  "trust_server_cert": true
}
```

## tdsproxy

A TDS traffic proxy that logs raw TDS packets. Useful for debugging protocol issues.

### Usage

```bash
cd examples/tdsproxy
go run tdsproxy.go --listen :11433 --target localhost:1433
```

Then connect your client to port 11433 instead of 1433.

### Options

| Flag | Description |
|------|-------------|
| `--listen` | Address to listen on (default: `:11433`) |
| `--target` | Target server address (default: `localhost:1433`) |

### Output

The proxy logs TDS packet headers and payloads:

```
=== CLIENT->SERVER ===
TDS Header: type=0x10 (LOGIN7) status=0x01 len=238 spid=0 pktID=1 window=0
Payload (first 64 of 230 bytes):
00000000  04 00 00 74 00 10 00 00  ...

=== SERVER->CLIENT ===
TDS Header: type=0x04 (REPLY) status=0x01 len=156 spid=52 pktID=1 window=0
Payload (first 64 of 148 bytes):
00000000  ad 36 00 01 74 00 00 04  ...
```

### Packet Types

| Type | Name | Description |
|------|------|-------------|
| 0x01 | SQL_BATCH | SQL query batch |
| 0x02 | RPC_REQUEST | Remote procedure call |
| 0x04 | REPLY | Server response |
| 0x10 | LOGIN7 | Login request |
| 0x12 | PRELOGIN | Pre-login negotiation |

## Building Standalone Binaries

```bash
# goclient
cd examples/goclient
go build -o goclient .

# tdsproxy
cd examples/tdsproxy
go build -o tdsproxy .
```

## Dependencies

All examples use the Microsoft Go MSSQL driver:

```bash
go get github.com/microsoft/go-mssqldb
```

## See Also

- [cmd/iaul](../cmd/iaul/README.md) - Interactive SQL client
- [procedures/README.md](../procedures/README.md) - Example stored procedures
- Main project README for aul server options
