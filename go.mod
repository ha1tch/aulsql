module github.com/ha1tch/aul

go 1.22.2

require (
	github.com/fsnotify/fsnotify v1.9.0
	github.com/jackc/pgx/v5 v5.5.0
	github.com/mattn/go-sqlite3 v1.14.33
	github.com/microsoft/go-mssqldb v1.7.2
	github.com/shopspring/decimal v1.3.1
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

// tsqlparser and tsqlruntime are vendored from:
// - github.com/ha1tch/tsqlparser v0.5.2
// - github.com/ha1tch/tgpiler/tsqlruntime (from tgpiler v0.5.2)
