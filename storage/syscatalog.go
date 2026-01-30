// Package storage provides storage backend implementations for aul.
// This file implements SQL Server-compatible system catalog views.

package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/runtime"
)

// SystemCatalog provides SQL Server-compatible system views.
// It intercepts queries to sys.* views and translates them to SQLite.
type SystemCatalog struct {
	mu sync.RWMutex

	// Procedure registry for sys.procedures
	registry *procedure.Registry

	// Schema mappings (schema_id -> name)
	schemas map[int]string
}

// NewSystemCatalog creates a new system catalog.
func NewSystemCatalog(registry *procedure.Registry) *SystemCatalog {
	return &SystemCatalog{
		registry: registry,
		schemas: map[int]string{
			1: "dbo",
			2: "guest",
			3: "INFORMATION_SCHEMA",
			4: "sys",
		},
	}
}

// IsSystemQuery checks if a query targets system catalog views.
func (sc *SystemCatalog) IsSystemQuery(sql string) bool {
	normalized := strings.ToLower(strings.TrimSpace(sql))
	return strings.Contains(normalized, "sys.tables") ||
		strings.Contains(normalized, "sys.procedures") ||
		strings.Contains(normalized, "sys.schemas") ||
		strings.Contains(normalized, "sys.objects") ||
		strings.Contains(normalized, "sys.columns") ||
		strings.Contains(normalized, "sys.types") ||
		strings.Contains(normalized, "sys.databases") ||
		strings.Contains(normalized, "information_schema.")
}

// ExecuteSystemQuery handles queries against system catalog views.
func (sc *SystemCatalog) ExecuteSystemQuery(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	normalized := strings.ToLower(strings.TrimSpace(sql))

	// Route to appropriate handler
	switch {
	case strings.Contains(normalized, "sys.tables"):
		return sc.queryTables(ctx, db, sql)
	case strings.Contains(normalized, "sys.procedures"):
		return sc.queryProcedures(ctx, db, sql)
	case strings.Contains(normalized, "sys.schemas"):
		return sc.querySchemas(ctx, db, sql)
	case strings.Contains(normalized, "sys.objects"):
		return sc.queryObjects(ctx, db, sql)
	case strings.Contains(normalized, "sys.columns"):
		return sc.queryColumns(ctx, db, sql)
	case strings.Contains(normalized, "sys.types"):
		return sc.queryTypes(ctx, db, sql)
	case strings.Contains(normalized, "sys.databases"):
		return sc.queryDatabases(ctx, db, sql)
	default:
		return nil, fmt.Errorf("unsupported system view query: %s", sql)
	}
}

// queryTables returns sys.tables data from SQLite metadata.
func (sc *SystemCatalog) queryTables(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Query SQLite for tables
	sqliteQuery := `
		SELECT 
			name,
			CASE WHEN name LIKE '#%' THEN 'temp' ELSE 'dbo' END as schema_name,
			CASE WHEN name LIKE '#%' THEN 2 ELSE 1 END as schema_id
		FROM sqlite_master 
		WHERE type = 'table' 
		AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`

	results, err := db.Query(ctx, sqliteQuery)
	if err != nil {
		return nil, err
	}

	// Transform to sys.tables format
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "schema_id", Type: "INT", Ordinal: 2},
			{Name: "type", Type: "NVARCHAR", Ordinal: 3},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "create_date", Type: "NVARCHAR", Ordinal: 5},
			{Name: "modify_date", Type: "NVARCHAR", Ordinal: 6},
			{Name: "is_ms_shipped", Type: "INT", Ordinal: 7},
		},
	}

	if len(results) > 0 {
		for i, row := range results[0].Rows {
			tableName := row[0].(string)
			schemaID := 1 // dbo
			if len(row) > 2 && row[2] != nil {
				if sid, ok := row[2].(int64); ok {
					schemaID = int(sid)
				}
			}

			rs.Rows = append(rs.Rows, []interface{}{
				tableName,        // name
				int64(i + 1),     // object_id (synthetic)
				int64(schemaID),  // schema_id
				"U ",             // type (user table)
				"USER_TABLE",     // type_desc
				"2025-01-01",     // create_date (placeholder)
				"2025-01-01",     // modify_date (placeholder)
				int64(0),         // is_ms_shipped
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryProcedures returns sys.procedures data from the procedure registry.
func (sc *SystemCatalog) queryProcedures(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "schema_id", Type: "INT", Ordinal: 2},
			{Name: "type", Type: "NVARCHAR", Ordinal: 3},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "create_date", Type: "NVARCHAR", Ordinal: 5},
			{Name: "modify_date", Type: "NVARCHAR", Ordinal: 6},
			{Name: "is_ms_shipped", Type: "INT", Ordinal: 7},
		},
	}

	if sc.registry == nil {
		return []runtime.ResultSet{rs}, nil
	}

	// Get all procedures from registry
	procs := sc.registry.List()
	for i, proc := range procs {
		schemaID := sc.schemaNameToID(proc.Schema)

		rs.Rows = append(rs.Rows, []interface{}{
			proc.Name,                     // name
			int64(10000 + i),              // object_id (synthetic, offset to avoid collision with tables)
			int64(schemaID),               // schema_id
			"P ",                          // type (stored procedure)
			"SQL_STORED_PROCEDURE",        // type_desc
			proc.LoadedAt.Format("2006-01-02 15:04:05"), // create_date
			proc.LoadedAt.Format("2006-01-02 15:04:05"), // modify_date
			int64(0),                      // is_ms_shipped
		})
	}

	return []runtime.ResultSet{rs}, nil
}

// querySchemas returns sys.schemas data.
func (sc *SystemCatalog) querySchemas(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "schema_id", Type: "INT", Ordinal: 1},
			{Name: "principal_id", Type: "INT", Ordinal: 2},
		},
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	for id, name := range sc.schemas {
		rs.Rows = append(rs.Rows, []interface{}{
			name,        // name
			int64(id),   // schema_id
			int64(1),    // principal_id (dbo)
		})
	}

	return []runtime.ResultSet{rs}, nil
}

// queryObjects returns sys.objects data (combined tables + procedures).
func (sc *SystemCatalog) queryObjects(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Get tables
	tables, err := sc.queryTables(ctx, db, sql)
	if err != nil {
		return nil, err
	}

	// Get procedures
	procs, err := sc.queryProcedures(ctx, db, sql)
	if err != nil {
		return nil, err
	}

	// Combine results
	rs := runtime.ResultSet{
		Columns: tables[0].Columns,
		Rows:    append(tables[0].Rows, procs[0].Rows...),
	}

	return []runtime.ResultSet{rs}, nil
}

// queryColumns returns sys.columns data for tables.
func (sc *SystemCatalog) queryColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Query SQLite for table info
	// We need to iterate through tables and get pragma table_info for each
	tablesQuery := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`
	tablesResult, err := db.Query(ctx, tablesQuery)
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "name", Type: "NVARCHAR", Ordinal: 1},
			{Name: "column_id", Type: "INT", Ordinal: 2},
			{Name: "system_type_id", Type: "INT", Ordinal: 3},
			{Name: "max_length", Type: "SMALLINT", Ordinal: 4},
			{Name: "is_nullable", Type: "INT", Ordinal: 5},
		},
	}

	if len(tablesResult) == 0 {
		return []runtime.ResultSet{rs}, nil
	}

	objectID := int64(1)
	for _, row := range tablesResult[0].Rows {
		tableName := row[0].(string)

		// Get columns for this table
		colQuery := fmt.Sprintf("PRAGMA table_info('%s')", tableName)
		colResult, err := db.Query(ctx, colQuery)
		if err != nil {
			continue
		}

		if len(colResult) > 0 {
			for _, colRow := range colResult[0].Rows {
				// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
				colID := colRow[0].(int64)
				colName := colRow[1].(string)
				colType := ""
				if colRow[2] != nil {
					colType = colRow[2].(string)
				}
				notNull := int64(0)
				if colRow[3] != nil {
					notNull = colRow[3].(int64)
				}

				rs.Rows = append(rs.Rows, []interface{}{
					objectID,                          // object_id
					colName,                           // name
					colID + 1,                         // column_id (1-based)
					int64(mapTypeToSystemTypeID(colType)), // system_type_id
					int64(mapTypeToMaxLength(colType)),    // max_length
					int64(1 - notNull),                // is_nullable (inverted)
				})
			}
		}
		objectID++
	}

	return []runtime.ResultSet{rs}, nil
}

// queryTypes returns sys.types data.
func (sc *SystemCatalog) queryTypes(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "system_type_id", Type: "INT", Ordinal: 1},
			{Name: "user_type_id", Type: "INT", Ordinal: 2},
			{Name: "max_length", Type: "SMALLINT", Ordinal: 3},
			{Name: "is_nullable", Type: "INT", Ordinal: 4},
		},
	}

	// Standard SQL Server types
	types := []struct {
		name      string
		typeID    int
		maxLength int
	}{
		{"int", 56, 4},
		{"bigint", 127, 8},
		{"smallint", 52, 2},
		{"tinyint", 48, 1},
		{"bit", 104, 1},
		{"decimal", 106, 17},
		{"numeric", 108, 17},
		{"float", 62, 8},
		{"real", 59, 4},
		{"money", 60, 8},
		{"smallmoney", 122, 4},
		{"datetime", 61, 8},
		{"datetime2", 42, 8},
		{"date", 40, 3},
		{"time", 41, 5},
		{"char", 175, 1},
		{"varchar", 167, 8000},
		{"nchar", 239, 2},
		{"nvarchar", 231, 8000},
		{"text", 35, 16},
		{"ntext", 99, 16},
		{"binary", 173, 1},
		{"varbinary", 165, 8000},
		{"image", 34, 16},
		{"uniqueidentifier", 36, 16},
		{"xml", 241, -1},
	}

	for _, t := range types {
		rs.Rows = append(rs.Rows, []interface{}{
			t.name,            // name
			int64(t.typeID),   // system_type_id
			int64(t.typeID),   // user_type_id
			int64(t.maxLength),// max_length
			int64(1),          // is_nullable
		})
	}

	return []runtime.ResultSet{rs}, nil
}

// queryDatabases returns sys.databases data.
func (sc *SystemCatalog) queryDatabases(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "database_id", Type: "INT", Ordinal: 1},
			{Name: "create_date", Type: "NVARCHAR", Ordinal: 2},
			{Name: "compatibility_level", Type: "INT", Ordinal: 3},
			{Name: "state", Type: "INT", Ordinal: 4},
			{Name: "state_desc", Type: "NVARCHAR", Ordinal: 5},
		},
	}

	// Return standard databases
	databases := []struct {
		name string
		id   int
	}{
		{"master", 1},
		{"tempdb", 2},
		{"model", 3},
		{"msdb", 4},
	}

	for _, d := range databases {
		rs.Rows = append(rs.Rows, []interface{}{
			d.name,        // name
			int64(d.id),   // database_id
			"2025-01-01",  // create_date
			int64(160),    // compatibility_level (SQL Server 2022)
			int64(0),      // state (ONLINE)
			"ONLINE",      // state_desc
		})
	}

	return []runtime.ResultSet{rs}, nil
}

// schemaNameToID converts a schema name to ID.
func (sc *SystemCatalog) schemaNameToID(name string) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	for id, n := range sc.schemas {
		if strings.EqualFold(n, name) {
			return id
		}
	}

	// Default to dbo
	return 1
}

// RegisterSchema adds a schema to the catalog.
func (sc *SystemCatalog) RegisterSchema(id int, name string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.schemas[id] = name
}

// mapTypeToSystemTypeID maps a SQLite type to SQL Server system_type_id.
func mapTypeToSystemTypeID(sqliteType string) int {
	switch strings.ToUpper(sqliteType) {
	case "INTEGER", "INT":
		return 56 // int
	case "REAL":
		return 62 // float
	case "TEXT":
		return 231 // nvarchar
	case "BLOB":
		return 165 // varbinary
	case "NUMERIC":
		return 108 // numeric
	default:
		return 231 // default to nvarchar
	}
}

// mapTypeToMaxLength maps a type to its max length.
func mapTypeToMaxLength(sqliteType string) int {
	switch strings.ToUpper(sqliteType) {
	case "INTEGER", "INT":
		return 4
	case "REAL":
		return 8
	case "TEXT":
		return -1 // max
	case "BLOB":
		return -1 // max
	default:
		return -1
	}
}
