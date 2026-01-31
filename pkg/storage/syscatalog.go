// Package storage provides storage backend implementations for aul.
// This file implements SQL Server-compatible system catalog views.

package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/ha1tch/aul/pkg/procedure"
	"github.com/ha1tch/aul/pkg/runtime"
)

// objectIDForName generates a consistent object_id for a given object name.
// This must match the algorithm used by OBJECT_ID() function in tsqlruntime/functions.go.
func objectIDForName(name string) int64 {
	// Strip database and schema prefixes to get just the table name
	parts := strings.Split(name, ".")
	tableName := parts[len(parts)-1]
	// Remove brackets if present
	tableName = strings.Trim(tableName, "[]")
	
	hash := int64(0)
	for _, c := range tableName {
		hash = hash*31 + int64(c)
	}
	return hash & 0x7FFFFFFF
}

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
		strings.Contains(normalized, "sys.all_objects") ||
		strings.Contains(normalized, "sys.columns") ||
		strings.Contains(normalized, "sys.all_columns") ||
		strings.Contains(normalized, "sys.types") ||
		strings.Contains(normalized, "sys.databases") ||
		strings.Contains(normalized, "sys.indexes") ||
		strings.Contains(normalized, "sys.index_columns") ||
		strings.Contains(normalized, "sys.key_constraints") ||
		strings.Contains(normalized, "sys.foreign_keys") ||
		strings.Contains(normalized, "sys.foreign_key_columns") ||
		strings.Contains(normalized, "sys.check_constraints") ||
		strings.Contains(normalized, "sys.default_constraints") ||
		strings.Contains(normalized, "sys.computed_columns") ||
		strings.Contains(normalized, "sys.identity_columns") ||
		strings.Contains(normalized, "sys.extended_properties") ||
		strings.Contains(normalized, "sys.sql_modules") ||
		strings.Contains(normalized, "sys.parameters") ||
		strings.Contains(normalized, "sys.triggers") ||
		strings.Contains(normalized, "sys.trigger_events") ||
		strings.Contains(normalized, "sys.views") ||
		strings.Contains(normalized, "sys.partitions") ||
		strings.Contains(normalized, "sys.allocation_units") ||
		strings.Contains(normalized, "sys.master_files") ||
		strings.Contains(normalized, "information_schema.")
}

// ExecuteSystemQuery handles queries against system catalog views.
func (sc *SystemCatalog) ExecuteSystemQuery(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	normalized := strings.ToLower(strings.TrimSpace(sql))

	// Route to appropriate handler - order matters for overlapping names
	switch {
	case strings.Contains(normalized, "sys.all_objects"):
		return sc.queryAllObjects(ctx, db, sql)
	case strings.Contains(normalized, "sys.all_columns"):
		return sc.queryAllColumns(ctx, db, sql)
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
	case strings.Contains(normalized, "sys.index_columns"):
		return sc.queryIndexColumns(ctx, db, sql)
	case strings.Contains(normalized, "sys.indexes"):
		return sc.queryIndexes(ctx, db, sql)
	case strings.Contains(normalized, "sys.key_constraints"):
		return sc.queryKeyConstraints(ctx, db, sql)
	case strings.Contains(normalized, "sys.foreign_key_columns"):
		return sc.queryForeignKeyColumns(ctx, db, sql)
	case strings.Contains(normalized, "sys.foreign_keys"):
		return sc.queryForeignKeys(ctx, db, sql)
	case strings.Contains(normalized, "sys.check_constraints"):
		return sc.queryCheckConstraints(ctx, db, sql)
	case strings.Contains(normalized, "sys.default_constraints"):
		return sc.queryDefaultConstraints(ctx, db, sql)
	case strings.Contains(normalized, "sys.computed_columns"):
		return sc.queryComputedColumns(ctx, db, sql)
	case strings.Contains(normalized, "sys.identity_columns"):
		return sc.queryIdentityColumns(ctx, db, sql)
	case strings.Contains(normalized, "sys.extended_properties"):
		return sc.queryExtendedProperties(ctx, db, sql)
	case strings.Contains(normalized, "sys.sql_modules"):
		return sc.querySqlModules(ctx, db, sql)
	case strings.Contains(normalized, "sys.parameters"):
		return sc.queryParameters(ctx, db, sql)
	case strings.Contains(normalized, "sys.trigger_events"):
		return sc.queryTriggerEvents(ctx, db, sql)
	case strings.Contains(normalized, "sys.triggers"):
		return sc.queryTriggers(ctx, db, sql)
	case strings.Contains(normalized, "sys.views"):
		return sc.queryViews(ctx, db, sql)
	case strings.Contains(normalized, "sys.partitions"):
		return sc.queryPartitions(ctx, db, sql)
	case strings.Contains(normalized, "sys.allocation_units"):
		return sc.queryAllocationUnits(ctx, db, sql)
	case strings.Contains(normalized, "sys.master_files"):
		return sc.queryMasterFiles(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.columns"):
		return sc.queryInformationSchemaColumns(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.tables"):
		return sc.queryInformationSchemaTables(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.routines"):
		return sc.queryInformationSchemaRoutines(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.parameters"):
		return sc.queryInformationSchemaParameters(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.key_column_usage"):
		return sc.queryInformationSchemaKeyColumnUsage(ctx, db, sql)
	case strings.Contains(normalized, "information_schema.table_constraints"):
		return sc.queryInformationSchemaTableConstraints(ctx, db, sql)
	case strings.Contains(normalized, "information_schema."):
		// Generic fallback for other INFORMATION_SCHEMA views - return empty
		return sc.queryInformationSchemaEmpty(ctx, db, sql)
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
		for _, row := range results[0].Rows {
			tableName := row[0].(string)
			schemaID := 1 // dbo
			if len(row) > 2 && row[2] != nil {
				if sid, ok := row[2].(int64); ok {
					schemaID = int(sid)
				}
			}

			rs.Rows = append(rs.Rows, []interface{}{
				tableName,                  // name
				objectIDForName(tableName), // object_id (hash-based, matches OBJECT_ID())
				int64(schemaID),            // schema_id
				"U ",                       // type (user table)
				"USER_TABLE",               // type_desc
				"2025-01-01",               // create_date (placeholder)
				"2025-01-01",               // modify_date (placeholder)
				int64(0),                   // is_ms_shipped
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

	for _, row := range tablesResult[0].Rows {
		tableName := row[0].(string)
		objectID := objectIDForName(tableName)

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

// queryIndexes returns sys.indexes data.
func (sc *SystemCatalog) queryIndexes(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "name", Type: "NVARCHAR", Ordinal: 1},
			{Name: "index_id", Type: "INT", Ordinal: 2},
			{Name: "type", Type: "TINYINT", Ordinal: 3},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "is_unique", Type: "BIT", Ordinal: 5},
			{Name: "is_primary_key", Type: "BIT", Ordinal: 6},
			{Name: "is_unique_constraint", Type: "BIT", Ordinal: 7},
		},
	}
	// Return empty - no indexes defined
	return []runtime.ResultSet{rs}, nil
}

// queryIndexColumns returns sys.index_columns data.
func (sc *SystemCatalog) queryIndexColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "index_id", Type: "INT", Ordinal: 1},
			{Name: "index_column_id", Type: "INT", Ordinal: 2},
			{Name: "column_id", Type: "INT", Ordinal: 3},
			{Name: "key_ordinal", Type: "TINYINT", Ordinal: 4},
			{Name: "is_descending_key", Type: "BIT", Ordinal: 5},
			{Name: "is_included_column", Type: "BIT", Ordinal: 6},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryKeyConstraints returns sys.key_constraints data.
func (sc *SystemCatalog) queryKeyConstraints(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "parent_object_id", Type: "INT", Ordinal: 2},
			{Name: "schema_id", Type: "INT", Ordinal: 3},
			{Name: "type", Type: "CHAR", Ordinal: 4},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 5},
			{Name: "unique_index_id", Type: "INT", Ordinal: 6},
			{Name: "is_system_named", Type: "BIT", Ordinal: 7},
		},
	}
	// Return empty - no key constraints defined
	return []runtime.ResultSet{rs}, nil
}

// queryForeignKeys returns sys.foreign_keys data.
func (sc *SystemCatalog) queryForeignKeys(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "parent_object_id", Type: "INT", Ordinal: 2},
			{Name: "referenced_object_id", Type: "INT", Ordinal: 3},
			{Name: "schema_id", Type: "INT", Ordinal: 4},
			{Name: "type", Type: "CHAR", Ordinal: 5},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 6},
			{Name: "is_disabled", Type: "BIT", Ordinal: 7},
			{Name: "is_not_trusted", Type: "BIT", Ordinal: 8},
			{Name: "delete_referential_action", Type: "TINYINT", Ordinal: 9},
			{Name: "delete_referential_action_desc", Type: "NVARCHAR", Ordinal: 10},
			{Name: "update_referential_action", Type: "TINYINT", Ordinal: 11},
			{Name: "update_referential_action_desc", Type: "NVARCHAR", Ordinal: 12},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryForeignKeyColumns returns sys.foreign_key_columns data.
func (sc *SystemCatalog) queryForeignKeyColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "constraint_object_id", Type: "INT", Ordinal: 0},
			{Name: "constraint_column_id", Type: "INT", Ordinal: 1},
			{Name: "parent_object_id", Type: "INT", Ordinal: 2},
			{Name: "parent_column_id", Type: "INT", Ordinal: 3},
			{Name: "referenced_object_id", Type: "INT", Ordinal: 4},
			{Name: "referenced_column_id", Type: "INT", Ordinal: 5},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryCheckConstraints returns sys.check_constraints data.
func (sc *SystemCatalog) queryCheckConstraints(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "parent_object_id", Type: "INT", Ordinal: 2},
			{Name: "parent_column_id", Type: "INT", Ordinal: 3},
			{Name: "schema_id", Type: "INT", Ordinal: 4},
			{Name: "type", Type: "CHAR", Ordinal: 5},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 6},
			{Name: "definition", Type: "NVARCHAR", Ordinal: 7},
			{Name: "is_disabled", Type: "BIT", Ordinal: 8},
			{Name: "is_not_trusted", Type: "BIT", Ordinal: 9},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryDefaultConstraints returns sys.default_constraints data.
func (sc *SystemCatalog) queryDefaultConstraints(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "parent_object_id", Type: "INT", Ordinal: 2},
			{Name: "parent_column_id", Type: "INT", Ordinal: 3},
			{Name: "schema_id", Type: "INT", Ordinal: 4},
			{Name: "type", Type: "CHAR", Ordinal: 5},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 6},
			{Name: "definition", Type: "NVARCHAR", Ordinal: 7},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryComputedColumns returns sys.computed_columns data.
func (sc *SystemCatalog) queryComputedColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "name", Type: "NVARCHAR", Ordinal: 1},
			{Name: "column_id", Type: "INT", Ordinal: 2},
			{Name: "definition", Type: "NVARCHAR", Ordinal: 3},
			{Name: "is_persisted", Type: "BIT", Ordinal: 4},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryIdentityColumns returns sys.identity_columns data.
func (sc *SystemCatalog) queryIdentityColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "name", Type: "NVARCHAR", Ordinal: 1},
			{Name: "column_id", Type: "INT", Ordinal: 2},
			{Name: "seed_value", Type: "SQL_VARIANT", Ordinal: 3},
			{Name: "increment_value", Type: "SQL_VARIANT", Ordinal: 4},
			{Name: "last_value", Type: "SQL_VARIANT", Ordinal: 5},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryExtendedProperties returns sys.extended_properties data.
func (sc *SystemCatalog) queryExtendedProperties(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "class", Type: "TINYINT", Ordinal: 0},
			{Name: "class_desc", Type: "NVARCHAR", Ordinal: 1},
			{Name: "major_id", Type: "INT", Ordinal: 2},
			{Name: "minor_id", Type: "INT", Ordinal: 3},
			{Name: "name", Type: "NVARCHAR", Ordinal: 4},
			{Name: "value", Type: "SQL_VARIANT", Ordinal: 5},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// querySqlModules returns sys.sql_modules data.
func (sc *SystemCatalog) querySqlModules(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "definition", Type: "NVARCHAR", Ordinal: 1},
			{Name: "uses_ansi_nulls", Type: "BIT", Ordinal: 2},
			{Name: "uses_quoted_identifier", Type: "BIT", Ordinal: 3},
			{Name: "is_schema_bound", Type: "BIT", Ordinal: 4},
		},
	}

	// Return procedure definitions if we have a registry
	if sc.registry != nil {
		procs := sc.registry.List()
		for i, proc := range procs {
			rs.Rows = append(rs.Rows, []interface{}{
				int64(10000 + i), // object_id (matches queryProcedures)
				proc.Source,      // definition
				int64(1),         // uses_ansi_nulls
				int64(1),         // uses_quoted_identifier
				int64(0),         // is_schema_bound
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryParameters returns sys.parameters data.
func (sc *SystemCatalog) queryParameters(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "name", Type: "NVARCHAR", Ordinal: 1},
			{Name: "parameter_id", Type: "INT", Ordinal: 2},
			{Name: "system_type_id", Type: "TINYINT", Ordinal: 3},
			{Name: "max_length", Type: "SMALLINT", Ordinal: 4},
			{Name: "is_output", Type: "BIT", Ordinal: 5},
			{Name: "has_default_value", Type: "BIT", Ordinal: 6},
			{Name: "default_value", Type: "SQL_VARIANT", Ordinal: 7},
		},
	}

	// Return procedure parameters if we have a registry
	if sc.registry != nil {
		procs := sc.registry.List()
		for i, proc := range procs {
			objectID := int64(10000 + i)
			for j, param := range proc.Parameters {
				rs.Rows = append(rs.Rows, []interface{}{
					objectID,                                 // object_id
					"@" + param.Name,                         // name
					int64(j + 1),                             // parameter_id
					int64(mapTypeToSystemTypeID(param.SQLType)), // system_type_id
					int64(mapTypeToMaxLength(param.SQLType)), // max_length
					int64(0),                                 // is_output (TODO: detect OUTPUT params)
					int64(0),                                 // has_default_value
					nil,                                      // default_value
				})
			}
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryTriggers returns sys.triggers data.
func (sc *SystemCatalog) queryTriggers(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "parent_id", Type: "INT", Ordinal: 2},
			{Name: "parent_class", Type: "TINYINT", Ordinal: 3},
			{Name: "parent_class_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "type", Type: "CHAR", Ordinal: 5},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 6},
			{Name: "is_disabled", Type: "BIT", Ordinal: 7},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryViews returns sys.views data.
func (sc *SystemCatalog) queryViews(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "schema_id", Type: "INT", Ordinal: 2},
			{Name: "type", Type: "CHAR", Ordinal: 3},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "is_ms_shipped", Type: "BIT", Ordinal: 5},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryPartitions returns sys.partitions data.
func (sc *SystemCatalog) queryPartitions(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Get table info to generate partition data
	tablesQuery := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`
	tablesResult, err := db.Query(ctx, tablesQuery)
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "partition_id", Type: "BIGINT", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "index_id", Type: "INT", Ordinal: 2},
			{Name: "partition_number", Type: "INT", Ordinal: 3},
			{Name: "hobt_id", Type: "BIGINT", Ordinal: 4},
			{Name: "rows", Type: "BIGINT", Ordinal: 5},
		},
	}

	if len(tablesResult) > 0 {
		for _, row := range tablesResult[0].Rows {
			tableName := row[0].(string)
			objectID := objectIDForName(tableName)
			rs.Rows = append(rs.Rows, []interface{}{
				objectID * 1000, // partition_id
				objectID,        // object_id
				int64(0),        // index_id (heap)
				int64(1),        // partition_number
				objectID * 1000, // hobt_id
				int64(0),        // rows (we don't track this)
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryAllocationUnits returns sys.allocation_units data.
func (sc *SystemCatalog) queryAllocationUnits(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "allocation_unit_id", Type: "BIGINT", Ordinal: 0},
			{Name: "type", Type: "TINYINT", Ordinal: 1},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 2},
			{Name: "container_id", Type: "BIGINT", Ordinal: 3},
			{Name: "data_space_id", Type: "INT", Ordinal: 4},
			{Name: "total_pages", Type: "BIGINT", Ordinal: 5},
			{Name: "used_pages", Type: "BIGINT", Ordinal: 6},
			{Name: "data_pages", Type: "BIGINT", Ordinal: 7},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryAllObjects returns sys.all_objects data (similar to sys.objects but includes system objects).
func (sc *SystemCatalog) queryAllObjects(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Query SQLite for tables
	sqliteQuery := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	results, err := db.Query(ctx, sqliteQuery)
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "name", Type: "NVARCHAR", Ordinal: 0},
			{Name: "object_id", Type: "INT", Ordinal: 1},
			{Name: "principal_id", Type: "INT", Ordinal: 2},
			{Name: "schema_id", Type: "INT", Ordinal: 3},
			{Name: "parent_object_id", Type: "INT", Ordinal: 4},
			{Name: "type", Type: "CHAR", Ordinal: 5},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 6},
			{Name: "create_date", Type: "DATETIME", Ordinal: 7},
			{Name: "modify_date", Type: "DATETIME", Ordinal: 8},
			{Name: "is_ms_shipped", Type: "BIT", Ordinal: 9},
			{Name: "is_published", Type: "BIT", Ordinal: 10},
			{Name: "is_schema_published", Type: "BIT", Ordinal: 11},
		},
	}

	if len(results) > 0 {
		for _, row := range results[0].Rows {
			tableName := row[0].(string)
			rs.Rows = append(rs.Rows, []interface{}{
				tableName,                  // name
				objectIDForName(tableName), // object_id
				int64(1),                   // principal_id
				int64(1),                   // schema_id (dbo)
				int64(0),                   // parent_object_id
				"U ",                       // type (user table)
				"USER_TABLE",               // type_desc
				"2025-01-01 00:00:00",      // create_date
				"2025-01-01 00:00:00",      // modify_date
				int64(0),                   // is_ms_shipped
				int64(0),                   // is_published
				int64(0),                   // is_schema_published
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryAllColumns returns sys.all_columns data (similar to sys.columns but includes system objects).
func (sc *SystemCatalog) queryAllColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
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
			{Name: "system_type_id", Type: "TINYINT", Ordinal: 3},
			{Name: "user_type_id", Type: "INT", Ordinal: 4},
			{Name: "max_length", Type: "SMALLINT", Ordinal: 5},
			{Name: "precision", Type: "TINYINT", Ordinal: 6},
			{Name: "scale", Type: "TINYINT", Ordinal: 7},
			{Name: "collation_name", Type: "NVARCHAR", Ordinal: 8},
			{Name: "is_nullable", Type: "BIT", Ordinal: 9},
			{Name: "is_ansi_padded", Type: "BIT", Ordinal: 10},
			{Name: "is_rowguidcol", Type: "BIT", Ordinal: 11},
			{Name: "is_identity", Type: "BIT", Ordinal: 12},
			{Name: "is_computed", Type: "BIT", Ordinal: 13},
			{Name: "is_filestream", Type: "BIT", Ordinal: 14},
			{Name: "is_replicated", Type: "BIT", Ordinal: 15},
			{Name: "is_non_sql_subscribed", Type: "BIT", Ordinal: 16},
			{Name: "is_merge_published", Type: "BIT", Ordinal: 17},
			{Name: "is_dts_replicated", Type: "BIT", Ordinal: 18},
			{Name: "is_xml_document", Type: "BIT", Ordinal: 19},
			{Name: "xml_collection_id", Type: "INT", Ordinal: 20},
			{Name: "default_object_id", Type: "INT", Ordinal: 21},
			{Name: "rule_object_id", Type: "INT", Ordinal: 22},
			{Name: "is_sparse", Type: "BIT", Ordinal: 23},
			{Name: "is_column_set", Type: "BIT", Ordinal: 24},
		},
	}

	if len(tablesResult) == 0 {
		return []runtime.ResultSet{rs}, nil
	}

	for _, row := range tablesResult[0].Rows {
		tableName := row[0].(string)
		objectID := objectIDForName(tableName)

		colQuery := fmt.Sprintf("PRAGMA table_info('%s')", tableName)
		colResult, err := db.Query(ctx, colQuery)
		if err != nil {
			continue
		}

		if len(colResult) > 0 {
			for _, colRow := range colResult[0].Rows {
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
				typeID := mapTypeToSystemTypeID(colType)

				rs.Rows = append(rs.Rows, []interface{}{
					objectID,                       // object_id
					colName,                        // name
					colID + 1,                      // column_id (1-based)
					int64(typeID),                  // system_type_id
					int64(typeID),                  // user_type_id
					int64(mapTypeToMaxLength(colType)), // max_length
					int64(0),                       // precision
					int64(0),                       // scale
					"SQL_Latin1_General_CP1_CI_AS", // collation_name
					int64(1 - notNull),             // is_nullable
					int64(1),                       // is_ansi_padded
					int64(0),                       // is_rowguidcol
					int64(0),                       // is_identity
					int64(0),                       // is_computed
					int64(0),                       // is_filestream
					int64(0),                       // is_replicated
					int64(0),                       // is_non_sql_subscribed
					int64(0),                       // is_merge_published
					int64(0),                       // is_dts_replicated
					int64(0),                       // is_xml_document
					int64(0),                       // xml_collection_id
					int64(0),                       // default_object_id
					int64(0),                       // rule_object_id
					int64(0),                       // is_sparse
					int64(0),                       // is_column_set
				})
			}
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryMasterFiles returns sys.master_files data.
func (sc *SystemCatalog) queryMasterFiles(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "database_id", Type: "INT", Ordinal: 0},
			{Name: "file_id", Type: "INT", Ordinal: 1},
			{Name: "file_guid", Type: "UNIQUEIDENTIFIER", Ordinal: 2},
			{Name: "type", Type: "TINYINT", Ordinal: 3},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 4},
			{Name: "data_space_id", Type: "INT", Ordinal: 5},
			{Name: "name", Type: "NVARCHAR", Ordinal: 6},
			{Name: "physical_name", Type: "NVARCHAR", Ordinal: 7},
			{Name: "state", Type: "TINYINT", Ordinal: 8},
			{Name: "state_desc", Type: "NVARCHAR", Ordinal: 9},
			{Name: "size", Type: "INT", Ordinal: 10},
			{Name: "max_size", Type: "INT", Ordinal: 11},
			{Name: "growth", Type: "INT", Ordinal: 12},
		},
	}

	// Return data files for the standard databases
	databases := []struct {
		id   int
		name string
	}{
		{1, "master"},
		{2, "tempdb"},
		{3, "model"},
		{4, "msdb"},
	}

	for _, d := range databases {
		// Data file
		rs.Rows = append(rs.Rows, []interface{}{
			int64(d.id),                             // database_id
			int64(1),                                // file_id
			nil,                                     // file_guid
			int64(0),                                // type (ROWS)
			"ROWS",                                  // type_desc
			int64(1),                                // data_space_id
			d.name,                                  // name
			fmt.Sprintf("/var/opt/mssql/data/%s.mdf", d.name), // physical_name
			int64(0),                                // state (ONLINE)
			"ONLINE",                                // state_desc
			int64(1024),                             // size (8KB pages)
			int64(-1),                               // max_size (unlimited)
			int64(1024),                             // growth
		})
		// Log file
		rs.Rows = append(rs.Rows, []interface{}{
			int64(d.id),                             // database_id
			int64(2),                                // file_id
			nil,                                     // file_guid
			int64(1),                                // type (LOG)
			"LOG",                                   // type_desc
			int64(0),                                // data_space_id
			d.name + "_log",                         // name
			fmt.Sprintf("/var/opt/mssql/data/%s_log.ldf", d.name), // physical_name
			int64(0),                                // state (ONLINE)
			"ONLINE",                                // state_desc
			int64(256),                              // size
			int64(-1),                               // max_size
			int64(256),                              // growth
		})
	}

	return []runtime.ResultSet{rs}, nil
}

// queryTriggerEvents returns sys.trigger_events data.
func (sc *SystemCatalog) queryTriggerEvents(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "object_id", Type: "INT", Ordinal: 0},
			{Name: "type", Type: "INT", Ordinal: 1},
			{Name: "type_desc", Type: "NVARCHAR", Ordinal: 2},
			{Name: "is_first", Type: "BIT", Ordinal: 3},
			{Name: "is_last", Type: "BIT", Ordinal: 4},
			{Name: "event_group_type", Type: "INT", Ordinal: 5},
			{Name: "event_group_type_desc", Type: "NVARCHAR", Ordinal: 6},
		},
	}
	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaColumns returns INFORMATION_SCHEMA.COLUMNS data.
func (sc *SystemCatalog) queryInformationSchemaColumns(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	tablesQuery := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`
	tablesResult, err := db.Query(ctx, tablesQuery)
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "TABLE_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "TABLE_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "TABLE_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "COLUMN_NAME", Type: "NVARCHAR", Ordinal: 3},
			{Name: "ORDINAL_POSITION", Type: "INT", Ordinal: 4},
			{Name: "COLUMN_DEFAULT", Type: "NVARCHAR", Ordinal: 5},
			{Name: "IS_NULLABLE", Type: "VARCHAR", Ordinal: 6},
			{Name: "DATA_TYPE", Type: "NVARCHAR", Ordinal: 7},
			{Name: "CHARACTER_MAXIMUM_LENGTH", Type: "INT", Ordinal: 8},
			{Name: "CHARACTER_OCTET_LENGTH", Type: "INT", Ordinal: 9},
			{Name: "NUMERIC_PRECISION", Type: "TINYINT", Ordinal: 10},
			{Name: "NUMERIC_PRECISION_RADIX", Type: "SMALLINT", Ordinal: 11},
			{Name: "NUMERIC_SCALE", Type: "INT", Ordinal: 12},
			{Name: "DATETIME_PRECISION", Type: "SMALLINT", Ordinal: 13},
			{Name: "CHARACTER_SET_CATALOG", Type: "NVARCHAR", Ordinal: 14},
			{Name: "CHARACTER_SET_SCHEMA", Type: "NVARCHAR", Ordinal: 15},
			{Name: "CHARACTER_SET_NAME", Type: "NVARCHAR", Ordinal: 16},
			{Name: "COLLATION_CATALOG", Type: "NVARCHAR", Ordinal: 17},
			{Name: "COLLATION_SCHEMA", Type: "NVARCHAR", Ordinal: 18},
			{Name: "COLLATION_NAME", Type: "NVARCHAR", Ordinal: 19},
			{Name: "DOMAIN_CATALOG", Type: "NVARCHAR", Ordinal: 20},
			{Name: "DOMAIN_SCHEMA", Type: "NVARCHAR", Ordinal: 21},
			{Name: "DOMAIN_NAME", Type: "NVARCHAR", Ordinal: 22},
		},
	}

	if len(tablesResult) == 0 || len(tablesResult[0].Rows) == 0 {
		return []runtime.ResultSet{rs}, nil
	}

	for _, row := range tablesResult[0].Rows {
		tableName := row[0].(string)

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
				var defaultVal interface{}
				if colRow[4] != nil {
					defaultVal = colRow[4]
				}

				isNullable := "YES"
				if notNull == 1 {
					isNullable = "NO"
				}

				// Parse type for length/precision
				dataType, maxLen, precision, scale := parseColumnType(colType)

				rs.Rows = append(rs.Rows, []interface{}{
					"master",                          // TABLE_CATALOG
					"dbo",                             // TABLE_SCHEMA
					tableName,                         // TABLE_NAME
					colName,                           // COLUMN_NAME
					colID + 1,                         // ORDINAL_POSITION (1-based)
					defaultVal,                        // COLUMN_DEFAULT
					isNullable,                        // IS_NULLABLE
					dataType,                          // DATA_TYPE
					maxLen,                            // CHARACTER_MAXIMUM_LENGTH
					maxLen,                            // CHARACTER_OCTET_LENGTH
					precision,                         // NUMERIC_PRECISION
					int64(10),                         // NUMERIC_PRECISION_RADIX
					scale,                             // NUMERIC_SCALE
					nil,                               // DATETIME_PRECISION
					nil,                               // CHARACTER_SET_CATALOG
					nil,                               // CHARACTER_SET_SCHEMA
					"iso_1",                           // CHARACTER_SET_NAME
					nil,                               // COLLATION_CATALOG
					nil,                               // COLLATION_SCHEMA
					"SQL_Latin1_General_CP1_CI_AS",    // COLLATION_NAME
					nil,                               // DOMAIN_CATALOG
					nil,                               // DOMAIN_SCHEMA
					nil,                               // DOMAIN_NAME
				})
			}
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// parseColumnType extracts data type, max length, precision and scale from a column type string
func parseColumnType(colType string) (dataType string, maxLen interface{}, precision interface{}, scale interface{}) {
	colType = strings.ToUpper(strings.TrimSpace(colType))
	
	// Handle types with parameters like VARCHAR(100), DECIMAL(10,2)
	if idx := strings.Index(colType, "("); idx > 0 {
		dataType = colType[:idx]
		params := strings.TrimSuffix(colType[idx+1:], ")")
		parts := strings.Split(params, ",")
		
		if len(parts) >= 1 {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
				if isStringType(dataType) {
					maxLen = n
				} else {
					precision = n
				}
			}
		}
		if len(parts) >= 2 {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
				scale = n
			}
		}
	} else {
		dataType = colType
		// Set defaults based on type
		switch dataType {
		case "INT", "INTEGER":
			dataType = "int"
			precision = int64(10)
			scale = int64(0)
		case "BIGINT":
			dataType = "bigint"
			precision = int64(19)
			scale = int64(0)
		case "SMALLINT":
			dataType = "smallint"
			precision = int64(5)
			scale = int64(0)
		case "TINYINT":
			dataType = "tinyint"
			precision = int64(3)
			scale = int64(0)
		case "FLOAT", "REAL":
			dataType = "float"
			precision = int64(53)
		case "TEXT":
			dataType = "nvarchar"
			maxLen = int64(-1) // MAX
		case "BLOB":
			dataType = "varbinary"
			maxLen = int64(-1) // MAX
		default:
			dataType = strings.ToLower(dataType)
		}
	}
	
	return strings.ToLower(dataType), maxLen, precision, scale
}

func isStringType(t string) bool {
	t = strings.ToUpper(t)
	return t == "VARCHAR" || t == "NVARCHAR" || t == "CHAR" || t == "NCHAR" || t == "TEXT" || t == "NTEXT"
}

// queryInformationSchemaTables returns INFORMATION_SCHEMA.TABLES data.
func (sc *SystemCatalog) queryInformationSchemaTables(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	tablesQuery := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	tablesResult, err := db.Query(ctx, tablesQuery)
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "TABLE_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "TABLE_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "TABLE_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "TABLE_TYPE", Type: "VARCHAR", Ordinal: 3},
		},
	}

	if len(tablesResult) > 0 {
		for _, row := range tablesResult[0].Rows {
			tableName := row[0].(string)
			rs.Rows = append(rs.Rows, []interface{}{
				"master",      // TABLE_CATALOG
				"dbo",         // TABLE_SCHEMA
				tableName,     // TABLE_NAME
				"BASE TABLE",  // TABLE_TYPE
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaRoutines returns INFORMATION_SCHEMA.ROUTINES data.
func (sc *SystemCatalog) queryInformationSchemaRoutines(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "SPECIFIC_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "SPECIFIC_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "SPECIFIC_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "ROUTINE_CATALOG", Type: "NVARCHAR", Ordinal: 3},
			{Name: "ROUTINE_SCHEMA", Type: "NVARCHAR", Ordinal: 4},
			{Name: "ROUTINE_NAME", Type: "NVARCHAR", Ordinal: 5},
			{Name: "ROUTINE_TYPE", Type: "NVARCHAR", Ordinal: 6},
			{Name: "DATA_TYPE", Type: "NVARCHAR", Ordinal: 7},
			{Name: "ROUTINE_DEFINITION", Type: "NVARCHAR", Ordinal: 8},
		},
	}

	if sc.registry != nil {
		procs := sc.registry.List()
		for _, proc := range procs {
			rs.Rows = append(rs.Rows, []interface{}{
				"master",           // SPECIFIC_CATALOG
				proc.Schema,        // SPECIFIC_SCHEMA
				proc.Name,          // SPECIFIC_NAME
				"master",           // ROUTINE_CATALOG
				proc.Schema,        // ROUTINE_SCHEMA
				proc.Name,          // ROUTINE_NAME
				"PROCEDURE",        // ROUTINE_TYPE
				nil,                // DATA_TYPE
				proc.Source,        // ROUTINE_DEFINITION
			})
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaParameters returns INFORMATION_SCHEMA.PARAMETERS data.
func (sc *SystemCatalog) queryInformationSchemaParameters(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "SPECIFIC_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "SPECIFIC_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "SPECIFIC_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "ORDINAL_POSITION", Type: "INT", Ordinal: 3},
			{Name: "PARAMETER_MODE", Type: "NVARCHAR", Ordinal: 4},
			{Name: "IS_RESULT", Type: "NVARCHAR", Ordinal: 5},
			{Name: "AS_LOCATOR", Type: "NVARCHAR", Ordinal: 6},
			{Name: "PARAMETER_NAME", Type: "NVARCHAR", Ordinal: 7},
			{Name: "DATA_TYPE", Type: "NVARCHAR", Ordinal: 8},
			{Name: "CHARACTER_MAXIMUM_LENGTH", Type: "INT", Ordinal: 9},
			{Name: "NUMERIC_PRECISION", Type: "TINYINT", Ordinal: 10},
			{Name: "NUMERIC_SCALE", Type: "INT", Ordinal: 11},
		},
	}

	if sc.registry != nil {
		procs := sc.registry.List()
		for _, proc := range procs {
			for i, param := range proc.Parameters {
				rs.Rows = append(rs.Rows, []interface{}{
					"master",                  // SPECIFIC_CATALOG
					proc.Schema,               // SPECIFIC_SCHEMA
					proc.Name,                 // SPECIFIC_NAME
					int64(i + 1),              // ORDINAL_POSITION
					"IN",                      // PARAMETER_MODE
					"NO",                      // IS_RESULT
					"NO",                      // AS_LOCATOR
					"@" + param.Name,          // PARAMETER_NAME
					param.SQLType,             // DATA_TYPE
					nil,                       // CHARACTER_MAXIMUM_LENGTH
					nil,                       // NUMERIC_PRECISION
					nil,                       // NUMERIC_SCALE
				})
			}
		}
	}

	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaKeyColumnUsage returns INFORMATION_SCHEMA.KEY_COLUMN_USAGE data.
func (sc *SystemCatalog) queryInformationSchemaKeyColumnUsage(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "CONSTRAINT_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "CONSTRAINT_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "CONSTRAINT_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "TABLE_CATALOG", Type: "NVARCHAR", Ordinal: 3},
			{Name: "TABLE_SCHEMA", Type: "NVARCHAR", Ordinal: 4},
			{Name: "TABLE_NAME", Type: "NVARCHAR", Ordinal: 5},
			{Name: "COLUMN_NAME", Type: "NVARCHAR", Ordinal: 6},
			{Name: "ORDINAL_POSITION", Type: "INT", Ordinal: 7},
		},
	}
	// Return empty - we don't track constraints in detail yet
	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaTableConstraints returns INFORMATION_SCHEMA.TABLE_CONSTRAINTS data.
func (sc *SystemCatalog) queryInformationSchemaTableConstraints(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{
			{Name: "CONSTRAINT_CATALOG", Type: "NVARCHAR", Ordinal: 0},
			{Name: "CONSTRAINT_SCHEMA", Type: "NVARCHAR", Ordinal: 1},
			{Name: "CONSTRAINT_NAME", Type: "NVARCHAR", Ordinal: 2},
			{Name: "TABLE_CATALOG", Type: "NVARCHAR", Ordinal: 3},
			{Name: "TABLE_SCHEMA", Type: "NVARCHAR", Ordinal: 4},
			{Name: "TABLE_NAME", Type: "NVARCHAR", Ordinal: 5},
			{Name: "CONSTRAINT_TYPE", Type: "VARCHAR", Ordinal: 6},
			{Name: "IS_DEFERRABLE", Type: "VARCHAR", Ordinal: 7},
			{Name: "INITIALLY_DEFERRED", Type: "VARCHAR", Ordinal: 8},
		},
	}
	// Return empty - we don't track constraints in detail yet
	return []runtime.ResultSet{rs}, nil
}

// queryInformationSchemaEmpty returns an empty result set for unimplemented INFORMATION_SCHEMA views.
func (sc *SystemCatalog) queryInformationSchemaEmpty(ctx context.Context, db interface{ Query(context.Context, string, ...interface{}) ([]runtime.ResultSet, error) }, sql string) ([]runtime.ResultSet, error) {
	// Return an empty result set with no columns
	rs := runtime.ResultSet{
		Columns: []runtime.ColumnInfo{},
	}
	return []runtime.ResultSet{rs}, nil
}
