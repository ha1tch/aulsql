// Package storage provides storage backend implementations for aul.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// StorageRouter routes queries to the appropriate database based on table metadata.
// Single-table queries to isolated tables are routed to their dedicated files.
// Multi-table queries involving isolated tables return an error.
type StorageRouter struct {
	mainDB      *sql.DB
	isolatedMgr *IsolatedTableManager
	catalogue   *MetadataCatalogue

	// Default database context
	defaultDatabase string
}

// NewStorageRouter creates a new storage router.
func NewStorageRouter(mainDB *sql.DB, isolatedMgr *IsolatedTableManager, catalogue *MetadataCatalogue) *StorageRouter {
	return &StorageRouter{
		mainDB:          mainDB,
		isolatedMgr:     isolatedMgr,
		catalogue:       catalogue,
		defaultDatabase: "master",
	}
}

// SetDefaultDatabase sets the default database for unqualified table names.
func (r *StorageRouter) SetDefaultDatabase(db string) {
	r.defaultDatabase = db
}

// RouteQuery determines which database(s) a query needs and returns the appropriate connection.
// Returns an error if the query involves multiple databases (isolated + main or multiple isolated).
func (r *StorageRouter) RouteQuery(ctx context.Context, query string) (*sql.DB, error) {
	tables := extractTableNames(query)
	
	if len(tables) == 0 {
		// No tables detected, use main database
		return r.mainDB, nil
	}

	// Categorise tables
	var isolatedTables []tableRef
	var mainTables []tableRef

	for _, tbl := range tables {
		database := tbl.database
		if database == "" {
			database = r.defaultDatabase
		}
		schema := tbl.schema
		if schema == "" {
			schema = "dbo"
		}

		if r.isolatedMgr != nil && r.isolatedMgr.IsIsolated(database, schema, tbl.name) {
			isolatedTables = append(isolatedTables, tbl)
		} else {
			mainTables = append(mainTables, tbl)
		}
	}

	// Routing decision
	if len(isolatedTables) == 0 {
		// All tables are in main database
		return r.mainDB, nil
	}

	if len(mainTables) > 0 {
		// Mix of isolated and main tables - not supported
		return nil, fmt.Errorf("query spans isolated and non-isolated tables: isolated=%v, main=%v",
			tableNames(isolatedTables), tableNames(mainTables))
	}

	if len(isolatedTables) > 1 {
		// Multiple isolated tables - check if they're the same
		first := isolatedTables[0]
		for _, tbl := range isolatedTables[1:] {
			if tbl.database != first.database || tbl.schema != first.schema || tbl.name != first.name {
				return nil, fmt.Errorf("query spans multiple isolated tables: %v",
					tableNames(isolatedTables))
			}
		}
	}

	// Single isolated table
	tbl := isolatedTables[0]
	database := tbl.database
	if database == "" {
		database = r.defaultDatabase
	}
	schema := tbl.schema
	if schema == "" {
		schema = "dbo"
	}

	return r.isolatedMgr.GetConnection(database, schema, tbl.name)
}

// Execute runs a query through the router.
func (r *StorageRouter) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db, err := r.RouteQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

// Query runs a query through the router and returns rows.
func (r *StorageRouter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	db, err := r.RouteQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

// tableRef represents a reference to a table in a query.
type tableRef struct {
	database string
	schema   string
	name     string
}

func (t tableRef) String() string {
	parts := []string{}
	if t.database != "" {
		parts = append(parts, t.database)
	}
	if t.schema != "" {
		parts = append(parts, t.schema)
	}
	parts = append(parts, t.name)
	return strings.Join(parts, ".")
}

func tableNames(tables []tableRef) []string {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.String()
	}
	return names
}

// extractTableNames extracts table names from a SQL query.
// This is a simplified implementation that handles common patterns.
func extractTableNames(query string) []tableRef {
	var tables []tableRef
	seen := make(map[string]bool)

	// Normalise whitespace
	query = strings.Join(strings.Fields(query), " ")

	// Patterns to match:
	// FROM table
	// FROM schema.table
	// FROM database.schema.table
	// JOIN table
	// INTO table
	// UPDATE table
	// DELETE FROM table

	// Regex patterns for table references
	patterns := []*regexp.Regexp{
		// FROM/JOIN/INTO followed by table name
		regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO)\s+(\[?[\w]+\]?(?:\.\[?[\w]+\]?){0,2})`),
		// UPDATE table
		regexp.MustCompile(`(?i)\bUPDATE\s+(\[?[\w]+\]?(?:\.\[?[\w]+\]?){0,2})`),
		// DELETE FROM table (already covered by FROM pattern)
		// INSERT INTO table (already covered by INTO pattern)
	}

	for _, pattern := range patterns {
		matches := pattern.FindAllStringSubmatch(query, -1)
		for _, match := range matches {
			if len(match) >= 2 {
				tableName := match[1]
				
				// Skip keywords that might be caught
				upper := strings.ToUpper(tableName)
				if upper == "SELECT" || upper == "FROM" || upper == "WHERE" ||
					upper == "SET" || upper == "VALUES" || upper == "NULL" {
					continue
				}

				// Parse the table reference
				ref := parseTableRef(tableName)
				key := ref.String()
				if !seen[key] {
					seen[key] = true
					tables = append(tables, ref)
				}
			}
		}
	}

	return tables
}

// parseTableRef parses a table reference like "db.schema.table" or "[schema].[table]".
func parseTableRef(name string) tableRef {
	// Remove brackets
	name = strings.ReplaceAll(name, "[", "")
	name = strings.ReplaceAll(name, "]", "")

	parts := strings.Split(name, ".")

	ref := tableRef{}
	switch len(parts) {
	case 1:
		ref.name = parts[0]
	case 2:
		ref.schema = parts[0]
		ref.name = parts[1]
	case 3:
		ref.database = parts[0]
		ref.schema = parts[1]
		ref.name = parts[2]
	}

	return ref
}

// CanRoute checks if a query can be routed (doesn't span isolated boundaries).
func (r *StorageRouter) CanRoute(query string) error {
	_, err := r.RouteQuery(context.Background(), query)
	return err
}

// GetMainDB returns the main database connection.
func (r *StorageRouter) GetMainDB() *sql.DB {
	return r.mainDB
}

// GetIsolatedManager returns the isolated table manager.
func (r *StorageRouter) GetIsolatedManager() *IsolatedTableManager {
	return r.isolatedMgr
}
