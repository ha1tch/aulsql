// Package storage provides storage backend implementations for aul.
package storage

import (
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/aul/pkg/annotations"
)

// TableMetadata stores metadata and annotations for a table.
type TableMetadata struct {
	Database    string
	Schema      string
	Name        string
	Annotations annotations.AnnotationSet
	Columns     []ColumnMetadata
	CreatedAt   time.Time
	ModifiedAt  time.Time
}

// QualifiedName returns the fully qualified table name.
func (t *TableMetadata) QualifiedName() string {
	var parts []string
	if t.Database != "" {
		parts = append(parts, t.Database)
	}
	if t.Schema != "" {
		parts = append(parts, t.Schema)
	}
	parts = append(parts, t.Name)
	return strings.Join(parts, ".")
}

// IsIsolated returns true if the table has the @aul:isolated annotation.
func (t *TableMetadata) IsIsolated() bool {
	return t.Annotations.GetBool("isolated")
}

// JournalMode returns the configured journal mode, or the default.
func (t *TableMetadata) JournalMode(defaultMode string) string {
	return t.Annotations.GetString("journal-mode", defaultMode)
}

// CacheSize returns the configured cache size, or the default.
func (t *TableMetadata) CacheSize(defaultSize int) int {
	return t.Annotations.GetInt("cache-size", defaultSize)
}

// Synchronous returns the configured synchronous setting, or the default.
func (t *TableMetadata) Synchronous(defaultSync string) string {
	return t.Annotations.GetString("synchronous", defaultSync)
}

// IsReadOnly returns true if the table has the @aul:read-only annotation.
func (t *TableMetadata) IsReadOnly() bool {
	return t.Annotations.GetBool("read-only")
}

// ColumnMetadata stores metadata for a table column.
type ColumnMetadata struct {
	Name       string
	Type       string
	Nullable   bool
	IsPrimary  bool
	IsIdentity bool
	Default    string
	Ordinal    int
}

// MetadataCatalogue stores metadata for all known tables.
type MetadataCatalogue struct {
	mu     sync.RWMutex
	tables map[string]*TableMetadata // keyed by qualified name (db.schema.table)
}

// NewMetadataCatalogue creates a new metadata catalogue.
func NewMetadataCatalogue() *MetadataCatalogue {
	return &MetadataCatalogue{
		tables: make(map[string]*TableMetadata),
	}
}

// tableKey generates the key for a table in the catalogue.
func tableKey(database, schema, table string) string {
	if schema == "" {
		schema = "dbo"
	}
	if database == "" {
		return schema + "." + table
	}
	return database + "." + schema + "." + table
}

// RegisterTable adds or updates table metadata in the catalogue.
func (c *MetadataCatalogue) RegisterTable(meta *TableMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := tableKey(meta.Database, meta.Schema, meta.Name)
	c.tables[key] = meta
}

// UnregisterTable removes table metadata from the catalogue.
func (c *MetadataCatalogue) UnregisterTable(database, schema, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := tableKey(database, schema, table)
	delete(c.tables, key)
}

// GetTable retrieves table metadata by name.
func (c *MetadataCatalogue) GetTable(database, schema, table string) (*TableMetadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := tableKey(database, schema, table)
	meta, ok := c.tables[key]
	return meta, ok
}

// IsIsolated checks if a table is marked as isolated.
func (c *MetadataCatalogue) IsIsolated(database, schema, table string) bool {
	meta, ok := c.GetTable(database, schema, table)
	if !ok {
		return false
	}
	return meta.IsIsolated()
}

// ListTables returns all registered tables.
func (c *MetadataCatalogue) ListTables() []*TableMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]*TableMetadata, 0, len(c.tables))
	for _, meta := range c.tables {
		tables = append(tables, meta)
	}
	return tables
}

// ListIsolatedTables returns all tables marked as isolated.
func (c *MetadataCatalogue) ListIsolatedTables() []*TableMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var isolated []*TableMetadata
	for _, meta := range c.tables {
		if meta.IsIsolated() {
			isolated = append(isolated, meta)
		}
	}
	return isolated
}

// DDLParser extracts table metadata from DDL statements.
type DDLParser struct {
	annParser *annotations.Parser
}

// NewDDLParser creates a new DDL parser.
func NewDDLParser() *DDLParser {
	return &DDLParser{
		annParser: annotations.NewParser(),
	}
}

// ParseCreateTable extracts metadata from a CREATE TABLE statement.
func (p *DDLParser) ParseCreateTable(ddl string, database string) (*TableMetadata, error) {
	// Extract annotations
	stmtAnns := p.annParser.Extract(ddl)

	var tableAnns annotations.AnnotationSet
	if len(stmtAnns) > 0 {
		tableAnns = stmtAnns[0].Annotations
	} else {
		tableAnns = make(annotations.AnnotationSet)
	}

	// Parse table name
	schema, name := parseTableName(ddl)

	meta := &TableMetadata{
		Database:    database,
		Schema:      schema,
		Name:        name,
		Annotations: tableAnns,
		CreatedAt:   time.Now(),
		ModifiedAt:  time.Now(),
	}

	// Parse columns (simplified)
	meta.Columns = parseColumns(ddl)

	return meta, nil
}

// parseTableName extracts schema and table name from CREATE TABLE statement.
func parseTableName(ddl string) (schema, name string) {
	upper := strings.ToUpper(ddl)
	idx := strings.Index(upper, "CREATE TABLE")
	if idx == -1 {
		idx = strings.Index(upper, "CREATE TEMP TABLE")
		if idx == -1 {
			return "dbo", ""
		}
		idx += len("CREATE TEMP TABLE")
	} else {
		idx += len("CREATE TABLE")
	}

	// Skip "IF NOT EXISTS" if present
	rest := strings.TrimSpace(ddl[idx:])
	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	}

	// Extract name (up to whitespace or parenthesis)
	endIdx := strings.IndexAny(rest, " \t\n(")
	if endIdx == -1 {
		endIdx = len(rest)
	}
	fullName := strings.TrimSpace(rest[:endIdx])

	// Remove brackets
	fullName = strings.Trim(fullName, "[]")

	// Split schema.table
	if idx := strings.LastIndex(fullName, "."); idx > 0 {
		schema = strings.Trim(fullName[:idx], "[]")
		name = strings.Trim(fullName[idx+1:], "[]")
	} else {
		schema = "dbo"
		name = fullName
	}

	return schema, name
}

// parseColumns extracts column definitions (simplified implementation).
func parseColumns(ddl string) []ColumnMetadata {
	var columns []ColumnMetadata

	// Find column section (between first ( and matching ))
	startIdx := strings.Index(ddl, "(")
	if startIdx == -1 {
		return columns
	}

	endIdx := strings.LastIndex(ddl, ")")
	if endIdx == -1 || endIdx <= startIdx {
		return columns
	}

	colSection := ddl[startIdx+1 : endIdx]

	// Split by comma (simplified - doesn't handle nested parens properly)
	parts := strings.Split(colSection, ",")
	ordinal := 0

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Skip constraints
		upperPart := strings.ToUpper(part)
		if strings.HasPrefix(upperPart, "PRIMARY KEY") ||
			strings.HasPrefix(upperPart, "FOREIGN KEY") ||
			strings.HasPrefix(upperPart, "UNIQUE") ||
			strings.HasPrefix(upperPart, "CHECK") ||
			strings.HasPrefix(upperPart, "CONSTRAINT") ||
			strings.HasPrefix(upperPart, "INDEX") {
			continue
		}

		col := parseColumnDef(part, ordinal)
		if col.Name != "" {
			columns = append(columns, col)
			ordinal++
		}
	}

	return columns
}

// parseColumnDef parses a single column definition.
func parseColumnDef(def string, ordinal int) ColumnMetadata {
	col := ColumnMetadata{
		Ordinal:  ordinal,
		Nullable: true, // Default
	}

	// Split into tokens
	tokens := strings.Fields(def)
	if len(tokens) < 2 {
		return col
	}

	// First token is name
	col.Name = strings.Trim(tokens[0], "[]")

	// Second token is type
	col.Type = tokens[1]

	// Check for modifiers
	upper := strings.ToUpper(def)
	if strings.Contains(upper, "NOT NULL") {
		col.Nullable = false
	}
	if strings.Contains(upper, "PRIMARY KEY") {
		col.IsPrimary = true
		col.Nullable = false
	}
	if strings.Contains(upper, "IDENTITY") || strings.Contains(upper, "AUTOINCREMENT") {
		col.IsIdentity = true
	}

	// Check for DEFAULT
	if idx := strings.Index(upper, "DEFAULT"); idx > 0 {
		// Extract default value (simplified)
		rest := strings.TrimSpace(def[idx+len("DEFAULT"):])
		// Take up to next keyword or end
		endIdx := len(rest)
		for _, kw := range []string{" NOT ", " NULL", " PRIMARY", " UNIQUE", " CHECK", " REFERENCES"} {
			if i := strings.Index(strings.ToUpper(rest), kw); i > 0 && i < endIdx {
				endIdx = i
			}
		}
		col.Default = strings.TrimSpace(rest[:endIdx])
	}

	return col
}
