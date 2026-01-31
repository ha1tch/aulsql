package tsqlruntime

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ha1tch/aul/pkg/tsqlparser/ast"
)

// DDLHandler handles DDL statements for temp tables and regular tables
type DDLHandler struct {
	ctx        *ExecutionContext
	normalizer *SQLNormalizer
}

// NewDDLHandler creates a new DDL handler
func NewDDLHandler(ctx *ExecutionContext) *DDLHandler {
	return &DDLHandler{
		ctx:        ctx,
		normalizer: NewSQLNormalizer(ctx.Dialect),
	}
}

// ExecuteCreateTable handles CREATE TABLE for temp tables and regular tables
func (h *DDLHandler) ExecuteCreateTable(stmt *ast.CreateTableStatement) error {
	if stmt == nil || stmt.Name == nil {
		return fmt.Errorf("invalid CREATE TABLE statement")
	}

	tableName := stmt.Name.String()

	// Handle temp tables in memory
	if strings.HasPrefix(tableName, "#") {
		return h.executeCreateTempTable(stmt, tableName)
	}

	// Handle regular tables via database backend
	if h.ctx.DB != nil {
		return h.executeCreateRegularTable(stmt)
	}

	return fmt.Errorf("CREATE TABLE for regular tables requires a database backend")
}

// executeCreateTempTable creates a temp table in memory
func (h *DDLHandler) executeCreateTempTable(stmt *ast.CreateTableStatement, tableName string) error {
	columns := h.parseColumnDefinitions(stmt.Columns)
	_, err := h.ctx.TempTables.CreateTempTable(tableName, columns)
	return err
}

// executeCreateRegularTable creates a regular table via the database backend
func (h *DDLHandler) executeCreateRegularTable(stmt *ast.CreateTableStatement) error {
	// Generate SQLite-compatible DDL from AST
	sql := h.generateSQLiteCreateTable(stmt)

	ctx := context.Background()
	var err error
	if h.ctx.Tx != nil {
		_, err = h.ctx.Tx.ExecContext(ctx, sql)
	} else {
		_, err = h.ctx.DB.ExecContext(ctx, sql)
	}
	return err
}

// generateSQLiteCreateTable generates SQLite-compatible CREATE TABLE from T-SQL AST
func (h *DDLHandler) generateSQLiteCreateTable(stmt *ast.CreateTableStatement) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(stmt.Name.String())
	sb.WriteString(" (\n")

	var columnDefs []string
	var tableConstraints []string

	for _, col := range stmt.Columns {
		colDef := h.generateSQLiteColumn(col)
		columnDefs = append(columnDefs, "  "+colDef)
	}

	// Handle table-level constraints
	for _, constraint := range stmt.Constraints {
		constraintSQL := h.generateSQLiteConstraint(constraint)
		if constraintSQL != "" {
			tableConstraints = append(tableConstraints, "  "+constraintSQL)
		}
	}

	// Combine columns and constraints
	allDefs := append(columnDefs, tableConstraints...)
	sb.WriteString(strings.Join(allDefs, ",\n"))
	sb.WriteString("\n)")

	return sb.String()
}

// generateSQLiteColumn generates a SQLite column definition from T-SQL
func (h *DDLHandler) generateSQLiteColumn(col *ast.ColumnDefinition) string {
	var parts []string

	// Column name
	parts = append(parts, col.Name.Value)

	// Data type - convert to SQLite
	if col.DataType != nil {
		sqliteType := h.convertTypeToSQLite(col.DataType)
		parts = append(parts, sqliteType)
	}

	// Handle IDENTITY - SQLite uses INTEGER PRIMARY KEY for auto-increment
	if col.Identity != nil {
		// For IDENTITY columns, use INTEGER PRIMARY KEY (implies AUTOINCREMENT in SQLite)
		parts[1] = "INTEGER"
		parts = append(parts, "PRIMARY KEY")
	} else {
		// NOT NULL constraint (only if not IDENTITY, which implies NOT NULL)
		if col.Nullable != nil && !*col.Nullable {
			parts = append(parts, "NOT NULL")
		}

		// Check inline constraints for PRIMARY KEY and UNIQUE
		for _, constraint := range col.Constraints {
			if constraint.IsPrimaryKey {
				parts = append(parts, "PRIMARY KEY")
			}
			if constraint.Type == ast.ConstraintUnique {
				parts = append(parts, "UNIQUE")
			}
		}
	}

	// DEFAULT
	if col.Default != nil {
		defaultVal := col.Default.String()
		// Convert GETDATE() to SQLite
		defaultVal = strings.ReplaceAll(defaultVal, "GETDATE()", "CURRENT_TIMESTAMP")
		defaultVal = strings.ReplaceAll(defaultVal, "getdate()", "CURRENT_TIMESTAMP")
		parts = append(parts, "DEFAULT", defaultVal)
	}

	return strings.Join(parts, " ")
}

// convertTypeToSQLite converts a T-SQL data type to SQLite
func (h *DDLHandler) convertTypeToSQLite(dt *ast.DataType) string {
	typeName := strings.ToUpper(dt.Name)

	switch typeName {
	// Integer types
	case "INT", "INTEGER":
		return "INTEGER"
	case "BIGINT", "SMALLINT", "TINYINT":
		return "INTEGER"
	case "BIT":
		return "INTEGER"

	// Text types
	case "VARCHAR", "NVARCHAR", "CHAR", "NCHAR":
		return "TEXT"
	case "TEXT", "NTEXT":
		return "TEXT"

	// Decimal types - use TEXT for exact representation
	case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
		return "TEXT"

	// Float types
	case "FLOAT", "REAL":
		return "REAL"

	// Date/time types
	case "DATETIME", "DATETIME2", "SMALLDATETIME", "DATE", "TIME", "DATETIMEOFFSET":
		return "TEXT"

	// Binary types
	case "BINARY", "VARBINARY", "IMAGE":
		return "BLOB"

	// Other types
	case "UNIQUEIDENTIFIER", "XML", "SQL_VARIANT":
		return "TEXT"

	default:
		return "TEXT"
	}
}

// generateSQLiteConstraint generates a SQLite table constraint
func (h *DDLHandler) generateSQLiteConstraint(constraint *ast.TableConstraint) string {
	if constraint == nil {
		return ""
	}

	var sb strings.Builder

	switch constraint.Type {
	case ast.ConstraintPrimaryKey:
		sb.WriteString("PRIMARY KEY (")
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, col.Name.Value)
		}
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(")")

	case ast.ConstraintUnique:
		sb.WriteString("UNIQUE (")
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, col.Name.Value)
		}
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(")")

	case ast.ConstraintForeignKey:
		sb.WriteString("FOREIGN KEY (")
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, col.Name.Value)
		}
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(") REFERENCES ")
		if constraint.ReferencesTable != nil {
			sb.WriteString(constraint.ReferencesTable.String())
			sb.WriteString(" (")
			var refCols []string
			for _, col := range constraint.ReferencesColumns {
				refCols = append(refCols, col.Value)
			}
			sb.WriteString(strings.Join(refCols, ", "))
			sb.WriteString(")")

			// ON DELETE / ON UPDATE actions
			if constraint.OnDelete != "" {
				sb.WriteString(" ON DELETE ")
				sb.WriteString(constraint.OnDelete)
			}
			if constraint.OnUpdate != "" {
				sb.WriteString(" ON UPDATE ")
				sb.WriteString(constraint.OnUpdate)
			}
		}

	case ast.ConstraintCheck:
		// Skip CHECK constraints for now - SQLite supports them but expression translation is complex
		return ""

	default:
		return ""
	}

	return sb.String()
}

// normalizeTypes converts T-SQL types to SQLite types (legacy method for string-based SQL)
func (h *DDLHandler) normalizeTypes(sql string) string {
	if h.ctx.Dialect != DialectSQLite {
		return sql
	}

	// T-SQL type -> SQLite type
	replacements := []struct {
		old, new string
	}{
		// Integer types
		{"BIGINT", "INTEGER"},
		{"SMALLINT", "INTEGER"},
		{"TINYINT", "INTEGER"},
		{"BIT", "INTEGER"},

		// Decimal/money types - store as TEXT for exact representation
		// Note: SQLite will coerce to REAL for arithmetic, but storage is exact
		{"MONEY", "TEXT"},
		{"SMALLMONEY", "TEXT"},

		// Float types - these are inherently approximate, use REAL
		{"FLOAT", "REAL"},

		// String types - need to handle VARCHAR(n), NVARCHAR(n), etc.
		// SQLite ignores length constraints on TEXT, so this is mainly cosmetic
		{"NVARCHAR(MAX)", "TEXT"},
		{"VARCHAR(MAX)", "TEXT"},
		{"NCHAR", "TEXT"},
		{"NTEXT", "TEXT"},

		// Date/time types
		{"DATETIME2", "TEXT"},
		{"DATETIME", "TEXT"},
		{"SMALLDATETIME", "TEXT"},
		{"DATE", "TEXT"},
		{"TIME", "TEXT"},
		{"DATETIMEOFFSET", "TEXT"},

		// Binary types
		{"VARBINARY(MAX)", "BLOB"},
		{"VARBINARY", "BLOB"},
		{"BINARY", "BLOB"},
		{"IMAGE", "BLOB"},

		// Other types
		{"UNIQUEIDENTIFIER", "TEXT"},
		{"XML", "TEXT"},
		{"SQL_VARIANT", "TEXT"},
	}

	for _, r := range replacements {
		sql = strings.ReplaceAll(sql, r.old, r.new)
		sql = strings.ReplaceAll(sql, strings.ToLower(r.old), r.new)
	}

	// Handle DECIMAL(p,s) and NUMERIC(p,s) - convert to TEXT for exact storage
	// This regex matches DECIMAL or NUMERIC with optional precision/scale
	decimalPattern := regexp.MustCompile(`(?i)\b(DECIMAL|NUMERIC)\s*(\(\s*\d+\s*(,\s*\d+\s*)?\))?`)
	sql = decimalPattern.ReplaceAllString(sql, "TEXT")

	return sql
}

// ExecuteDropTable handles DROP TABLE for temp tables and regular tables
func (h *DDLHandler) ExecuteDropTable(stmt *ast.DropTableStatement) error {
	if stmt == nil {
		return fmt.Errorf("invalid DROP TABLE statement")
	}

	for _, table := range stmt.Tables {
		tableName := table.String()

		if strings.HasPrefix(tableName, "#") {
			// Drop temp table
			if err := h.ctx.TempTables.DropTempTable(tableName); err != nil {
				if stmt.IfExists {
					continue
				}
				return err
			}
		} else if h.ctx.DB != nil {
			// Drop regular table via database
			sql := "DROP TABLE "
			if stmt.IfExists {
				sql += "IF EXISTS "
			}
			sql += tableName

			ctx := context.Background()
			var err error
			if h.ctx.Tx != nil {
				_, err = h.ctx.Tx.ExecContext(ctx, sql)
			} else {
				_, err = h.ctx.DB.ExecContext(ctx, sql)
			}
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("DROP TABLE for regular tables requires a database backend")
		}
	}

	return nil
}

// ExecuteTruncateTable handles TRUNCATE TABLE for temp tables
func (h *DDLHandler) ExecuteTruncateTable(stmt *ast.TruncateTableStatement) error {
	if stmt == nil || stmt.Table == nil {
		return fmt.Errorf("invalid TRUNCATE TABLE statement")
	}

	tableName := stmt.Table.String()

	if strings.HasPrefix(tableName, "#") {
		// Truncate temp table
		table, ok := h.ctx.TempTables.GetTempTable(tableName)
		if !ok {
			return fmt.Errorf("temp table %s does not exist", tableName)
		}
		table.Truncate()
		h.ctx.UpdateRowCount(0)
		return nil
	}

	// For regular tables, use DELETE (SQLite doesn't have TRUNCATE)
	if h.ctx.DB != nil {
		sql := "DELETE FROM " + tableName
		ctx := context.Background()
		var err error
		if h.ctx.Tx != nil {
			_, err = h.ctx.Tx.ExecContext(ctx, sql)
		} else {
			_, err = h.ctx.DB.ExecContext(ctx, sql)
		}
		return err
	}

	return fmt.Errorf("TRUNCATE TABLE for regular tables requires a database backend")
}

// ExecuteSelectInto handles SELECT INTO #temp
func (h *DDLHandler) ExecuteSelectInto(columns []string, rows [][]Value, intoTable string) error {
	if !strings.HasPrefix(intoTable, "#") && !strings.HasPrefix(intoTable, "@") {
		return fmt.Errorf("SELECT INTO only supported for temp tables (#table) or table variables (@table)")
	}

	// Create column definitions from the result set
	colDefs := make([]TempTableColumn, len(columns))
	for i, name := range columns {
		colDefs[i] = TempTableColumn{
			Name:     name,
			Type:     TypeVarChar, // Default type - could infer from data
			Nullable: true,
			MaxLen:   -1,
		}

		// Try to infer type from first row
		if len(rows) > 0 && i < len(rows[0]) {
			colDefs[i].Type = rows[0][i].Type
		}
	}

	// Create the table
	var table *TempTable
	if strings.HasPrefix(intoTable, "@") {
		tv, err := h.ctx.TempTables.CreateTableVariable(intoTable, colDefs)
		if err != nil {
			return err
		}
		table = tv.TempTable
	} else {
		var err error
		table, err = h.ctx.TempTables.CreateTempTable(intoTable, colDefs)
		if err != nil {
			return err
		}
	}

	// Insert all rows
	for _, row := range rows {
		if _, err := table.InsertRow(row); err != nil {
			return err
		}
	}

	h.ctx.UpdateRowCount(int64(len(rows)))

	return nil
}

// parseColumnDefinitions parses column definitions from AST
func (h *DDLHandler) parseColumnDefinitions(defs []*ast.ColumnDefinition) []TempTableColumn {
	columns := make([]TempTableColumn, len(defs))
	for i, def := range defs {
		columns[i] = h.parseColumnDef(def)
	}
	return columns
}

// parseColumnDef parses a single column definition
func (h *DDLHandler) parseColumnDef(def *ast.ColumnDefinition) TempTableColumn {
	col := TempTableColumn{
		Name:     def.Name.Value,
		Nullable: true, // Default to nullable
	}

	// Parse data type
	if def.DataType != nil {
		col.Type, col.Precision, col.Scale, col.MaxLen = ParseDataType(def.DataType.String())
	}

	// Handle Nullable field
	if def.Nullable != nil {
		col.Nullable = *def.Nullable
	}

	// Handle Identity
	if def.Identity != nil {
		col.Identity = true
		col.IdentitySeed = def.Identity.Seed
		col.IdentityIncr = def.Identity.Increment
		if col.IdentitySeed == 0 {
			col.IdentitySeed = 1
		}
		if col.IdentityIncr == 0 {
			col.IdentityIncr = 1
		}
	}

	// Handle Default
	if def.Default != nil {
		col.DefaultValue = NewVarChar(def.Default.String(), -1)
	}

	// Check inline constraints
	for _, constraint := range def.Constraints {
		if constraint.IsPrimaryKey {
			col.Nullable = false
		}
	}

	// Set default value to NULL if not specified
	if col.DefaultValue.Type == TypeUnknown {
		col.DefaultValue = Null(col.Type)
	}

	return col
}

// DeclareTableVariable handles DECLARE @t TABLE (...)
func (h *DDLHandler) DeclareTableVariable(name string, columns []TempTableColumn) error {
	_, err := h.ctx.TempTables.CreateTableVariable(name, columns)
	return err
}

// IsTempTable checks if a table name refers to a temp table
func IsTempTable(name string) bool {
	return strings.HasPrefix(name, "#")
}

// IsTableVariable checks if a name refers to a table variable
func IsTableVariable(name string) bool {
	return strings.HasPrefix(name, "@") && !strings.HasPrefix(name, "@@")
}

// ExecuteCreateIndex handles CREATE INDEX statements
func (h *DDLHandler) ExecuteCreateIndex(stmt *ast.CreateIndexStatement) error {
	if stmt == nil {
		return fmt.Errorf("invalid CREATE INDEX statement")
	}

	// Only support on database backend
	if h.ctx.DB == nil {
		return fmt.Errorf("CREATE INDEX requires a database backend")
	}

	// Generate SQLite-compatible CREATE INDEX
	sql := h.generateSQLiteCreateIndex(stmt)

	ctx := context.Background()
	var err error
	if h.ctx.Tx != nil {
		_, err = h.ctx.Tx.ExecContext(ctx, sql)
	} else {
		_, err = h.ctx.DB.ExecContext(ctx, sql)
	}
	return err
}

// generateSQLiteCreateIndex generates SQLite-compatible CREATE INDEX
func (h *DDLHandler) generateSQLiteCreateIndex(stmt *ast.CreateIndexStatement) string {
	var sb strings.Builder

	sb.WriteString("CREATE ")
	if stmt.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")

	// Index name
	if stmt.Name != nil {
		sb.WriteString(stmt.Name.Value)
	}

	sb.WriteString(" ON ")

	// Table name
	if stmt.Table != nil {
		sb.WriteString(stmt.Table.String())
	}

	// Columns
	sb.WriteString(" (")
	var cols []string
	for _, col := range stmt.Columns {
		colStr := col.Name.Value
		if col.Descending {
			colStr += " DESC"
		}
		cols = append(cols, colStr)
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	return sb.String()
}
