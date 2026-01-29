package tsqlruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/ha1tch/aul/tsqlparser/ast"
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
	// Generate DDL SQL from AST
	sql := stmt.String()

	// Normalize for target dialect
	sql = h.normalizer.Normalize(sql)

	// Also normalize type names for SQLite compatibility
	sql = h.normalizeTypes(sql)

	ctx := context.Background()
	var err error
	if h.ctx.Tx != nil {
		_, err = h.ctx.Tx.ExecContext(ctx, sql)
	} else {
		_, err = h.ctx.DB.ExecContext(ctx, sql)
	}
	return err
}

// normalizeTypes converts T-SQL types to SQLite types
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

		// Decimal/float types (preserve as-is or map to REAL)
		{"MONEY", "REAL"},
		{"SMALLMONEY", "REAL"},
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
