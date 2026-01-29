package tsqlruntime

import (
	"regexp"
	"strings"
)

// SQLNormalizer translates T-SQL specific syntax to target dialect.
type SQLNormalizer struct {
	dialect Dialect
}

// NewSQLNormalizer creates a normalizer for the given dialect.
func NewSQLNormalizer(dialect Dialect) *SQLNormalizer {
	return &SQLNormalizer{dialect: dialect}
}

// Normalize converts T-SQL syntax to the target dialect.
func (n *SQLNormalizer) Normalize(sql string) string {
	switch n.dialect {
	case DialectSQLite:
		return n.normalizeForSQLite(sql)
	case DialectPostgres:
		return n.normalizeForPostgres(sql)
	case DialectMySQL:
		return n.normalizeForMySQL(sql)
	case DialectSQLServer:
		// No normalization needed for SQL Server
		return sql
	default:
		// Generic: try SQLite-style normalization
		return n.normalizeForSQLite(sql)
	}
}

// normalizeForSQLite converts T-SQL to SQLite dialect.
func (n *SQLNormalizer) normalizeForSQLite(sql string) string {
	// GETDATE() -> datetime('now')
	sql = replaceFunction(sql, "GETDATE", "datetime('now')")
	sql = replaceFunction(sql, "SYSDATETIME", "datetime('now')")
	
	// GETUTCDATE() -> datetime('now', 'utc')
	sql = replaceFunction(sql, "GETUTCDATE", "datetime('now', 'utc')")
	sql = replaceFunction(sql, "SYSUTCDATETIME", "datetime('now', 'utc')")

	// ISNULL(a, b) -> IFNULL(a, b)
	sql = replaceFunctionName(sql, "ISNULL", "IFNULL")

	// LEN(s) -> LENGTH(s)
	sql = replaceFunctionName(sql, "LEN", "LENGTH")
	sql = replaceFunctionName(sql, "DATALENGTH", "LENGTH")

	// CHARINDEX(sub, str) -> INSTR(str, sub) - argument order swapped!
	sql = replaceCharIndex(sql)

	// SUBSTRING(str, start, len) -> SUBSTR(str, start, len)
	sql = replaceFunctionName(sql, "SUBSTRING", "SUBSTR")

	// CONVERT(type, value) -> CAST(value AS type) - complex, handle common cases
	sql = replaceConvert(sql)

	// NEWID() -> lower(hex(randomblob(16)))
	sql = replaceFunction(sql, "NEWID", "lower(hex(randomblob(16)))")

	// String concatenation: 'a' + 'b' -> 'a' || 'b'
	// This is tricky because + is also arithmetic. We handle simple cases.
	sql = replaceStringConcat(sql)

	// TOP N -> LIMIT N (handled separately in query building, but try basic case)
	sql = replaceTopWithLimit(sql)

	return sql
}

// normalizeForPostgres converts T-SQL to PostgreSQL dialect.
func (n *SQLNormalizer) normalizeForPostgres(sql string) string {
	// GETDATE() -> NOW()
	sql = replaceFunction(sql, "GETDATE", "NOW()")
	sql = replaceFunction(sql, "SYSDATETIME", "NOW()")

	// GETUTCDATE() -> NOW() AT TIME ZONE 'UTC'
	sql = replaceFunction(sql, "GETUTCDATE", "(NOW() AT TIME ZONE 'UTC')")
	sql = replaceFunction(sql, "SYSUTCDATETIME", "(NOW() AT TIME ZONE 'UTC')")

	// ISNULL(a, b) -> COALESCE(a, b)
	sql = replaceFunctionName(sql, "ISNULL", "COALESCE")

	// LEN(s) -> LENGTH(s)
	sql = replaceFunctionName(sql, "LEN", "LENGTH")
	sql = replaceFunctionName(sql, "DATALENGTH", "OCTET_LENGTH")

	// CHARINDEX(sub, str) -> POSITION(sub IN str)
	sql = replaceCharIndexPostgres(sql)

	// NEWID() -> gen_random_uuid()
	sql = replaceFunction(sql, "NEWID", "gen_random_uuid()")

	// TOP N -> LIMIT N
	sql = replaceTopWithLimit(sql)

	return sql
}

// normalizeForMySQL converts T-SQL to MySQL dialect.
func (n *SQLNormalizer) normalizeForMySQL(sql string) string {
	// GETDATE() -> NOW()
	sql = replaceFunction(sql, "GETDATE", "NOW()")
	sql = replaceFunction(sql, "SYSDATETIME", "NOW()")

	// GETUTCDATE() -> UTC_TIMESTAMP()
	sql = replaceFunction(sql, "GETUTCDATE", "UTC_TIMESTAMP()")
	sql = replaceFunction(sql, "SYSUTCDATETIME", "UTC_TIMESTAMP()")

	// ISNULL(a, b) -> IFNULL(a, b) or COALESCE(a, b)
	sql = replaceFunctionName(sql, "ISNULL", "IFNULL")

	// LEN(s) -> LENGTH(s) or CHAR_LENGTH(s)
	sql = replaceFunctionName(sql, "LEN", "CHAR_LENGTH")
	sql = replaceFunctionName(sql, "DATALENGTH", "LENGTH")

	// CHARINDEX(sub, str) -> LOCATE(sub, str)
	sql = replaceFunctionName(sql, "CHARINDEX", "LOCATE")

	// NEWID() -> UUID()
	sql = replaceFunction(sql, "NEWID", "UUID()")

	// TOP N -> LIMIT N
	sql = replaceTopWithLimit(sql)

	return sql
}

// replaceFunction replaces a parameterless function call.
func replaceFunction(sql, funcName, replacement string) string {
	// Match FUNCNAME() with optional whitespace
	pattern := `(?i)\b` + funcName + `\s*\(\s*\)`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sql, replacement)
}

// replaceFunctionName replaces a function name, preserving arguments.
func replaceFunctionName(sql, oldName, newName string) string {
	// Match OLDNAME( preserving case-insensitivity
	pattern := `(?i)\b` + oldName + `\s*\(`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sql, newName+"(")
}

// replaceCharIndex handles CHARINDEX(sub, str) -> INSTR(str, sub) for SQLite.
// Arguments are swapped!
func replaceCharIndex(sql string) string {
	pattern := `(?i)\bCHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sql, "INSTR($2, $1)")
}

// replaceCharIndexPostgres handles CHARINDEX(sub, str) -> POSITION(sub IN str).
func replaceCharIndexPostgres(sql string) string {
	pattern := `(?i)\bCHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sql, "POSITION($1 IN $2)")
}

// replaceConvert handles common CONVERT patterns.
func replaceConvert(sql string) string {
	// CONVERT(VARCHAR, x) -> CAST(x AS TEXT)
	// CONVERT(INT, x) -> CAST(x AS INTEGER)
	// This is a simplified handler for common cases

	patterns := []struct {
		pattern string
		replace string
	}{
		// CONVERT(VARCHAR(n), x) -> CAST(x AS TEXT)
		{`(?i)\bCONVERT\s*\(\s*VARCHAR\s*\([^)]*\)\s*,\s*([^)]+)\)`, "CAST($1 AS TEXT)"},
		// CONVERT(NVARCHAR(n), x) -> CAST(x AS TEXT)
		{`(?i)\bCONVERT\s*\(\s*NVARCHAR\s*\([^)]*\)\s*,\s*([^)]+)\)`, "CAST($1 AS TEXT)"},
		// CONVERT(VARCHAR, x) -> CAST(x AS TEXT)
		{`(?i)\bCONVERT\s*\(\s*VARCHAR\s*,\s*([^)]+)\)`, "CAST($1 AS TEXT)"},
		// CONVERT(INT, x) -> CAST(x AS INTEGER)
		{`(?i)\bCONVERT\s*\(\s*INT\s*,\s*([^)]+)\)`, "CAST($1 AS INTEGER)"},
		// CONVERT(BIGINT, x) -> CAST(x AS INTEGER)
		{`(?i)\bCONVERT\s*\(\s*BIGINT\s*,\s*([^)]+)\)`, "CAST($1 AS INTEGER)"},
		// CONVERT(FLOAT, x) -> CAST(x AS REAL)
		{`(?i)\bCONVERT\s*\(\s*FLOAT\s*,\s*([^)]+)\)`, "CAST($1 AS REAL)"},
		// CONVERT(DECIMAL(p,s), x) -> CAST(x AS REAL)
		{`(?i)\bCONVERT\s*\(\s*DECIMAL\s*\([^)]*\)\s*,\s*([^)]+)\)`, "CAST($1 AS REAL)"},
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p.pattern)
		sql = re.ReplaceAllString(sql, p.replace)
	}

	return sql
}

// replaceTopWithLimit handles simple TOP N -> LIMIT N conversion.
// Note: This is a simplified handler - proper TOP handling should be in the AST.
func replaceTopWithLimit(sql string) string {
	// Only handle simple cases: SELECT TOP N ... -> SELECT ... LIMIT N
	// This won't handle all cases (e.g., TOP with ORDER BY, TOP PERCENT, etc.)
	
	pattern := `(?i)\bSELECT\s+TOP\s+(\d+)\s+`
	re := regexp.MustCompile(pattern)
	
	matches := re.FindStringSubmatch(sql)
	if len(matches) >= 2 {
		n := matches[1]
		// Remove TOP N from SELECT
		sql = re.ReplaceAllString(sql, "SELECT ")
		// Add LIMIT N at the end (if not already present)
		if !strings.Contains(strings.ToUpper(sql), "LIMIT") {
			sql = strings.TrimRight(sql, "; \t\n") + " LIMIT " + n
		}
	}
	
	return sql
}

// replaceStringConcat converts T-SQL string concatenation (+) to SQLite (||).
// This handles simple cases where strings are being concatenated.
func replaceStringConcat(sql string) string {
	// This is a heuristic approach - we look for patterns like:
	// 'string' + 'string' or 'string' + @var or @var + 'string'
	// We cannot reliably distinguish string + from numeric + without type info,
	// but we can handle obvious cases.

	// Pattern: 'literal' + followed by 'literal' or identifier
	// Replace 'a' + 'b' with 'a' || 'b'
	patterns := []struct {
		pattern string
		replace string
	}{
		// 'string' + 'string'
		{`'([^']*)'\s*\+\s*'`, "'$1' || '"},
		// 'string' + @var
		{`'([^']*)'\s*\+\s*@`, "'$1' || @"},
		// @var + 'string' (preceded by word boundary)
		{`(@\w+)\s*\+\s*'`, "$1 || '"},
		// ) + 'string' (result of expression + string literal)
		{`\)\s*\+\s*'`, ") || '"},
		// ? + 'string' (placeholder + string literal)
		{`\?\s*\+\s*'`, "? || '"},
		// 'string' + ? (string literal + placeholder)
		{`'([^']*)'\s*\+\s*\?`, "'$1' || ?"},
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p.pattern)
		sql = re.ReplaceAllString(sql, p.replace)
	}

	return sql
}
