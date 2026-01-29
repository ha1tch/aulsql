// Package procedure provides stored procedure management for aul.
//
// It handles loading procedures from SQL files, parsing them to extract
// metadata (name, parameters, return types), and maintaining a registry
// of available procedures for execution.
package procedure

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/aul/pkg/annotations"
	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
)

// Dialect identifies the SQL dialect of a procedure.
type Dialect string

const (
	DialectTSQL     Dialect = "tsql"     // Microsoft T-SQL
	DialectPostgres Dialect = "postgres" // PostgreSQL PL/pgSQL
	DialectMySQL    Dialect = "mysql"    // MySQL stored procedures
)

func (d Dialect) String() string {
	return string(d)
}

// Procedure represents a stored procedure.
type Procedure struct {
	// Identity
	Name     string  // Procedure name (without schema)
	Database string  // Database name (e.g., "master", "salesdb")
	Schema   string  // Schema name (e.g., "dbo")
	Dialect  Dialect // SQL dialect
	FullName string  // Schema.Name (for backward compat)

	// Source
	Source     string // Original SQL source
	SourceFile string // File path (if loaded from file)
	SourceHash string // Hash of source for change detection

	// Location flags
	IsGlobal bool   // True if from _global directory (shared across databases)
	Tenant   string // Tenant ID if this is a tenant-specific override

	// Metadata
	Parameters  []Parameter
	ResultSets  []ResultSetDef
	ReturnType  string // Return type for scalar functions
	IsFunction  bool   // True if this is a function, not procedure
	IsTVF       bool   // True if table-valued function

	// Annotations from -- @aul: directives
	Annotations map[string]string

	// Timestamps
	LoadedAt   time.Time
	ModifiedAt time.Time

	// Execution state
	ExecCount   int64 // Number of times executed
	TotalTimeNs int64 // Total execution time in nanoseconds
	LastExecAt  time.Time

	// JIT compilation state
	JITCompiled bool        // Whether JIT-compiled version exists
	JITCode     interface{} // Compiled Go code (func pointer or plugin)
	JITCompiledAt time.Time
}

// QualifiedName returns the fully qualified procedure name.
// Format: [database.][schema.]name
func (p *Procedure) QualifiedName() string {
	var parts []string
	if p.Database != "" {
		parts = append(parts, p.Database)
	}
	if p.Schema != "" {
		parts = append(parts, p.Schema)
	}
	parts = append(parts, p.Name)
	return strings.Join(parts, ".")
}

// ShortName returns schema.name (for backward compatibility).
func (p *Procedure) ShortName() string {
	if p.Schema != "" {
		return p.Schema + "." + p.Name
	}
	return p.Name
}

// AvgExecTimeMs returns the average execution time in milliseconds.
func (p *Procedure) AvgExecTimeMs() float64 {
	if p.ExecCount == 0 {
		return 0
	}
	return float64(p.TotalTimeNs) / float64(p.ExecCount) / 1_000_000
}

// Parameter describes a procedure parameter.
type Parameter struct {
	Name       string // Parameter name (without @)
	SQLType    string // Original SQL type
	GoType     string // Mapped Go type
	Direction  ParamDirection
	HasDefault bool
	Default    interface{}
	Ordinal    int
}

// ParamDirection indicates parameter direction.
type ParamDirection int

const (
	ParamIn    ParamDirection = iota // Input only
	ParamOut                         // Output only
	ParamInOut                       // Input and output
)

func (d ParamDirection) String() string {
	switch d {
	case ParamIn:
		return "IN"
	case ParamOut:
		return "OUT"
	case ParamInOut:
		return "INOUT"
	default:
		return "UNKNOWN"
	}
}

// ResultSetDef describes an expected result set.
type ResultSetDef struct {
	Columns []ColumnDef
	Index   int // Result set index (0-based)
}

// ColumnDef describes a column in a result set.
type ColumnDef struct {
	Name     string
	SQLType  string
	GoType   string
	Nullable bool
	Ordinal  int
}

// Registry maintains a collection of stored procedures.
type Registry struct {
	mu         sync.RWMutex
	procedures map[string]*Procedure // key: lowercase qualified name (db.schema.name)
	byFile     map[string]*Procedure // key: source file path
	globals    map[string]*Procedure // key: lowercase schema.name (global procedures)
	tenants    map[string]map[string]*Procedure // key: tenant -> qualified name -> procedure
}

// NewRegistry creates a new procedure registry.
func NewRegistry() *Registry {
	return &Registry{
		procedures: make(map[string]*Procedure),
		byFile:     make(map[string]*Procedure),
		globals:    make(map[string]*Procedure),
		tenants:    make(map[string]map[string]*Procedure),
	}
}

// Register adds a procedure to the registry.
func (r *Registry) Register(proc *Procedure) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := strings.ToLower(proc.QualifiedName())

	// Handle tenant-specific procedures
	if proc.Tenant != "" {
		return r.registerTenantProcedure(proc, key)
	}

	// Check for duplicate in main registry
	if existing, ok := r.procedures[key]; ok {
		// Allow re-registration if source changed
		if existing.SourceHash == proc.SourceHash {
			return aulerrors.Newf(aulerrors.ErrCodeProcAlreadyExists,
				"procedure already registered: %s", proc.QualifiedName()).
				WithOp("Registry.Register").
				WithField("procedure", proc.QualifiedName()).
				Err()
		}
	}

	r.procedures[key] = proc
	if proc.SourceFile != "" {
		r.byFile[proc.SourceFile] = proc
	}

	// Also register globals by short name for fallback lookup
	if proc.IsGlobal {
		shortKey := strings.ToLower(proc.ShortName())
		r.globals[shortKey] = proc
	}

	return nil
}

// registerTenantProcedure registers a tenant-specific procedure override.
// Must be called with lock held.
func (r *Registry) registerTenantProcedure(proc *Procedure, key string) error {
	tenant := strings.ToLower(proc.Tenant)

	// Ensure tenant map exists
	if r.tenants[tenant] == nil {
		r.tenants[tenant] = make(map[string]*Procedure)
	}

	// Check for duplicate
	if existing, ok := r.tenants[tenant][key]; ok {
		if existing.SourceHash == proc.SourceHash {
			return aulerrors.Newf(aulerrors.ErrCodeProcAlreadyExists,
				"tenant procedure already registered: %s (tenant: %s)", proc.QualifiedName(), tenant).
				WithOp("Registry.Register").
				WithField("procedure", proc.QualifiedName()).
				WithField("tenant", tenant).
				Err()
		}
	}

	r.tenants[tenant][key] = proc
	if proc.SourceFile != "" {
		r.byFile[proc.SourceFile] = proc
	}

	return nil
}

// Unregister removes a procedure from the registry.
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := strings.ToLower(name)
	proc, ok := r.procedures[key]
	if !ok {
		return aulerrors.NotFound("procedure", name).
			WithOp("Registry.Unregister").
			Err()
	}

	delete(r.procedures, key)
	if proc.SourceFile != "" {
		delete(r.byFile, proc.SourceFile)
	}
	if proc.IsGlobal {
		shortKey := strings.ToLower(proc.ShortName())
		delete(r.globals, shortKey)
	}

	return nil
}

// Lookup finds a procedure by name.
// Resolution order:
//  1. Exact match (db.schema.name)
//  2. If database provided: db.dbo.name
//  3. Global procedures (schema.name)
//  4. Global procedures (dbo.name)
func (r *Registry) Lookup(name string) (*Procedure, error) {
	return r.LookupInDatabase(name, "")
}

// LookupForTenant finds a procedure with tenant-specific override support.
// Resolution order:
//  1. Tenant override (if tenant provided)
//  2. Database-specific procedure
//  3. Global procedures
func (r *Registry) LookupForTenant(name, database, tenant string) (*Procedure, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := strings.ToLower(name)
	parts := strings.Split(key, ".")

	// 1. Try tenant-specific override first
	if tenant != "" {
		tenantLower := strings.ToLower(tenant)
		if tenantProcs, ok := r.tenants[tenantLower]; ok {
			if proc := r.lookupInMap(tenantProcs, key, parts, database); proc != nil {
				return proc, nil
			}
		}
	}

	// 2. Try main procedures
	if proc := r.lookupInMap(r.procedures, key, parts, database); proc != nil {
		return proc, nil
	}

	// 3. Try global procedures
	if proc := r.lookupGlobal(key, parts); proc != nil {
		return proc, nil
	}

	return nil, aulerrors.NotFound("procedure", name).
		WithOp("Registry.LookupForTenant").
		Err()
}

// lookupInMap searches for a procedure in a map with database context.
// Must be called with lock held.
func (r *Registry) lookupInMap(procs map[string]*Procedure, key string, parts []string, database string) *Procedure {
	// Exact match
	if proc, ok := procs[key]; ok {
		return proc
	}

	// Try with database context
	if database != "" {
		dbLower := strings.ToLower(database)

		switch len(parts) {
		case 1:
			// name only -> try db.dbo.name
			if proc, ok := procs[dbLower+".dbo."+key]; ok {
				return proc
			}
		case 2:
			// schema.name -> try db.schema.name
			if proc, ok := procs[dbLower+"."+key]; ok {
				return proc
			}
		}
	}

	return nil
}

// lookupGlobal searches for a procedure in globals.
// Must be called with lock held.
func (r *Registry) lookupGlobal(key string, parts []string) *Procedure {
	switch len(parts) {
	case 1:
		// name only -> try dbo.name in globals
		if proc, ok := r.globals["dbo."+key]; ok {
			return proc
		}
	case 2:
		// schema.name -> try in globals
		if proc, ok := r.globals[key]; ok {
			return proc
		}
	case 3:
		// db.schema.name -> strip db, try schema.name in globals
		shortKey := parts[1] + "." + parts[2]
		if proc, ok := r.globals[shortKey]; ok {
			return proc
		}
	}
	return nil
}

// LookupInDatabase finds a procedure, scoped to a database context.
// Resolution order:
//  1. Exact match (db.schema.name or schema.name)
//  2. database.schema.name (if database provided and name has schema)
//  3. database.dbo.name (if database provided)
//  4. Global procedures (schema.name)
//  5. Global procedures (dbo.name)
func (r *Registry) LookupInDatabase(name, database string) (*Procedure, error) {
	// Delegate to tenant-aware lookup with empty tenant
	return r.LookupForTenant(name, database, "")
}

// LookupByFile finds a procedure by its source file.
func (r *Registry) LookupByFile(path string) (*Procedure, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if proc, ok := r.byFile[path]; ok {
		return proc, nil
	}

	return nil, aulerrors.Newf(aulerrors.ErrCodeProcNotFound,
		"no procedure loaded from: %s", path).
		WithOp("Registry.LookupByFile").
		WithField("path", path).
		Err()
}

// List returns all registered procedures.
func (r *Registry) List() []*Procedure {
	r.mu.RLock()
	defer r.mu.RUnlock()

	procs := make([]*Procedure, 0, len(r.procedures))
	for _, proc := range r.procedures {
		procs = append(procs, proc)
	}
	return procs
}

// Count returns the number of registered procedures.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.procedures)
}

// Loader loads procedures from files.
type Loader struct {
	dialect Dialect
	parser  Parser
	logger  *log.Logger
}

// NewLoader creates a new procedure loader.
func NewLoader(dialect string, logger *log.Logger) *Loader {
	d := Dialect(dialect)
	return &Loader{
		dialect: d,
		parser:  NewParser(d),
		logger:  logger,
	}
}

// LoadFile loads a procedure from a SQL file.
func (l *Loader) LoadFile(path string) (*Procedure, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
			"failed to read file").
			WithOp("Loader.LoadFile").
			WithField("path", path).
			Err()
	}

	proc, err := l.parser.Parse(string(source))
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcParseError,
			"failed to parse procedure").
			WithOp("Loader.LoadFile").
			WithField("path", path).
			Err()
	}

	proc.SourceFile = path
	proc.LoadedAt = time.Now()

	// Get file modification time
	if info, err := os.Stat(path); err == nil {
		proc.ModifiedAt = info.ModTime()
	}

	l.logger.Application().Debug("procedure file loaded",
		"path", path,
		"procedure", proc.QualifiedName(),
	)

	return proc, nil
}

// LoadDir loads all procedures from a directory.
func (l *Loader) LoadDir(dir string) ([]*Procedure, error) {
	var procs []*Procedure
	var loadErrors []error

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-SQL files
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".sql") {
			return nil
		}

		proc, err := l.LoadFile(path)
		if err != nil {
			l.logger.Application().Warn("failed to load procedure file",
				"path", path,
				"error", err.Error(),
			)
			loadErrors = append(loadErrors, err)
			return nil
		}

		procs = append(procs, proc)
		return nil
	})

	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
			"failed to walk directory").
			WithOp("Loader.LoadDir").
			WithField("directory", dir).
			Err()
	}

	if len(loadErrors) > 0 {
		l.logger.Application().Warn("some procedures failed to load",
			"successful", len(procs),
			"failed", len(loadErrors),
		)
	}

	return procs, nil
}

// Parser parses SQL source to extract procedure metadata.
type Parser interface {
	Parse(source string) (*Procedure, error)
}

// NewParser creates a parser for the given dialect.
func NewParser(dialect Dialect) Parser {
	switch dialect {
	case DialectTSQL:
		return &TSQLParser{}
	case DialectPostgres:
		return &PostgresParser{}
	case DialectMySQL:
		return &MySQLParser{}
	default:
		return &TSQLParser{} // Default to T-SQL
	}
}


// TSQLParser parses T-SQL procedures.
// Note: Full AST parsing requires tsqlparser integration.
// This implementation uses string-based extraction as a fallback.
type TSQLParser struct{}

// Parse extracts procedure metadata from T-SQL source.
func (p *TSQLParser) Parse(source string) (*Procedure, error) {
	proc := &Procedure{
		Source:      source,
		Dialect:     DialectTSQL,
		Annotations: make(map[string]string),
	}

	// Extract annotations
	annParser := annotations.NewParser()
	stmtAnnotations := annParser.Extract(source)

	lines := strings.Split(source, "\n")
	for lineNum, line := range lines {
		upper := strings.ToUpper(strings.TrimSpace(line))
		currentLine := lineNum + 1 // 1-indexed

		// Look for CREATE PROCEDURE
		if strings.HasPrefix(upper, "CREATE PROCEDURE") || strings.HasPrefix(upper, "CREATE PROC") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				name := parts[2]
				if strings.Contains(name, ".") {
					nameParts := strings.SplitN(name, ".", 2)
					proc.Schema = strings.Trim(nameParts[0], "[]")
					proc.Name = strings.Trim(nameParts[1], "[]")
				} else {
					proc.Name = strings.Trim(name, "[]")
					proc.Schema = "dbo"
				}
				proc.FullName = proc.Schema + "." + proc.Name
			}

			// Find annotations for this statement
			for _, sa := range stmtAnnotations {
				if sa.StmtLine == currentLine {
					for k, v := range sa.Annotations {
						proc.Annotations[k] = v
					}
					break
				}
			}
			break
		}

		// Look for CREATE FUNCTION
		if strings.HasPrefix(upper, "CREATE FUNCTION") {
			proc.IsFunction = true
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				name := parts[2]
				// Remove parameters if on same line: fn_Name(@X INT) -> fn_Name
				if idx := strings.Index(name, "("); idx > 0 {
					name = name[:idx]
				}
				if strings.Contains(name, ".") {
					nameParts := strings.SplitN(name, ".", 2)
					proc.Schema = strings.Trim(nameParts[0], "[]")
					proc.Name = strings.Trim(nameParts[1], "[]")
				} else {
					proc.Name = strings.Trim(name, "[]")
					proc.Schema = "dbo"
				}
				proc.FullName = proc.Schema + "." + proc.Name
			}

			// Find annotations for this statement
			for _, sa := range stmtAnnotations {
				if sa.StmtLine == currentLine {
					for k, v := range sa.Annotations {
						proc.Annotations[k] = v
					}
					break
				}
			}
			break
		}
	}

	if proc.Name == "" {
		return nil, aulerrors.New(aulerrors.ErrCodeProcParseError,
			"could not find procedure/function name in source").
			WithOp("TSQLParser.Parse").
			Err()
	}

	// Extract parameters using simple pattern matching
	proc.Parameters = p.extractParameters(source)

	// Compute source hash for change detection
	proc.SourceHash = computeHash(source)

	return proc, nil
}

// extractParameters extracts parameter definitions from source using pattern matching
func (p *TSQLParser) extractParameters(source string) []Parameter {
	var params []Parameter

	// Find the parameter section (between procedure name and AS)
	upper := strings.ToUpper(source)
	asIdx := strings.Index(upper, "\nAS\n")
	if asIdx == -1 {
		asIdx = strings.Index(upper, "\nAS ")
	}
	if asIdx == -1 {
		asIdx = len(source)
	}

	// Look for @param patterns
	lines := strings.Split(source[:asIdx], "\n")
	ordinal := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "@") {
			// Parse parameter: @name type [= default] [OUTPUT]
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				param := Parameter{
					Name:      strings.TrimPrefix(strings.TrimSuffix(parts[0], ","), "@"),
					Ordinal:   ordinal,
					Direction: ParamIn,
				}

				// Get type (remove trailing comma if present)
				param.SQLType = strings.TrimSuffix(parts[1], ",")
				param.GoType = mapSQLTypeToGo(param.SQLType)

				// Check for OUTPUT keyword
				lineUpper := strings.ToUpper(line)
				if strings.Contains(lineUpper, " OUTPUT") || strings.Contains(lineUpper, " OUT") {
					param.Direction = ParamOut
				}

				// Check for default value
				if strings.Contains(line, "=") {
					param.HasDefault = true
					eqIdx := strings.Index(line, "=")
					if eqIdx > 0 {
						rest := strings.TrimSpace(line[eqIdx+1:])
						rest = strings.Split(rest, ",")[0]
						rest = strings.Split(rest, " ")[0]
						param.Default = rest
					}
				}

				params = append(params, param)
				ordinal++
			}
		}
	}

	return params
}
func mapSQLTypeToGo(sqlType string) string {
	upper := strings.ToUpper(sqlType)

	// Remove size specifications for matching
	if idx := strings.Index(upper, "("); idx > 0 {
		upper = upper[:idx]
	}

	switch upper {
	case "BIT":
		return "bool"
	case "TINYINT":
		return "uint8"
	case "SMALLINT":
		return "int16"
	case "INT", "INTEGER":
		return "int32"
	case "BIGINT":
		return "int64"
	case "FLOAT", "REAL":
		return "float64"
	case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
		return "decimal.Decimal"
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "NTEXT":
		return "string"
	case "DATE", "TIME", "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET":
		return "time.Time"
	case "BINARY", "VARBINARY", "IMAGE":
		return "[]byte"
	case "UNIQUEIDENTIFIER":
		return "string"
	case "XML":
		return "string"
	default:
		return "interface{}"
	}
}

// PostgresParser parses PostgreSQL procedures.
type PostgresParser struct{}

func (p *PostgresParser) Parse(source string) (*Procedure, error) {
	return nil, aulerrors.NotImplemented("PostgreSQL parser").
		WithOp("PostgresParser.Parse").
		Err()
}

// MySQLParser parses MySQL procedures.
type MySQLParser struct{}

func (p *MySQLParser) Parse(source string) (*Procedure, error) {
	return nil, aulerrors.NotImplemented("MySQL parser").
		WithOp("MySQLParser.Parse").
		Err()
}

// computeHash computes a SHA256 hash of the source for change detection.
func computeHash(source string) string {
	h := sha256.New()
	h.Write([]byte(source))
	return hex.EncodeToString(h.Sum(nil))[:16] // First 16 chars of hex
}
