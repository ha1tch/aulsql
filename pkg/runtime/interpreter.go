package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/pkg/procedure"
	"github.com/ha1tch/aul/pkg/tsqlruntime"
)

// interpreter wraps tsqlruntime.Interpreter for procedure execution.
type interpreter struct {
	config   Config
	logger   *log.Logger
	db       *sql.DB
	registry *procedure.Registry // For nested EXEC resolution
}

// newInterpreter creates a new interpreter instance.
func newInterpreter(cfg Config, logger *log.Logger, registry *procedure.Registry) *interpreter {
	return &interpreter{
		config:   cfg,
		logger:   logger,
		registry: registry,
	}
}

// SetDB sets the database connection for the interpreter.
func (i *interpreter) SetDB(db *sql.DB) {
	i.db = db
}

// mapDialect converts aul dialect string to tsqlruntime.Dialect
func mapDialect(dialect string) tsqlruntime.Dialect {
	switch dialect {
	case "postgres":
		return tsqlruntime.DialectPostgres
	case "mysql":
		return tsqlruntime.DialectMySQL
	case "sqlite":
		return tsqlruntime.DialectSQLite
	case "sqlserver", "tsql":
		return tsqlruntime.DialectSQLServer
	default:
		return tsqlruntime.DialectGeneric
	}
}

// Execute runs a stored procedure using the tsqlruntime interpreter.
func (i *interpreter) Execute(ctx context.Context, proc *procedure.Procedure, execCtx *ExecContext, storage StorageBackend) (*ExecResult, error) {
	// Validate procedure
	if proc == nil {
		return nil, aulerrors.New(aulerrors.ErrCodeProcInvalidParam, "nil procedure").
			WithOp("interpreter.Execute").
			Err()
	}
	if proc.Source == "" {
		return nil, aulerrors.Newf(aulerrors.ErrCodeProcInvalidParam,
			"procedure has no source: %s", proc.Name).
			WithOp("interpreter.Execute").
			WithField("procedure", proc.Name).
			Err()
	}

	// Check nesting level
	if execCtx.NestingLevel > i.config.MaxNestingLevel {
		return nil, aulerrors.Newf(aulerrors.ErrCodeExecNestingLimit,
			"maximum nesting level exceeded (%d)", i.config.MaxNestingLevel).
			WithOp("interpreter.Execute").
			WithField("procedure", proc.Name).
			WithField("nesting_level", execCtx.NestingLevel).
			WithField("max_nesting", i.config.MaxNestingLevel).
			Err()
	}

	i.logger.Execution().Debug("executing procedure (interpreted)",
		"procedure", proc.QualifiedName(),
		"session_id", execCtx.SessionID,
		"tenant", execCtx.Tenant,
		"nesting_level", execCtx.NestingLevel,
	)

	// Get database connection from storage backend
	// For tenant-aware storage, use the tenant's database
	var db *sql.DB
	if tenantStorage, ok := storage.(TenantAwareStorageBackend); ok && execCtx.Tenant != "" {
		var err error
		db, err = tenantStorage.GetDBForTenant(execCtx.Tenant, execCtx.Database)
		if err != nil {
			i.logger.Execution().Warn("failed to get tenant database, falling back to default",
				"tenant", execCtx.Tenant,
				"database", execCtx.Database,
				"error", err.Error(),
			)
			db = storage.GetDB()
		}
	} else {
		db = storage.GetDB()
	}

	if db == nil {
		// Fall back to in-memory execution (no actual DB queries)
		i.logger.Execution().Debug("no database connection, using in-memory execution",
			"procedure", proc.QualifiedName(),
		)
	}

	// Create tsqlruntime interpreter
	// Use the storage backend's dialect for proper SQL translation
	dialect := mapDialect(storage.Dialect())
	if dialect == tsqlruntime.DialectGeneric {
		// Fall back to configured dialect if storage doesn't specify
		dialect = mapDialect(i.config.DefaultDialect)
	}
	interp := tsqlruntime.NewInterpreter(db, dialect)
	interp.Debug = i.logger != nil && i.config.DefaultDialect == "debug"

	// Set up nested EXEC support with tenant context
	if i.registry != nil {
		interp.SetResolver(newTenantAwareResolver(i.registry, execCtx.Tenant))
	}
	interp.SetDatabase(execCtx.Database)
	interp.SetNestingLevel(execCtx.NestingLevel)

	// Set parameters as variables
	params := make(map[string]interface{})
	for name, value := range execCtx.Parameters {
		// Ensure parameter names have @ prefix for T-SQL
		paramName := name
		if len(paramName) > 0 && paramName[0] != '@' {
			paramName = "@" + paramName
		}
		params[paramName] = value
	}

	// Handle transaction context
	if execCtx.InTxn && execCtx.TxnContext != nil {
		// If we have a transaction, we'd need to pass it to the interpreter
		// For now, the interpreter will manage its own transactions
		i.logger.Execution().Debug("executing within transaction context",
			"txn_id", execCtx.TxnContext.ID,
		)
	}

	// Execute the procedure source
	result, err := interp.Execute(ctx, proc.Source, params)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeExecFailed,
			"procedure execution failed").
			WithOp("interpreter.Execute").
			WithField("procedure", proc.QualifiedName()).
			Err()
	}

	// Convert tsqlruntime.ExecutionResult to runtime.ExecResult
	execResult := &ExecResult{
		RowsAffected: result.RowsAffected,
		OutputParams: make(map[string]interface{}),
	}

	// Convert return value
	if result.ReturnValue != nil {
		execResult.ReturnValue = *result.ReturnValue
	}

	// Convert result sets
	for _, rs := range result.ResultSets {
		resultSet := ResultSet{
			Columns: make([]ColumnInfo, len(rs.Columns)),
			Rows:    make([][]interface{}, len(rs.Rows)),
		}

		// Set column info
		for j, col := range rs.Columns {
			resultSet.Columns[j] = ColumnInfo{
				Name:    col,
				Type:    "varchar", // tsqlruntime doesn't expose type info in ResultSet
				Ordinal: j,
			}
		}

		// Convert rows (tsqlruntime.Value to interface{})
		for j, row := range rs.Rows {
			resultSet.Rows[j] = make([]interface{}, len(row))
			for k, val := range row {
				resultSet.Rows[j][k] = tsqlruntime.FromValue(val)
			}
		}

		execResult.ResultSets = append(execResult.ResultSets, resultSet)
	}

	// Extract output parameters from interpreter
	// Output params would be variables that were declared as OUTPUT
	// For now, we get them from the procedure's parameter definitions
	for _, param := range proc.Parameters {
		if param.Direction == procedure.ParamOut || param.Direction == procedure.ParamInOut {
			if val, ok := interp.GetVariable("@" + param.Name); ok {
				execResult.OutputParams[param.Name] = val
			}
		}
	}

	i.logger.Execution().Debug("procedure execution completed",
		"procedure", proc.QualifiedName(),
		"rows_affected", execResult.RowsAffected,
		"result_sets", len(execResult.ResultSets),
	)

	return execResult, nil
}

// ExecuteSQL runs ad-hoc SQL using the tsqlruntime interpreter.
func (i *interpreter) ExecuteSQL(ctx context.Context, sqlStr string, execCtx *ExecContext, storage StorageBackend) (*ExecResult, error) {
	if sqlStr == "" {
		return nil, aulerrors.New(aulerrors.ErrCodeExecSQLError, "empty SQL").
			WithOp("interpreter.ExecuteSQL").
			Err()
	}

	i.logger.Execution().Debug("executing ad-hoc SQL",
		"session_id", execCtx.SessionID,
		"tenant", execCtx.Tenant,
		"sql_length", len(sqlStr),
	)

	// Check for system catalog queries - these are handled by the storage layer
	// which intercepts sys.* queries and returns SQL Server-compatible metadata
	normalizedSQL := strings.ToLower(strings.TrimSpace(sqlStr))
	if strings.Contains(normalizedSQL, "sys.") ||
		strings.Contains(normalizedSQL, "information_schema.") {
		// Route through storage layer which handles system catalog
		results, err := storage.Query(ctx, sqlStr)
		if err != nil {
			return nil, aulerrors.Wrap(err, aulerrors.ErrCodeExecSQLError,
				"SQL execution failed").
				WithOp("interpreter.ExecuteSQL").
				Err()
		}

		execResult := &ExecResult{}
		for _, rs := range results {
			execResult.ResultSets = append(execResult.ResultSets, rs)
		}
		return execResult, nil
	}

	// Get database connection from storage backend
	// For tenant-aware storage, use the tenant's database
	var db *sql.DB
	if tenantStorage, ok := storage.(TenantAwareStorageBackend); ok && execCtx.Tenant != "" {
		var err error
		db, err = tenantStorage.GetDBForTenant(execCtx.Tenant, execCtx.Database)
		if err != nil {
			i.logger.Execution().Warn("failed to get tenant database for SQL, falling back to default",
				"tenant", execCtx.Tenant,
				"database", execCtx.Database,
				"error", err.Error(),
			)
			db = storage.GetDB()
		}
	} else {
		db = storage.GetDB()
	}

	// Determine dialect from storage backend (auto-detect) or use configured default
	dialect := mapDialect(storage.Dialect())
	if dialect == tsqlruntime.DialectGeneric {
		// Fall back to configured dialect if storage doesn't specify
		dialect = mapDialect(i.config.DefaultDialect)
	}
	interp := tsqlruntime.NewInterpreter(db, dialect)

	// Configure rewritten query logging
	if i.config.LogQueriesRewritten && i.logger != nil {
		interp.LogRewritten = true
		interp.LogFunc = func(format string, args ...interface{}) {
			i.logger.Execution().Info(fmt.Sprintf(format, args...),
				"session_id", execCtx.SessionID,
			)
		}
	}

	// Set database context for procedure resolution
	if execCtx.Database != "" {
		interp.SetDatabase(execCtx.Database)
	}

	// Set resolver for nested EXEC support
	if i.registry != nil {
		if execCtx.Tenant != "" {
			interp.SetResolver(newTenantAwareResolver(i.registry, execCtx.Tenant))
		} else {
			interp.SetResolver(newRegistryResolver(i.registry))
		}
	}

	// Set parameters
	params := make(map[string]interface{})
	for name, value := range execCtx.Parameters {
		paramName := name
		if len(paramName) > 0 && paramName[0] != '@' {
			paramName = "@" + paramName
		}
		params[paramName] = value
	}

	// Execute
	result, err := interp.Execute(ctx, sqlStr, params)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeExecSQLError,
			"SQL execution failed").
			WithOp("interpreter.ExecuteSQL").
			Err()
	}

	// Convert result
	execResult := &ExecResult{
		RowsAffected: result.RowsAffected,
	}

	// Convert result sets
	for _, rs := range result.ResultSets {
		resultSet := ResultSet{
			Columns: make([]ColumnInfo, len(rs.Columns)),
			Rows:    make([][]interface{}, len(rs.Rows)),
		}

		for j, col := range rs.Columns {
			resultSet.Columns[j] = ColumnInfo{
				Name:    col,
				Type:    "varchar",
				Ordinal: j,
			}
		}

		for j, row := range rs.Rows {
			resultSet.Rows[j] = make([]interface{}, len(row))
			for k, val := range row {
				resultSet.Rows[j][k] = tsqlruntime.FromValue(val)
			}
		}

		execResult.ResultSets = append(execResult.ResultSets, resultSet)
	}

	return execResult, nil
}

// Reset clears the interpreter state for reuse.
func (i *interpreter) Reset() {
	// The interpreter is recreated for each execution, so nothing to reset
}

// registryResolver adapts procedure.Registry to tsqlruntime.ProcedureResolver.
type registryResolver struct {
	registry *procedure.Registry
}

// Resolve implements tsqlruntime.ProcedureResolver.
func (r *registryResolver) Resolve(ctx context.Context, name string, database string) (source string, params []tsqlruntime.ProcedureParam, err error) {
	// Try to lookup the procedure
	proc, err := r.registry.LookupInDatabase(name, database)
	if err != nil {
		return "", nil, err
	}

	// Convert procedure parameters
	params = make([]tsqlruntime.ProcedureParam, len(proc.Parameters))
	for i, p := range proc.Parameters {
		params[i] = tsqlruntime.ProcedureParam{
			Name:       p.Name,
			SQLType:    p.SQLType,
			IsOutput:   p.Direction == procedure.ParamOut || p.Direction == procedure.ParamInOut,
			HasDefault: p.HasDefault,
			Default:    p.Default,
		}
	}

	return proc.Source, params, nil
}

// newRegistryResolver creates a resolver that uses the procedure registry.
func newRegistryResolver(registry *procedure.Registry) tsqlruntime.ProcedureResolver {
	if registry == nil {
		return nil
	}
	return &registryResolver{registry: registry}
}

// tenantAwareResolver adapts procedure.Registry to tsqlruntime.ProcedureResolver with tenant support.
type tenantAwareResolver struct {
	registry *procedure.Registry
	tenant   string
}

// Resolve implements tsqlruntime.ProcedureResolver with tenant-aware lookup.
func (r *tenantAwareResolver) Resolve(ctx context.Context, name string, database string) (source string, params []tsqlruntime.ProcedureParam, err error) {
	// Use LookupForTenant to respect tenant overrides
	proc, err := r.registry.LookupForTenant(name, database, r.tenant)
	if err != nil {
		return "", nil, err
	}

	// Convert procedure parameters
	params = make([]tsqlruntime.ProcedureParam, len(proc.Parameters))
	for i, p := range proc.Parameters {
		params[i] = tsqlruntime.ProcedureParam{
			Name:       p.Name,
			SQLType:    p.SQLType,
			IsOutput:   p.Direction == procedure.ParamOut || p.Direction == procedure.ParamInOut,
			HasDefault: p.HasDefault,
			Default:    p.Default,
		}
	}

	return proc.Source, params, nil
}

// newTenantAwareResolver creates a resolver that uses the procedure registry with tenant context.
func newTenantAwareResolver(registry *procedure.Registry, tenant string) tsqlruntime.ProcedureResolver {
	if registry == nil {
		return nil
	}
	return &tenantAwareResolver{registry: registry, tenant: tenant}
}
