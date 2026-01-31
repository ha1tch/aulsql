// Package procedure provides stored procedure management for aul.
package procedure

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
)

// HierarchicalLoader loads procedures from a hierarchical directory structure.
//
// Directory structure:
//
//	procedures/
//	├── _global/                    # Shared across all databases
//	│   └── dbo/
//	│       └── GetServerInfo.sql
//	├── master/                     # Database "master"
//	│   └── dbo/
//	│       └── sp_who.sql
//	└── salesdb/                    # Database "salesdb"
//	    ├── dbo/
//	    │   └── GetCustomer.sql
//	    └── reporting/
//	        └── MonthlySales.sql
//
// Procedures in _global are available to all databases.
// The directory structure determines database.schema.name qualification.
type HierarchicalLoader struct {
	dialect Dialect
	parser  Parser
	logger  *log.Logger

	// Options
	validateSchema bool // Verify declared schema matches directory
}

// HierarchicalLoaderOption configures the loader.
type HierarchicalLoaderOption func(*HierarchicalLoader)

// WithSchemaValidation enables/disables schema validation.
func WithSchemaValidation(enable bool) HierarchicalLoaderOption {
	return func(l *HierarchicalLoader) {
		l.validateSchema = enable
	}
}

// NewHierarchicalLoader creates a new hierarchical procedure loader.
func NewHierarchicalLoader(dialect string, logger *log.Logger, opts ...HierarchicalLoaderOption) *HierarchicalLoader {
	l := &HierarchicalLoader{
		dialect:        Dialect(dialect),
		parser:         NewParser(Dialect(dialect)),
		logger:         logger,
		validateSchema: true, // Default: validate schema
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// LoadResult holds the result of loading procedures.
type LoadResult struct {
	Procedures   []*Procedure
	GlobalProcs  []*Procedure // Procedures from _global
	ByDatabase   map[string][]*Procedure
	ByTenant     map[string][]*Procedure // Procedures by tenant (from _tenant/{tenant}/)
	Errors       []LoadError
	TotalFiles   int
	SuccessCount int
	FailCount    int
}

// LoadError records a loading error with context.
type LoadError struct {
	Path    string
	Error   error
	Message string
}

// LoadDirectory loads all procedures from a hierarchical directory structure.
func (l *HierarchicalLoader) LoadDirectory(root string) (*LoadResult, error) {
	result := &LoadResult{
		Procedures:  make([]*Procedure, 0),
		GlobalProcs: make([]*Procedure, 0),
		ByDatabase:  make(map[string][]*Procedure),
		ByTenant:    make(map[string][]*Procedure),
		Errors:      make([]LoadError, 0),
	}

	// Check root exists
	info, err := os.Stat(root)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
			"procedure directory not found").
			WithOp("HierarchicalLoader.LoadDirectory").
			WithField("path", root).
			Err()
	}
	if !info.IsDir() {
		return nil, aulerrors.Newf(aulerrors.ErrCodeProcLoadError,
			"not a directory: %s", root).
			WithOp("HierarchicalLoader.LoadDirectory").
			Err()
	}

	// List top-level directories (databases)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
			"failed to read directory").
			WithOp("HierarchicalLoader.LoadDirectory").
			WithField("path", root).
			Err()
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dbName := entry.Name()
		dbPath := filepath.Join(root, dbName)

		if dbName == "_global" {
			// Load global procedures
			procs, errs := l.loadDatabase(dbPath, "", true, "")
			result.GlobalProcs = append(result.GlobalProcs, procs...)
			result.Procedures = append(result.Procedures, procs...)
			result.Errors = append(result.Errors, errs...)
			result.SuccessCount += len(procs)
			result.FailCount += len(errs)
		} else if dbName == "_tenant" {
			// Load tenant-specific procedures
			tenantProcs, tenantErrs := l.loadTenantDirectory(dbPath)
			for tenant, procs := range tenantProcs {
				result.ByTenant[tenant] = append(result.ByTenant[tenant], procs...)
				result.Procedures = append(result.Procedures, procs...)
			}
			result.Errors = append(result.Errors, tenantErrs...)
			for _, procs := range tenantProcs {
				result.SuccessCount += len(procs)
			}
			result.FailCount += len(tenantErrs)
		} else if !strings.HasPrefix(dbName, "_") && !strings.HasPrefix(dbName, ".") {
			// Load database procedures (skip hidden/special dirs)
			procs, errs := l.loadDatabase(dbPath, dbName, false, "")
			result.ByDatabase[dbName] = procs
			result.Procedures = append(result.Procedures, procs...)
			result.Errors = append(result.Errors, errs...)
			result.SuccessCount += len(procs)
			result.FailCount += len(errs)
		}
	}

	result.TotalFiles = result.SuccessCount + result.FailCount

	l.logger.Application().Info("hierarchical load complete",
		"root", root,
		"databases", len(result.ByDatabase),
		"tenants", len(result.ByTenant),
		"global_procs", len(result.GlobalProcs),
		"total_procs", len(result.Procedures),
		"errors", len(result.Errors),
	)

	return result, nil
}

// loadTenantDirectory loads procedures from all tenant subdirectories.
func (l *HierarchicalLoader) loadTenantDirectory(tenantRoot string) (map[string][]*Procedure, []LoadError) {
	result := make(map[string][]*Procedure)
	var allErrors []LoadError

	entries, err := os.ReadDir(tenantRoot)
	if err != nil {
		allErrors = append(allErrors, LoadError{
			Path:    tenantRoot,
			Error:   err,
			Message: "failed to read tenant directory",
		})
		return result, allErrors
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		tenantName := entry.Name()
		if strings.HasPrefix(tenantName, ".") {
			continue // Skip hidden
		}

		tenantPath := filepath.Join(tenantRoot, tenantName)

		// Load tenant's procedures (structure mirrors main: database/schema/proc.sql)
		tenantEntries, err := os.ReadDir(tenantPath)
		if err != nil {
			allErrors = append(allErrors, LoadError{
				Path:    tenantPath,
				Error:   err,
				Message: "failed to read tenant subdirectory",
			})
			continue
		}

		for _, dbEntry := range tenantEntries {
			if !dbEntry.IsDir() {
				continue
			}

			dbName := dbEntry.Name()
			if strings.HasPrefix(dbName, "_") || strings.HasPrefix(dbName, ".") {
				continue
			}

			dbPath := filepath.Join(tenantPath, dbName)
			procs, errs := l.loadDatabase(dbPath, dbName, false, tenantName)
			result[tenantName] = append(result[tenantName], procs...)
			allErrors = append(allErrors, errs...)
		}
	}

	return result, allErrors
}

// loadDatabase loads all procedures from a database directory.
func (l *HierarchicalLoader) loadDatabase(dbPath, dbName string, isGlobal bool, tenant string) ([]*Procedure, []LoadError) {
	var procs []*Procedure
	var errs []LoadError

	// List schema directories
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		errs = append(errs, LoadError{
			Path:    dbPath,
			Error:   err,
			Message: "failed to read database directory",
		})
		return procs, errs
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			// Also check for .sql files directly in database dir (assume dbo schema)
			if strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
				proc, err := l.loadFile(filepath.Join(dbPath, entry.Name()), dbName, "dbo", isGlobal, tenant)
				if err != nil {
					errs = append(errs, LoadError{
						Path:    filepath.Join(dbPath, entry.Name()),
						Error:   err,
						Message: "failed to load procedure",
					})
				} else {
					procs = append(procs, proc)
				}
			}
			continue
		}

		schemaName := entry.Name()
		if strings.HasPrefix(schemaName, "_") || strings.HasPrefix(schemaName, ".") {
			continue // Skip special directories
		}

		schemaPath := filepath.Join(dbPath, schemaName)
		schemaProcs, schemaErrs := l.loadSchema(schemaPath, dbName, schemaName, isGlobal, tenant)
		procs = append(procs, schemaProcs...)
		errs = append(errs, schemaErrs...)
	}

	return procs, errs
}

// loadSchema loads all procedures from a schema directory.
func (l *HierarchicalLoader) loadSchema(schemaPath, dbName, schemaName string, isGlobal bool, tenant string) ([]*Procedure, []LoadError) {
	var procs []*Procedure
	var errs []LoadError

	entries, err := os.ReadDir(schemaPath)
	if err != nil {
		errs = append(errs, LoadError{
			Path:    schemaPath,
			Error:   err,
			Message: "failed to read schema directory",
		})
		return procs, errs
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue // Don't recurse deeper
		}

		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
			continue
		}

		filePath := filepath.Join(schemaPath, entry.Name())
		proc, err := l.loadFile(filePath, dbName, schemaName, isGlobal, tenant)
		if err != nil {
			errs = append(errs, LoadError{
				Path:    filePath,
				Error:   err,
				Message: "failed to load procedure",
			})
		} else {
			procs = append(procs, proc)
		}
	}

	return procs, errs
}

// loadFile loads a single procedure file.
func (l *HierarchicalLoader) loadFile(path, dbName, schemaName string, isGlobal bool, tenant string) (*Procedure, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
			"failed to read file").
			WithOp("HierarchicalLoader.loadFile").
			WithField("path", path).
			Err()
	}

	proc, err := l.parser.Parse(string(source))
	if err != nil {
		return nil, aulerrors.Wrap(err, aulerrors.ErrCodeProcParseError,
			"failed to parse procedure").
			WithOp("HierarchicalLoader.loadFile").
			WithField("path", path).
			Err()
	}

	// Set database from directory structure
	proc.Database = dbName
	proc.IsGlobal = isGlobal
	proc.Tenant = tenant

	// Schema validation
	if l.validateSchema && proc.Schema != "" && proc.Schema != schemaName {
		return nil, aulerrors.Newf(aulerrors.ErrCodeProcValidationError,
			"schema mismatch: declared '%s' but located in '%s'", proc.Schema, schemaName).
			WithOp("HierarchicalLoader.loadFile").
			WithField("path", path).
			WithField("declared_schema", proc.Schema).
			WithField("directory_schema", schemaName).
			Err()
	}

	// If no schema declared, inherit from directory
	if proc.Schema == "" {
		proc.Schema = schemaName
	}

	// Update FullName for backward compatibility
	proc.FullName = proc.ShortName()

	proc.SourceFile = path
	proc.LoadedAt = time.Now()

	// Get file modification time
	if info, err := os.Stat(path); err == nil {
		proc.ModifiedAt = info.ModTime()
	}

	l.logger.Application().Debug("procedure file loaded",
		"path", path,
		"procedure", proc.QualifiedName(),
		"database", dbName,
		"schema", schemaName,
		"global", isGlobal,
		"tenant", tenant,
	)

	return proc, nil
}

// LoadFlat loads procedures from a flat directory (backward compatible).
// All procedures are assigned to the specified database and dbo schema.
func (l *HierarchicalLoader) LoadFlat(dir, defaultDB string) ([]*Procedure, error) {
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

		proc, err := l.loadFile(path, defaultDB, "dbo", false, "")
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
			WithOp("HierarchicalLoader.LoadFlat").
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
