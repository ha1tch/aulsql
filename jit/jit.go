// Package jit provides just-in-time compilation for stored procedures.
//
// When a procedure is executed frequently (exceeding the configured threshold),
// the JIT manager transpiles it to Go code using tgpiler, compiles it to a
// plugin, and loads it for optimised execution.
//
// The JIT system supports:
// - Automatic promotion based on execution count
// - Manual compilation via API
// - Hot-swapping of compiled code on source change
// - Fallback to interpreter on compilation failure
// - Proper state machine for compilation lifecycle
package jit

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ha1tch/aul/jit/abi"
	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
)

// OptLevel represents optimisation level for compilation.
type OptLevel int

const (
	OptLevel0 OptLevel = iota // No optimisation
	OptLevel1                 // Basic optimisation
	OptLevel2                 // Full optimisation (default)
	OptLevelS                 // Size optimisation
)

func (o OptLevel) String() string {
	switch o {
	case OptLevel0:
		return "O0"
	case OptLevel1:
		return "O1"
	case OptLevel2:
		return "O2"
	case OptLevelS:
		return "Os"
	default:
		return "unknown"
	}
}

// Config holds JIT manager configuration.
type Config struct {
	// Compilation threshold (executions before JIT)
	Threshold int

	// Output directory for generated code and plugins
	OutputDir string

	// Keep generated Go source files (useful for debugging)
	KeepSource bool

	// Optimisation level
	Optimisation OptLevel

	// Compiler settings
	GoPath    string // Path to go binary
	BuildTags string // Build tags for compilation

	// Async compilation
	MaxConcurrentCompiles int

	// Module resolution for generated code
	AulVersion    string // Version of aul module (e.g., "0.4.9")
	AulModulePath string // Absolute path to aul repo root

	// Retry settings
	MaxRetries    int
	RetryBackoffs []time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	// Try to detect module path
	aulPath, _ := filepath.Abs(".")

	return Config{
		Threshold:             100,
		OutputDir:             "./jit_cache",
		KeepSource:            false,
		Optimisation:          OptLevel2,
		GoPath:                "go",
		MaxConcurrentCompiles: 4,
		AulVersion:            "0.4.9",
		AulModulePath:         aulPath,
		MaxRetries:            5,
		RetryBackoffs: []time.Duration{
			1 * time.Minute,
			5 * time.Minute,
			30 * time.Minute,
			2 * time.Hour,
			24 * time.Hour,
		},
	}
}

// Manager handles JIT compilation and execution.
type Manager struct {
	mu sync.RWMutex

	config Config
	logger *log.Logger

	// Compiled procedures (keyed by qualified name)
	compiled map[string]*CompiledProc

	// Compilation status (keyed by qualified name)
	// This is the authoritative state, not procedure.JITCompiled
	status map[string]*CompileStatus

	// Compilation queue
	compileQueue chan *procedure.Procedure

	// Statistics
	stats Stats

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CompiledProc represents a JIT-compiled procedure.
type CompiledProc struct {
	// Source procedure name and hash
	QualifiedName string
	SourceHash    string

	// Compiled artifacts
	WorkspaceDir string // Directory containing source and plugin
	SourceFile   string // Generated Go source
	PluginFile   string // Compiled plugin (.so)

	// Loaded function (from abi package for type compatibility)
	Func abi.CompiledFunc

	// Metadata
	CompiledAt  time.Time
	CompileTime time.Duration
	Version     int // Incremented on recompilation
}

// Stats holds JIT statistics.
type Stats struct {
	CompiledCount       int
	CompilationTime     time.Duration
	CompilationsTotal   int64
	CompilationErrors   int64
	CacheHits           int64
	CacheMisses         int64
	TotalExecs          int64
	TotalExecTimeNs     int64
	RecompilationsTotal int64
}

// NewManager creates a new JIT manager.
func NewManager(cfg Config, logger *log.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		config:       cfg,
		logger:       logger,
		compiled:     make(map[string]*CompiledProc),
		status:       make(map[string]*CompileStatus),
		compileQueue: make(chan *procedure.Procedure, 100),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Ensure output directory exists
	os.MkdirAll(cfg.OutputDir, 0755)

	// Start compilation workers (no separate semaphore needed - Fix 7)
	for i := 0; i < cfg.MaxConcurrentCompiles; i++ {
		m.wg.Add(1)
		go m.compileWorker()
	}

	logger.System().Debug("JIT manager initialised",
		"workers", cfg.MaxConcurrentCompiles,
		"output_dir", cfg.OutputDir,
	)

	return m
}

// IsReady checks if a procedure is JIT-compiled and ready for execution.
// This is the authoritative check - Fix 1.
func (m *Manager) IsReady(name string, sourceHash string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := m.status[name]
	if status == nil || !status.IsExecutable(sourceHash) {
		return false
	}

	compiled := m.compiled[name]
	return compiled != nil && compiled.Func != nil && compiled.SourceHash == sourceHash
}

// MaybeEnqueue considers a procedure for JIT compilation based on execution count.
// This replaces the old triggerJIT which set state too early - Fix 2.
func (m *Manager) MaybeEnqueue(proc *procedure.Procedure) {
	// Check execution count threshold
	if proc.ExecCount < int64(m.config.Threshold) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := proc.QualifiedName()
	status := m.status[name]

	// Check if we can enqueue
	if status != nil {
		switch status.State {
		case StateQueued, StateCompiling:
			return // Already in progress

		case StateReady:
			if status.SourceHash == proc.SourceHash {
				return // Already compiled with current source
			}
			// Source changed, need recompile
			m.logger.Execution().Debug("source changed, triggering recompile",
				"procedure", name,
			)

		case StateFailed:
			// Check backoff
			backoff := m.retryBackoff(status.RetryCount)
			if time.Since(status.CompletedAt) < backoff {
				return // Still in backoff period
			}
		}
	}

	// Create or update status
	retryCount := 0
	if status != nil && status.State == StateFailed {
		retryCount = status.RetryCount
	}

	m.status[name] = &CompileStatus{
		State:      StateQueued,
		SourceHash: proc.SourceHash,
		QueuedAt:   time.Now(),
		RetryCount: retryCount,
	}

	// Enqueue (non-blocking)
	select {
	case m.compileQueue <- proc:
		m.logger.Execution().Debug("procedure queued for JIT compilation",
			"procedure", name,
			"exec_count", proc.ExecCount,
		)
	default:
		// Queue full, reset state
		m.status[name].State = StateNone
		m.logger.Execution().Warn("JIT queue full, skipping compilation",
			"procedure", name,
		)
	}
}

// Compile triggers immediate compilation of a procedure (queued).
func (m *Manager) Compile(proc *procedure.Procedure) error {
	if proc == nil {
		return aulerrors.New(aulerrors.ErrCodeProcInvalidParam, "nil procedure").
			WithOp("JIT.Compile").
			Err()
	}

	m.mu.Lock()
	name := proc.QualifiedName()
	status := m.status[name]

	// Check if already queued/compiling
	if status != nil && (status.State == StateQueued || status.State == StateCompiling) {
		m.mu.Unlock()
		return nil
	}

	m.status[name] = &CompileStatus{
		State:      StateQueued,
		SourceHash: proc.SourceHash,
		QueuedAt:   time.Now(),
	}
	m.mu.Unlock()

	// Queue for compilation
	select {
	case m.compileQueue <- proc:
		return nil
	default:
		m.mu.Lock()
		m.status[name].State = StateNone
		m.mu.Unlock()
		return aulerrors.New(aulerrors.ErrCodeJITQueueFull, "compilation queue full").
			WithOp("JIT.Compile").
			WithField("procedure", name).
			Err()
	}
}

// CompileSync synchronously compiles a procedure.
func (m *Manager) CompileSync(proc *procedure.Procedure) error {
	return m.doCompile(proc)
}

// Execute runs a JIT-compiled procedure.
func (m *Manager) Execute(ctx context.Context, proc *procedure.Procedure, params map[string]interface{}, storage abi.StorageBackend) (*abi.ExecResult, error) {
	name := proc.QualifiedName()

	m.mu.RLock()
	compiled := m.compiled[name]
	m.mu.RUnlock()

	if compiled == nil || compiled.Func == nil {
		atomic.AddInt64(&m.stats.CacheMisses, 1)
		return nil, aulerrors.Newf(aulerrors.ErrCodeJITNotCompiled,
			"procedure not JIT-compiled: %s", name).
			WithOp("JIT.Execute").
			Err()
	}

	// Verify source hash matches
	if compiled.SourceHash != proc.SourceHash {
		atomic.AddInt64(&m.stats.CacheMisses, 1)
		return nil, aulerrors.Newf(aulerrors.ErrCodeJITNotCompiled,
			"JIT cache stale for: %s", name).
			WithOp("JIT.Execute").
			Err()
	}

	atomic.AddInt64(&m.stats.CacheHits, 1)
	atomic.AddInt64(&m.stats.TotalExecs, 1)

	start := time.Now()
	result, err := compiled.Func(ctx, params, storage)
	elapsed := time.Since(start)
	atomic.AddInt64(&m.stats.TotalExecTimeNs, elapsed.Nanoseconds())

	m.logger.Performance().Debug("JIT execution completed",
		"procedure", name,
		"duration_ns", elapsed.Nanoseconds(),
	)

	return result, err
}

// GetStatus returns the compilation status for a procedure.
func (m *Manager) GetStatus(name string) *CompileStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status[name]
}

// Invalidate removes a compiled procedure (e.g., when source changes).
func (m *Manager) Invalidate(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if compiled, ok := m.compiled[name]; ok {
		// Clean up workspace
		if compiled.WorkspaceDir != "" && !m.config.KeepSource {
			os.RemoveAll(compiled.WorkspaceDir)
		}
		delete(m.compiled, name)
	}

	// Reset status
	delete(m.status, name)
}

// Stats returns JIT statistics.
func (m *Manager) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := m.stats
	stats.CompiledCount = len(m.compiled)
	return stats
}

// Close shuts down the JIT manager.
func (m *Manager) Close() error {
	m.cancel()
	close(m.compileQueue)
	m.wg.Wait()
	return nil
}

// compileWorker processes the compilation queue.
func (m *Manager) compileWorker() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case proc, ok := <-m.compileQueue:
			if !ok {
				return
			}
			if err := m.doCompile(proc); err != nil {
				atomic.AddInt64(&m.stats.CompilationErrors, 1)
				m.logger.Execution().Error("JIT compilation failed", err,
					"procedure", proc.Name,
				)
			}
		}
	}
}

// doCompile performs the actual compilation.
func (m *Manager) doCompile(proc *procedure.Procedure) error {
	name := proc.QualifiedName()

	// Update status to compiling
	m.mu.Lock()
	status := m.status[name]
	if status == nil {
		status = &CompileStatus{SourceHash: proc.SourceHash}
		m.status[name] = status
	}
	status.State = StateCompiling
	status.StartedAt = time.Now()
	isRecompile := m.compiled[name] != nil
	m.mu.Unlock()

	m.logger.Execution().Info("starting JIT compilation",
		"procedure", name,
		"recompile", isRecompile,
	)

	start := time.Now()

	// Prepare workspace with module context - Fix 5
	workDir, err := m.prepareWorkspace(proc)
	if err != nil {
		m.markFailed(name, err)
		return aulerrors.Wrap(err, aulerrors.ErrCodeJITTranspile, "failed to prepare workspace").
			WithOp("JIT.doCompile").
			WithField("procedure", name).
			Err()
	}

	// Generate Go source using tgpiler
	sourceFile, err := m.generateSource(workDir, proc)
	if err != nil {
		m.markFailed(name, err)
		return aulerrors.Wrap(err, aulerrors.ErrCodeJITTranspile, "failed to generate source").
			WithOp("JIT.doCompile").
			WithField("procedure", name).
			Err()
	}

	// Compile to plugin
	pluginFile, err := m.compilePlugin(workDir, sourceFile, proc)
	if err != nil {
		m.markFailed(name, err)
		return aulerrors.Wrap(err, aulerrors.ErrCodeJITCompile, "failed to compile plugin").
			WithOp("JIT.doCompile").
			WithField("procedure", name).
			WithField("source_file", sourceFile).
			Err()
	}

	// Load plugin - Fix 3 (using abi types)
	fn, err := m.loadPlugin(pluginFile)
	if err != nil {
		m.markFailed(name, err)
		return aulerrors.Wrap(err, aulerrors.ErrCodeJITLoad, "failed to load plugin").
			WithOp("JIT.doCompile").
			WithField("procedure", name).
			WithField("plugin_file", pluginFile).
			Err()
	}

	compileTime := time.Since(start)

	// Store compiled procedure
	m.mu.Lock()
	existing := m.compiled[name]
	version := 1
	if existing != nil {
		version = existing.Version + 1
		atomic.AddInt64(&m.stats.RecompilationsTotal, 1)
	}

	m.compiled[name] = &CompiledProc{
		QualifiedName: name,
		SourceHash:    proc.SourceHash,
		WorkspaceDir:  workDir,
		SourceFile:    sourceFile,
		PluginFile:    pluginFile,
		Func:          fn,
		CompiledAt:    time.Now(),
		CompileTime:   compileTime,
		Version:       version,
	}

	m.status[name] = &CompileStatus{
		State:       StateReady,
		SourceHash:  proc.SourceHash,
		CompletedAt: time.Now(),
	}

	m.stats.CompilationTime += compileTime
	atomic.AddInt64(&m.stats.CompilationsTotal, 1)
	m.mu.Unlock()

	// Clean up old workspace if not keeping source
	if existing != nil && existing.WorkspaceDir != "" && !m.config.KeepSource {
		os.RemoveAll(existing.WorkspaceDir)
	}

	m.logger.Execution().Info("JIT compilation completed",
		"procedure", name,
		"version", version,
		"duration_ms", compileTime.Milliseconds(),
	)

	return nil
}

// markFailed updates status to failed with error information.
func (m *Manager) markFailed(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := m.status[name]
	if status == nil {
		status = &CompileStatus{}
		m.status[name] = status
	}

	status.State = StateFailed
	status.CompletedAt = time.Now()
	status.Error = err.Error()
	status.RetryCount++
}

// retryBackoff returns the backoff duration for a given retry count.
func (m *Manager) retryBackoff(retryCount int) time.Duration {
	if retryCount >= len(m.config.RetryBackoffs) {
		return m.config.RetryBackoffs[len(m.config.RetryBackoffs)-1]
	}
	return m.config.RetryBackoffs[retryCount]
}

// prepareWorkspace creates a compilation workspace with proper go.mod - Fix 5.
func (m *Manager) prepareWorkspace(proc *procedure.Procedure) (string, error) {
	// Create deterministic directory: jit_cache/<safe_name>_<hash>/
	dirName := WorkspaceDirName(proc.QualifiedName(), proc.SourceHash)
	workDir := filepath.Join(m.config.OutputDir, dirName)

	if err := os.MkdirAll(workDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create workspace: %w", err)
	}

	// Create go.mod with replace directive for local development
	goMod := fmt.Sprintf(`module jitproc

go 1.22

require github.com/ha1tch/aul %s

replace github.com/ha1tch/aul => %s
`, m.config.AulVersion, m.config.AulModulePath)

	if err := os.WriteFile(filepath.Join(workDir, "go.mod"), []byte(goMod), 0644); err != nil {
		return "", fmt.Errorf("failed to write go.mod: %w", err)
	}

	return workDir, nil
}

// generateSource generates Go source code for a procedure.
func (m *Manager) generateSource(workDir string, proc *procedure.Procedure) (string, error) {
	// TODO: Integrate with tgpiler transpiler
	//
	// code, err := transpiler.TranspileWithDML(proc.Source, "main", transpiler.DMLConfig{
	//     Backend: transpiler.BackendSQL,
	//     SQLDialect: "sqlite", // or from config
	// })
	// if err != nil {
	//     return "", err
	// }

	// Safe function name - Fix 4
	funcName := SafeGoName(proc.QualifiedName())

	// For now, generate a stub that imports abi package correctly - Fix 3
	code := fmt.Sprintf(`// Code generated by aul JIT compiler. DO NOT EDIT.
// Source: %s
// Hash: %s
package main

import (
	"context"

	"github.com/ha1tch/aul/jit/abi"
)

// %s is the JIT-compiled procedure.
func %s(ctx context.Context, params map[string]interface{}, storage abi.StorageBackend) (*abi.ExecResult, error) {
	// TODO: Generated code from tgpiler transpilation
	// Original T-SQL:
	// %s

	return &abi.ExecResult{
		RowsAffected: 0,
		ResultSets:   []abi.ResultSet{},
		Warnings:     []string{"JIT stub - tgpiler integration pending"},
	}, nil
}

// Execute is the exported symbol for plugin loading.
// Type must match abi.CompiledFunc exactly.
var Execute abi.CompiledFunc = %s
`, proc.QualifiedName(), proc.SourceHash, funcName, funcName, proc.Source, funcName)

	// Write to file
	filename := filepath.Join(workDir, "proc.go")
	if err := os.WriteFile(filename, []byte(code), 0644); err != nil {
		return "", err
	}

	return filename, nil
}

// compilePlugin compiles Go source to a plugin.
func (m *Manager) compilePlugin(workDir string, sourceFile string, proc *procedure.Procedure) (string, error) {
	pluginFile := filepath.Join(workDir, "proc.so")

	// Build command
	args := []string{"build", "-buildmode=plugin"}

	// Add optimisation flags
	switch m.config.Optimisation {
	case OptLevel0:
		args = append(args, "-gcflags=-N -l")
	case OptLevelS:
		args = append(args, "-ldflags=-s -w")
	}

	// Add build tags
	if m.config.BuildTags != "" {
		args = append(args, "-tags", m.config.BuildTags)
	}

	args = append(args, "-o", pluginFile, sourceFile)

	// Run compiler in workspace directory (has go.mod) - Fix 5
	cmd := exec.Command(m.config.GoPath, args...)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"GOPROXY=https://proxy.golang.org,direct",
		"GOFLAGS=-mod=mod",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Save error output for debugging
		errLogFile := filepath.Join(workDir, "compile_error.log")
		os.WriteFile(errLogFile, output, 0644)

		return "", aulerrors.Wrapf(err, aulerrors.ErrCodeJITCompile,
			"Go compiler failed").
			WithOp("JIT.compilePlugin").
			WithField("procedure", proc.QualifiedName()).
			WithField("output", string(output)).
			WithField("error_log", errLogFile).
			Err()
	}

	return pluginFile, nil
}

// loadPlugin loads a compiled plugin and extracts the Execute function - Fix 3.
func (m *Manager) loadPlugin(pluginFile string) (abi.CompiledFunc, error) {
	p, err := plugin.Open(pluginFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin: %w", err)
	}

	sym, err := p.Lookup("Execute")
	if err != nil {
		return nil, fmt.Errorf("failed to find Execute symbol: %w", err)
	}

	// The symbol is a pointer to the variable (var Execute abi.CompiledFunc = ...)
	fnPtr, ok := sym.(*abi.CompiledFunc)
	if !ok {
		return nil, fmt.Errorf("Execute symbol has wrong type: %T (expected *abi.CompiledFunc)", sym)
	}

	return *fnPtr, nil
}
