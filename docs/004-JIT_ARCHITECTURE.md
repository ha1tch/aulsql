# JIT Architecture Fixes

**Version:** 0.4.9  
**Status:** Implementation plan  
**Last updated:** January 2026  
**Based on:** External code review feedback

---

## Summary of Issues

The reviewer identified 7 critical issues that prevent JIT from ever executing real compiled code:

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | Runtime gating makes JIT unreachable | Blocker | ✓ Fixed |
| 2 | `triggerJIT()` marks compiled too early | Blocker | ✓ Fixed |
| 3 | Generated types don't match host types | Blocker | ✓ Fixed |
| 4 | Procedure names not safe Go identifiers | Bug | ✓ Fixed |
| 5 | `go build` has no module context | Blocker | ✓ Fixed |
| 6 | Promotion can spam compiles (race) | Bug | ✓ Fixed |
| 7 | Redundant worker + semaphore | Minor | ✓ Fixed |

**All fixes have been implemented.** See the updated files:
- `jit/abi/abi.go` — Shared ABI types (Fix 3)
- `jit/state.go` — State machine for compilation (Fix 2)
- `jit/naming.go` — Safe Go identifier generation (Fix 4)
- `jit/jit.go` — All fixes integrated

---

## Fix 1: Runtime Gating Logic

### Problem

```go
// runtime/runtime.go line 153
if proc.JITCompiled && proc.JITCode != nil {
    return r.executeJIT(ctx, proc, execCtx)
}
```

`proc.JITCode` is never set anywhere. JIT path is unreachable.

### Solution

Ask the JIT manager, don't rely on procedure state:

```go
// runtime/runtime.go - Execute method
func (r *Runtime) Execute(ctx context.Context, proc *procedure.Procedure, execCtx *ExecContext) (*ExecResult, error) {
    // ... existing setup ...

    // Choose execution strategy
    // 1. Try JIT if available
    if r.jitManager != nil && r.jitManager.IsReady(proc.QualifiedName(), proc.SourceHash) {
        result, err := r.executeJIT(ctx, proc, execCtx)
        if err == nil {
            return result, nil
        }
        // JIT failed, fall through to interpreted
        r.logger.Execution().Warn("JIT execution failed, falling back to interpreted",
            "procedure", proc.QualifiedName(),
            "error", err,
        )
    }

    // 2. Interpreted execution
    result, err := r.executeInterpreted(ctx, proc, execCtx)
    if err != nil {
        return nil, err
    }

    // 3. Consider JIT promotion (async)
    if r.jitManager != nil {
        r.jitManager.MaybeEnqueue(proc)
    }

    return result, nil
}
```

**New JIT manager method:**

```go
// jit/jit.go
func (m *Manager) IsReady(name string, sourceHash string) bool {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    compiled, ok := m.compiled[name]
    if !ok {
        return false
    }
    // Must match current source hash
    return compiled.SourceHash == sourceHash && compiled.Func != nil
}
```

---

## Fix 2: State Machine for Compilation

### Problem

`triggerJIT()` sets `proc.JITCompiled = true` before compilation completes. Race conditions and incorrect state.

### Solution

Remove state from `procedure.Procedure`, keep it inside JIT manager with proper state machine:

```go
// jit/state.go (new file)
package jit

type CompileState int

const (
    StateNone CompileState = iota
    StateQueued
    StateCompiling
    StateReady
    StateFailed
)

func (s CompileState) String() string {
    switch s {
    case StateNone:
        return "none"
    case StateQueued:
        return "queued"
    case StateCompiling:
        return "compiling"
    case StateReady:
        return "ready"
    case StateFailed:
        return "failed"
    default:
        return "unknown"
    }
}

type CompileStatus struct {
    State       CompileState
    SourceHash  string
    QueuedAt    time.Time
    StartedAt   time.Time
    CompletedAt time.Time
    Error       string
    RetryCount  int
}
```

**Updated manager:**

```go
// jit/jit.go
type Manager struct {
    // ... existing fields ...
    
    // State tracking (replaces relying on proc.JITCompiled)
    status map[string]*CompileStatus  // keyed by qualified name
}

func (m *Manager) MaybeEnqueue(proc *procedure.Procedure) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    name := proc.QualifiedName()
    status := m.status[name]
    
    // Check execution count threshold
    if proc.ExecCount < int64(m.config.Threshold) {
        return
    }
    
    // Already queued, compiling, or ready with same hash?
    if status != nil {
        switch status.State {
        case StateQueued, StateCompiling:
            return // Already in progress
        case StateReady:
            if status.SourceHash == proc.SourceHash {
                return // Already compiled with current source
            }
            // Source changed, need recompile
        case StateFailed:
            // Check backoff
            if time.Since(status.CompletedAt) < m.retryBackoff(status.RetryCount) {
                return
            }
        }
    }
    
    // Create or update status
    m.status[name] = &CompileStatus{
        State:      StateQueued,
        SourceHash: proc.SourceHash,
        QueuedAt:   time.Now(),
    }
    
    // Enqueue (non-blocking)
    select {
    case m.compileQueue <- proc:
    default:
        // Queue full, reset state
        m.status[name].State = StateNone
    }
}
```

**Remove from procedure.Procedure:**

```go
// procedure/procedure.go - remove these fields:
// JITCompiled   bool
// JITCompiledAt time.Time
// JITCode       interface{}
```

---

## Fix 3: Shared ABI Package

### Problem

Generated code defines its own `StorageBackend`, `ExecResult`, etc. Types don't match host types in Go plugin system.

### Solution

Create `jit/abi` package with shared types:

```go
// jit/abi/abi.go
package abi

import "context"

// StorageBackend is the interface for database access.
// This type is shared between the host and generated plugins.
type StorageBackend interface {
    Query(ctx context.Context, sql string, args ...interface{}) ([]ResultSet, error)
    QueryRow(ctx context.Context, sql string, args ...interface{}) ([]interface{}, error)
    Exec(ctx context.Context, sql string, args ...interface{}) (int64, error)
}

// ExecResult holds the result of a procedure execution.
type ExecResult struct {
    RowsAffected int64
    ResultSets   []ResultSet
    ReturnValue  interface{}
    OutputParams map[string]interface{}
    Warnings     []string
}

// ResultSet represents a tabular result.
type ResultSet struct {
    Columns []ColumnInfo
    Rows    [][]interface{}
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
    Name     string
    Type     string
    Nullable bool
    Length   int
    Ordinal  int
}

// CompiledFunc is the signature for compiled procedure functions.
// Generated plugins must export a variable of this type named "Execute".
type CompiledFunc func(ctx context.Context, params map[string]interface{}, storage StorageBackend) (*ExecResult, error)
```

**Generated code now imports the ABI:**

```go
// Generated plugin code
package main

import (
    "context"
    
    "github.com/ha1tch/aul/jit/abi"
)

func execute(ctx context.Context, params map[string]interface{}, storage abi.StorageBackend) (*abi.ExecResult, error) {
    // ... generated procedure logic ...
    return &abi.ExecResult{
        RowsAffected: 0,
        ResultSets:   []abi.ResultSet{},
    }, nil
}

// Export for plugin loading
var Execute abi.CompiledFunc = execute
```

**Updated plugin loading:**

```go
// jit/jit.go
import "plugin"

func (m *Manager) loadPlugin(pluginFile string, proc *procedure.Procedure) (abi.CompiledFunc, error) {
    p, err := plugin.Open(pluginFile)
    if err != nil {
        return nil, fmt.Errorf("failed to open plugin: %w", err)
    }
    
    sym, err := p.Lookup("Execute")
    if err != nil {
        return nil, fmt.Errorf("failed to find Execute symbol: %w", err)
    }
    
    // The symbol is a pointer to the variable
    fnPtr, ok := sym.(*abi.CompiledFunc)
    if !ok {
        return nil, fmt.Errorf("Execute symbol has wrong type: %T", sym)
    }
    
    return *fnPtr, nil
}
```

---

## Fix 4: Safe Go Identifiers

### Problem

Procedure names like `dbo.GetCustomer` or `[My Proc]` are not valid Go identifiers.

### Solution

Create deterministic safe identifier mapping:

```go
// jit/naming.go (new file)
package jit

import (
    "crypto/sha256"
    "encoding/hex"
    "regexp"
    "strings"
)

var unsafeChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// SafeGoName creates a valid Go identifier from a procedure name.
func SafeGoName(qualifiedName string) string {
    // Replace unsafe characters with underscores
    safe := unsafeChars.ReplaceAllString(qualifiedName, "_")
    
    // Ensure doesn't start with digit
    if len(safe) > 0 && safe[0] >= '0' && safe[0] <= '9' {
        safe = "_" + safe
    }
    
    // Add short hash for uniqueness
    hash := sha256.Sum256([]byte(qualifiedName))
    shortHash := hex.EncodeToString(hash[:4])
    
    return "Proc_" + safe + "_" + shortHash
}

// Example:
// "salesdb.dbo.GetCustomer" -> "Proc_salesdb_dbo_GetCustomer_a1b2c3d4"
// "[My Weird Proc!]" -> "Proc__My_Weird_Proc__e5f6g7h8"
```

---

## Fix 5: Build Workspace with Module Context

### Problem

`go build` is executed in OutputDir with no module context. Imports fail.

### Solution

Create a temp module per compile with proper `go.mod`:

```go
// jit/jit.go

func (m *Manager) prepareWorkspace(proc *procedure.Procedure) (workDir string, err error) {
    // Create deterministic directory: jit_cache/<safe_name>/<hash>/
    safeName := SafeGoName(proc.QualifiedName())
    workDir = filepath.Join(m.config.OutputDir, safeName, proc.SourceHash[:8])
    
    if err := os.MkdirAll(workDir, 0755); err != nil {
        return "", err
    }
    
    // Create go.mod
    goMod := fmt.Sprintf(`module jitproc

go 1.22

require github.com/ha1tch/aul v%s

replace github.com/ha1tch/aul => %s
`, m.config.AulVersion, m.config.AulModulePath)
    
    if err := os.WriteFile(filepath.Join(workDir, "go.mod"), []byte(goMod), 0644); err != nil {
        return "", err
    }
    
    return workDir, nil
}

func (m *Manager) compilePlugin(workDir string, sourceFile string, proc *procedure.Procedure) (string, error) {
    pluginFile := filepath.Join(workDir, "proc.so")
    
    args := []string{"build", "-buildmode=plugin", "-o", pluginFile, sourceFile}
    
    cmd := exec.Command(m.config.GoPath, args...)
    cmd.Dir = workDir  // Now has module context
    cmd.Env = append(os.Environ(),
        "GOPROXY="+m.config.GoProxy,
        "GOFLAGS=-mod=mod",
    )
    
    output, err := cmd.CombinedOutput()
    if err != nil {
        // Persist output for debugging
        os.WriteFile(filepath.Join(workDir, "compile_error.log"), output, 0644)
        return "", fmt.Errorf("compilation failed: %s", string(output))
    }
    
    return pluginFile, nil
}
```

**Updated config:**

```go
type Config struct {
    // ... existing fields ...
    
    // Module resolution
    AulVersion    string // e.g., "0.4.9"
    AulModulePath string // Absolute path to aul repo root
    GoProxy       string // GOPROXY setting
}

func DefaultConfig() Config {
    // Auto-detect module path
    aulPath, _ := filepath.Abs(".")
    
    return Config{
        // ... existing defaults ...
        AulVersion:    "0.4.9",
        AulModulePath: aulPath,
        GoProxy:       "https://proxy.golang.org,direct",
    }
}
```

---

## Fix 6: Prevent Duplicate Compiles

### Problem

Multiple requests could queue the same procedure for compilation.

### Solution

Already addressed by Fix 2's state machine. The `MaybeEnqueue` method checks state before queueing:

```go
func (m *Manager) MaybeEnqueue(proc *procedure.Procedure) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // ... threshold check ...
    
    status := m.status[name]
    if status != nil {
        switch status.State {
        case StateQueued, StateCompiling:
            return // Already in progress - don't duplicate
        // ...
        }
    }
    
    // Only one enqueue succeeds
    m.status[name] = &CompileStatus{State: StateQueued, ...}
    // ...
}
```

---

## Fix 7: Remove Redundant Semaphore

### Problem

Both N workers and semaphore of size N. Redundant.

### Solution

Keep workers, remove semaphore:

```go
// jit/jit.go

type Manager struct {
    // Remove: compileSem chan struct{}
    
    // Keep workers
    compileQueue chan *procedure.Procedure
}

func (m *Manager) doCompile(proc *procedure.Procedure) error {
    // Remove semaphore acquisition:
    // m.compileSem <- struct{}{}
    // defer func() { <-m.compileSem }()
    
    // Workers already limit concurrency
    // ...
}
```

---

## Implementation Order

| Order | Fix | Files | Effort |
|-------|-----|-------|--------|
| 1 | Create ABI package | `jit/abi/abi.go` (new) | Low |
| 2 | Safe Go identifiers | `jit/naming.go` (new) | Low |
| 3 | State machine | `jit/state.go` (new), `jit/jit.go` | Medium |
| 4 | Runtime gating | `runtime/runtime.go` | Low |
| 5 | Build workspace | `jit/jit.go` | Medium |
| 6 | Plugin loading | `jit/jit.go` | Low |
| 7 | Remove semaphore | `jit/jit.go` | Trivial |
| 8 | Update code generator | `jit/jit.go` | Medium |

**Total estimated effort:** 1-2 days

---

## Testing Plan

### Unit Tests

```go
// jit/naming_test.go
func TestSafeGoName(t *testing.T) {
    tests := []struct {
        input    string
        wantSafe bool
    }{
        {"GetCustomer", true},
        {"salesdb.dbo.GetCustomer", true},
        {"[My Weird Proc!]", true},
        {"123numeric", true},  // Should prefix with _
    }
    // ...
}

// jit/state_test.go
func TestStateTransitions(t *testing.T) {
    // None -> Queued -> Compiling -> Ready
    // None -> Queued -> Compiling -> Failed
    // Ready -> Queued (on source change)
}
```

### Integration Tests

```go
// jit/jit_test.go
func TestJITCompileAndExecute(t *testing.T) {
    // Create simple procedure
    // Compile it
    // Execute via JIT
    // Verify results match interpreted
}

func TestJITSourceChange(t *testing.T) {
    // Compile procedure
    // Change source
    // Verify recompile triggered
    // Verify new version executes
}
```

### End-to-End Test

```go
func TestJITPromotion(t *testing.T) {
    // Execute procedure 99 times (below threshold)
    // Verify still interpreted
    // Execute 100th time
    // Verify JIT compilation triggered
    // Execute again
    // Verify JIT path used
}
```

---

## Rollout Plan

1. **Phase 1:** Implement fixes 1-3 (ABI, naming, state machine)
2. **Phase 2:** Implement fixes 4-6 (gating, workspace, loading)
3. **Phase 3:** Clean up (fix 7) and testing
4. **Phase 4:** Integrate with tgpiler for real code generation

After these fixes, the JIT pipeline will be architecturally sound and ready for tgpiler integration.
