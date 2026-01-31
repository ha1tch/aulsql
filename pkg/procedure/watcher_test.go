package procedure

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ha1tch/aul/pkg/log"
)

func TestWatcher_DetectsNewFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database/schema structure
	schemaDir := filepath.Join(tmpDir, "testdb", "dbo")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatalf("failed to create schema dir: %v", err)
	}

	// Setup
	logger := log.New(log.Config{DefaultLevel: log.LevelError})
	registry := NewRegistry()

	var reloadMu sync.Mutex
	var reloadedProcs []string
	var reloadEvents []string

	watcher, err := NewWatcher(tmpDir, "tsql", registry, logger,
		WithDebounceDelay(50*time.Millisecond),
		WithOnReload(func(proc *Procedure, event string) {
			reloadMu.Lock()
			reloadedProcs = append(reloadedProcs, proc.QualifiedName())
			reloadEvents = append(reloadEvents, event)
			reloadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to set up
	time.Sleep(100 * time.Millisecond)

	// Create a new procedure file
	procContent := `
CREATE PROCEDURE dbo.NewProc
AS
BEGIN
    SELECT 1 AS Value
END
`
	procPath := filepath.Join(schemaDir, "NewProc.sql")
	if err := os.WriteFile(procPath, []byte(procContent), 0644); err != nil {
		t.Fatalf("failed to write procedure file: %v", err)
	}

	// Wait for debounce + processing
	time.Sleep(200 * time.Millisecond)

	// Verify procedure was registered
	proc, err := registry.Lookup("testdb.dbo.NewProc")
	if err != nil {
		t.Fatalf("procedure not found in registry: %v", err)
	}
	if proc.Name != "NewProc" {
		t.Errorf("expected name 'NewProc', got '%s'", proc.Name)
	}

	// Verify callback was called
	reloadMu.Lock()
	if len(reloadedProcs) != 1 {
		t.Errorf("expected 1 reload callback, got %d", len(reloadedProcs))
	}
	if len(reloadEvents) > 0 && reloadEvents[0] != "created" {
		t.Errorf("expected event 'created', got '%s'", reloadEvents[0])
	}
	reloadMu.Unlock()
}

func TestWatcher_DetectsModifiedFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database/schema structure with initial procedure
	schemaDir := filepath.Join(tmpDir, "testdb", "dbo")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatalf("failed to create schema dir: %v", err)
	}

	procPath := filepath.Join(schemaDir, "ModifyMe.sql")
	initialContent := `
CREATE PROCEDURE dbo.ModifyMe
AS
BEGIN
    SELECT 'initial' AS Value
END
`
	if err := os.WriteFile(procPath, []byte(initialContent), 0644); err != nil {
		t.Fatalf("failed to write initial procedure: %v", err)
	}

	// Setup
	logger := log.New(log.Config{DefaultLevel: log.LevelError})
	registry := NewRegistry()

	// Load initial procedures
	loader := NewHierarchicalLoader("tsql", logger)
	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to load procedures: %v", err)
	}
	for _, proc := range result.Procedures {
		registry.Register(proc)
	}

	var reloadMu sync.Mutex
	var reloadEvents []string

	watcher, err := NewWatcher(tmpDir, "tsql", registry, logger,
		WithDebounceDelay(50*time.Millisecond),
		WithOnReload(func(proc *Procedure, event string) {
			reloadMu.Lock()
			reloadEvents = append(reloadEvents, event)
			reloadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to set up
	time.Sleep(100 * time.Millisecond)

	// Modify the procedure
	modifiedContent := `
CREATE PROCEDURE dbo.ModifyMe
AS
BEGIN
    SELECT 'modified' AS Value
END
`
	if err := os.WriteFile(procPath, []byte(modifiedContent), 0644); err != nil {
		t.Fatalf("failed to write modified procedure: %v", err)
	}

	// Wait for debounce + processing
	time.Sleep(200 * time.Millisecond)

	// Verify procedure was updated
	proc, err := registry.Lookup("testdb.dbo.ModifyMe")
	if err != nil {
		t.Fatalf("procedure not found: %v", err)
	}

	// Source should contain 'modified'
	if proc.Source != modifiedContent {
		t.Errorf("source not updated")
	}

	// Verify callback indicated modification
	reloadMu.Lock()
	if len(reloadEvents) != 1 {
		t.Errorf("expected 1 reload event, got %d", len(reloadEvents))
	} else if reloadEvents[0] != "modified" {
		t.Errorf("expected event 'modified', got '%s'", reloadEvents[0])
	}
	reloadMu.Unlock()
}

func TestWatcher_DetectsDeletedFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database/schema structure with initial procedure
	schemaDir := filepath.Join(tmpDir, "testdb", "dbo")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatalf("failed to create schema dir: %v", err)
	}

	procPath := filepath.Join(schemaDir, "DeleteMe.sql")
	content := `
CREATE PROCEDURE dbo.DeleteMe
AS
BEGIN
    SELECT 1 AS Value
END
`
	if err := os.WriteFile(procPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write procedure: %v", err)
	}

	// Setup
	logger := log.New(log.Config{DefaultLevel: log.LevelError})
	registry := NewRegistry()

	// Load initial procedures
	loader := NewHierarchicalLoader("tsql", logger)
	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to load procedures: %v", err)
	}
	for _, proc := range result.Procedures {
		registry.Register(proc)
	}

	// Verify procedure exists
	if _, err := registry.Lookup("testdb.dbo.DeleteMe"); err != nil {
		t.Fatalf("procedure should exist before deletion: %v", err)
	}

	var reloadMu sync.Mutex
	var reloadEvents []string

	watcher, err := NewWatcher(tmpDir, "tsql", registry, logger,
		WithDebounceDelay(50*time.Millisecond),
		WithOnReload(func(proc *Procedure, event string) {
			reloadMu.Lock()
			reloadEvents = append(reloadEvents, event)
			reloadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to set up
	time.Sleep(100 * time.Millisecond)

	// Delete the procedure file
	if err := os.Remove(procPath); err != nil {
		t.Fatalf("failed to delete procedure file: %v", err)
	}

	// Wait for debounce + processing
	time.Sleep(200 * time.Millisecond)

	// Verify procedure was removed from registry
	_, err = registry.Lookup("testdb.dbo.DeleteMe")
	if err == nil {
		t.Error("procedure should have been removed from registry")
	}

	// Verify callback indicated removal
	reloadMu.Lock()
	if len(reloadEvents) != 1 {
		t.Errorf("expected 1 reload event, got %d", len(reloadEvents))
	} else if reloadEvents[0] != "removed" {
		t.Errorf("expected event 'removed', got '%s'", reloadEvents[0])
	}
	reloadMu.Unlock()
}

func TestWatcher_IgnoresUnchangedFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database/schema structure with initial procedure
	schemaDir := filepath.Join(tmpDir, "testdb", "dbo")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatalf("failed to create schema dir: %v", err)
	}

	procPath := filepath.Join(schemaDir, "Unchanged.sql")
	content := `
CREATE PROCEDURE dbo.Unchanged
AS
BEGIN
    SELECT 1 AS Value
END
`
	if err := os.WriteFile(procPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write procedure: %v", err)
	}

	// Setup
	logger := log.New(log.Config{DefaultLevel: log.LevelError})
	registry := NewRegistry()

	// Load initial procedures
	loader := NewHierarchicalLoader("tsql", logger)
	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to load procedures: %v", err)
	}
	for _, proc := range result.Procedures {
		registry.Register(proc)
	}

	reloadCount := 0
	var reloadMu sync.Mutex

	watcher, err := NewWatcher(tmpDir, "tsql", registry, logger,
		WithDebounceDelay(50*time.Millisecond),
		WithOnReload(func(proc *Procedure, event string) {
			reloadMu.Lock()
			reloadCount++
			reloadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to set up
	time.Sleep(100 * time.Millisecond)

	// "Touch" the file (write same content)
	if err := os.WriteFile(procPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to touch procedure file: %v", err)
	}

	// Wait for debounce + processing
	time.Sleep(200 * time.Millisecond)

	// Verify callback was NOT called (source hash unchanged)
	reloadMu.Lock()
	if reloadCount != 0 {
		t.Errorf("expected 0 reload callbacks for unchanged file, got %d", reloadCount)
	}
	reloadMu.Unlock()
}

func TestWatcher_GlobalProcedures(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create _global/dbo structure
	globalDir := filepath.Join(tmpDir, "_global", "dbo")
	if err := os.MkdirAll(globalDir, 0755); err != nil {
		t.Fatalf("failed to create global dir: %v", err)
	}

	// Setup
	logger := log.New(log.Config{DefaultLevel: log.LevelError})
	registry := NewRegistry()

	var reloadedProc *Procedure
	var reloadMu sync.Mutex

	watcher, err := NewWatcher(tmpDir, "tsql", registry, logger,
		WithDebounceDelay(50*time.Millisecond),
		WithOnReload(func(proc *Procedure, event string) {
			reloadMu.Lock()
			reloadedProc = proc
			reloadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to set up
	time.Sleep(100 * time.Millisecond)

	// Create a global procedure
	procContent := `
CREATE PROCEDURE dbo.GlobalProc
AS
BEGIN
    SELECT 'global' AS Scope
END
`
	procPath := filepath.Join(globalDir, "GlobalProc.sql")
	if err := os.WriteFile(procPath, []byte(procContent), 0644); err != nil {
		t.Fatalf("failed to write procedure file: %v", err)
	}

	// Wait for debounce + processing
	time.Sleep(200 * time.Millisecond)

	// Verify procedure was registered as global
	reloadMu.Lock()
	if reloadedProc == nil {
		t.Fatal("procedure was not reloaded")
	}
	if !reloadedProc.IsGlobal {
		t.Error("procedure should be marked as global")
	}
	if reloadedProc.Database != "" {
		t.Errorf("global procedure should have empty database, got '%s'", reloadedProc.Database)
	}
	reloadMu.Unlock()

	// Verify it's accessible via global lookup
	proc, err := registry.Lookup("dbo.GlobalProc")
	if err != nil {
		t.Fatalf("global procedure not found: %v", err)
	}
	if !proc.IsGlobal {
		t.Error("looked up procedure should be global")
	}
}
