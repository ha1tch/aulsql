// Package procedure provides stored procedure management for aul.
package procedure

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ha1tch/aul/pkg/log"
)

// Watcher monitors a procedure directory for changes and triggers reloads.
type Watcher struct {
	mu sync.RWMutex

	// Configuration
	root     string
	dialect  string
	registry *Registry
	logger   *log.Logger

	// Loader for reloading procedures
	loader *HierarchicalLoader

	// fsnotify watcher
	fsWatcher *fsnotify.Watcher

	// State
	running  bool
	stopCh   chan struct{}
	doneCh   chan struct{}

	// Debouncing: collect events and process in batches
	debounceDelay time.Duration
	pendingEvents map[string]fsnotify.Op
	eventTimer    *time.Timer

	// Callbacks
	onReload func(proc *Procedure, event string) // Called when a procedure is reloaded
	onError  func(err error)                     // Called on errors
}

// WatcherOption configures the watcher.
type WatcherOption func(*Watcher)

// WithDebounceDelay sets the debounce delay for batching file events.
// Default is 100ms.
func WithDebounceDelay(d time.Duration) WatcherOption {
	return func(w *Watcher) {
		w.debounceDelay = d
	}
}

// WithOnReload sets a callback for reload events.
func WithOnReload(fn func(proc *Procedure, event string)) WatcherOption {
	return func(w *Watcher) {
		w.onReload = fn
	}
}

// WithOnError sets a callback for error events.
func WithOnError(fn func(err error)) WatcherOption {
	return func(w *Watcher) {
		w.onError = fn
	}
}

// NewWatcher creates a new procedure watcher.
func NewWatcher(root, dialect string, registry *Registry, logger *log.Logger, opts ...WatcherOption) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		root:          root,
		dialect:       dialect,
		registry:      registry,
		logger:        logger,
		loader:        NewHierarchicalLoader(dialect, logger),
		fsWatcher:     fsw,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		debounceDelay: 100 * time.Millisecond,
		pendingEvents: make(map[string]fsnotify.Op),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Start begins watching for file changes.
func (w *Watcher) Start() error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = true
	w.mu.Unlock()

	// Add watches for all directories
	if err := w.addWatchesRecursive(w.root); err != nil {
		return err
	}

	w.logger.Application().Info("procedure watcher started",
		"root", w.root,
	)

	// Start event processing goroutine
	go w.processEvents()

	return nil
}

// Stop stops the watcher.
func (w *Watcher) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	close(w.stopCh)
	<-w.doneCh // Wait for event processor to finish

	w.logger.Application().Info("procedure watcher stopped")

	return w.fsWatcher.Close()
}

// addWatchesRecursive adds watches for a directory and all subdirectories.
func (w *Watcher) addWatchesRecursive(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		// Skip hidden directories and special directories
		name := info.Name()
		if strings.HasPrefix(name, ".") {
			return filepath.SkipDir
		}

		if err := w.fsWatcher.Add(path); err != nil {
			w.logger.Application().Warn("failed to watch directory",
				"path", path,
				"error", err.Error(),
			)
			// Continue watching other directories
			return nil
		}

		w.logger.Application().Debug("watching directory",
			"path", path,
		)

		return nil
	})
}

// processEvents handles fsnotify events.
func (w *Watcher) processEvents() {
	defer close(w.doneCh)

	for {
		select {
		case <-w.stopCh:
			// Stop any pending timer
			if w.eventTimer != nil {
				w.eventTimer.Stop()
			}
			return

		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}
			w.handleEvent(event)

		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			w.logger.Application().Error("watcher error", err)
			if w.onError != nil {
				w.onError(err)
			}
		}
	}
}

// handleEvent processes a single fsnotify event with debouncing.
func (w *Watcher) handleEvent(event fsnotify.Event) {
	// Only care about SQL files
	if !strings.HasSuffix(strings.ToLower(event.Name), ".sql") {
		// But handle new directories
		if event.Has(fsnotify.Create) {
			if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
				w.fsWatcher.Add(event.Name)
				w.logger.Application().Debug("added watch for new directory",
					"path", event.Name,
				)
			}
		}
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Accumulate events (last operation wins for same file)
	w.pendingEvents[event.Name] = event.Op

	// Reset/start debounce timer
	if w.eventTimer != nil {
		w.eventTimer.Stop()
	}
	w.eventTimer = time.AfterFunc(w.debounceDelay, w.processPendingEvents)
}

// processPendingEvents processes all accumulated events.
func (w *Watcher) processPendingEvents() {
	w.mu.Lock()
	events := w.pendingEvents
	w.pendingEvents = make(map[string]fsnotify.Op)
	w.mu.Unlock()

	for path, op := range events {
		w.processFileEvent(path, op)
	}
}

// processFileEvent handles a single file change.
func (w *Watcher) processFileEvent(path string, op fsnotify.Op) {
	// Determine the event type
	if op.Has(fsnotify.Remove) || op.Has(fsnotify.Rename) {
		w.handleFileRemoved(path)
		return
	}

	if op.Has(fsnotify.Create) || op.Has(fsnotify.Write) {
		w.handleFileChanged(path)
		return
	}
}

// handleFileChanged handles a new or modified procedure file.
func (w *Watcher) handleFileChanged(path string) {
	// Parse the path to extract database and schema
	relPath, err := filepath.Rel(w.root, path)
	if err != nil {
		w.logger.Application().Warn("failed to get relative path",
			"path", path,
			"error", err.Error(),
		)
		return
	}

	parts := strings.Split(filepath.ToSlash(relPath), "/")
	if len(parts) < 1 {
		return
	}

	// Determine database, schema, and tenant from path
	var dbName, schemaName, tenant string
	var isGlobal bool

	// Check for tenant path: _tenant/{tenant}/database/schema/file.sql
	if parts[0] == "_tenant" && len(parts) >= 4 {
		tenant = parts[1]
		dbName = parts[2]
		if len(parts) >= 5 {
			schemaName = parts[3]
		} else {
			schemaName = "dbo"
		}
	} else {
		switch len(parts) {
		case 1:
			// File directly in root - use dbo schema, no database
			schemaName = "dbo"
		case 2:
			// database/file.sql or _global/file.sql
			if parts[0] == "_global" {
				isGlobal = true
				schemaName = "dbo"
			} else {
				dbName = parts[0]
				schemaName = "dbo"
			}
		default:
			// database/schema/file.sql or _global/schema/file.sql
			if parts[0] == "_global" {
				isGlobal = true
				schemaName = parts[1]
			} else {
				dbName = parts[0]
				schemaName = parts[1]
			}
		}
	}

	// Load the procedure
	proc, err := w.loader.loadFile(path, dbName, schemaName, isGlobal, tenant)
	if err != nil {
		w.logger.Application().Error("failed to reload procedure", err,
			"path", path,
		)
		if w.onError != nil {
			w.onError(err)
		}
		return
	}

	// Check if this is an update or new procedure
	existingProc, lookupErr := w.registry.LookupByFile(path)
	eventType := "created"
	if lookupErr == nil && existingProc != nil {
		// Check if source actually changed
		if existingProc.SourceHash == proc.SourceHash {
			w.logger.Application().Debug("procedure unchanged, skipping reload",
				"procedure", proc.QualifiedName(),
				"path", path,
			)
			return
		}
		eventType = "modified"

		// Preserve execution stats
		proc.ExecCount = existingProc.ExecCount
		proc.TotalTimeNs = existingProc.TotalTimeNs
		proc.LastExecAt = existingProc.LastExecAt

		// Clear JIT state (will need recompilation)
		proc.JITCompiled = false
		proc.JITCode = nil
	}

	// Register (will overwrite if exists)
	if err := w.registry.Register(proc); err != nil {
		w.logger.Application().Error("failed to register reloaded procedure", err,
			"procedure", proc.QualifiedName(),
			"path", path,
		)
		if w.onError != nil {
			w.onError(err)
		}
		return
	}

	w.logger.Application().Info("procedure reloaded",
		"procedure", proc.QualifiedName(),
		"event", eventType,
		"path", path,
	)

	if w.onReload != nil {
		w.onReload(proc, eventType)
	}
}

// handleFileRemoved handles a deleted procedure file.
func (w *Watcher) handleFileRemoved(path string) {
	// Find procedure by file path
	proc, err := w.registry.LookupByFile(path)
	if err != nil {
		// Procedure wasn't registered, nothing to do
		return
	}

	// Unregister the procedure
	if err := w.registry.Unregister(proc.QualifiedName()); err != nil {
		w.logger.Application().Error("failed to unregister removed procedure", err,
			"procedure", proc.QualifiedName(),
			"path", path,
		)
		if w.onError != nil {
			w.onError(err)
		}
		return
	}

	w.logger.Application().Info("procedure removed",
		"procedure", proc.QualifiedName(),
		"path", path,
	)

	if w.onReload != nil {
		w.onReload(proc, "removed")
	}
}

// IsRunning returns whether the watcher is currently running.
func (w *Watcher) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.running
}
