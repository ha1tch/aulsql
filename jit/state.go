package jit

import "time"

// CompileState represents the state of a procedure in the JIT compilation pipeline.
type CompileState int

const (
	// StateNone means the procedure has not been considered for JIT compilation.
	StateNone CompileState = iota

	// StateQueued means the procedure is waiting in the compilation queue.
	StateQueued

	// StateCompiling means the procedure is currently being compiled.
	StateCompiling

	// StateReady means the procedure is compiled and ready for execution.
	StateReady

	// StateFailed means compilation failed; may be retried after backoff.
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

// CompileStatus tracks the compilation status of a procedure.
type CompileStatus struct {
	// Current state in the compilation pipeline
	State CompileState

	// Source hash when compilation was initiated
	// Used to detect if source changed and recompilation is needed
	SourceHash string

	// Timing information
	QueuedAt    time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	// Error information (when State == StateFailed)
	Error      string
	RetryCount int
}

// CanEnqueue returns true if this status allows enqueueing a new compilation.
func (s *CompileStatus) CanEnqueue(newSourceHash string) bool {
	if s == nil {
		return true
	}

	switch s.State {
	case StateNone:
		return true
	case StateQueued, StateCompiling:
		// Already in progress
		return false
	case StateReady:
		// Only recompile if source changed
		return s.SourceHash != newSourceHash
	case StateFailed:
		// Can retry (backoff is checked elsewhere)
		return true
	default:
		return false
	}
}

// IsExecutable returns true if the procedure can be executed via JIT.
func (s *CompileStatus) IsExecutable(currentSourceHash string) bool {
	if s == nil {
		return false
	}
	return s.State == StateReady && s.SourceHash == currentSourceHash
}
