// Package errors provides structured error handling for aul.
//
// This package defines error types with:
//   - Error codes for programmatic handling
//   - Categories for grouping related errors
//   - Context fields for debugging
//   - Stack traces for development
//   - Wrapping support for error chains
//
// Error codes follow a hierarchical scheme:
//   - 1xxx: Configuration errors
//   - 2xxx: Connection/protocol errors
//   - 3xxx: Procedure errors
//   - 4xxx: Execution errors
//   - 5xxx: Storage errors
//   - 6xxx: JIT compilation errors
//   - 9xxx: Internal errors
package errors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// Code is a numeric error code for programmatic handling.
type Code int

// Error codes by category
const (
	// Configuration errors (1xxx)
	ErrCodeConfigInvalid     Code = 1001
	ErrCodeConfigMissing     Code = 1002
	ErrCodeConfigParse       Code = 1003
	ErrCodeConfigValidation  Code = 1004

	// Connection/protocol errors (2xxx)
	ErrCodeConnectionFailed  Code = 2001
	ErrCodeConnectionClosed  Code = 2002
	ErrCodeConnectionTimeout Code = 2003
	ErrCodeProtocolError     Code = 2004
	ErrCodeHandshakeFailed   Code = 2005
	ErrCodeAuthFailed        Code = 2006
	ErrCodeTLSError          Code = 2007
	ErrCodeUnsupportedProto  Code = 2008

	// Procedure errors (3xxx)
	ErrCodeProcNotFound        Code = 3001
	ErrCodeProcAlreadyExists   Code = 3002
	ErrCodeProcParseError      Code = 3003
	ErrCodeProcLoadError       Code = 3004
	ErrCodeProcInvalidParam    Code = 3005
	ErrCodeProcMissingParam    Code = 3006
	ErrCodeProcValidationError Code = 3007

	// Execution errors (4xxx)
	ErrCodeExecFailed        Code = 4001
	ErrCodeExecTimeout       Code = 4002
	ErrCodeExecCancelled     Code = 4003
	ErrCodeExecNestingLimit  Code = 4004
	ErrCodeExecConcurrency   Code = 4005
	ErrCodeExecSQLError      Code = 4006
	ErrCodeExecInvalidState  Code = 4007
	ErrCodeExecNoTransaction Code = 4008

	// Storage errors (5xxx)
	ErrCodeStorageConnect    Code = 5001
	ErrCodeStorageQuery      Code = 5002
	ErrCodeStorageExec       Code = 5003
	ErrCodeStorageTxn        Code = 5004
	ErrCodeStorageNotFound   Code = 5005
	ErrCodeStorageConstraint Code = 5006

	// JIT compilation errors (6xxx)
	ErrCodeJITDisabled       Code = 6001
	ErrCodeJITQueueFull      Code = 6002
	ErrCodeJITTranspile      Code = 6003
	ErrCodeJITCompile        Code = 6004
	ErrCodeJITLoad           Code = 6005
	ErrCodeJITNotCompiled    Code = 6006

	// Internal errors (9xxx)
	ErrCodeInternal          Code = 9001
	ErrCodeNotImplemented    Code = 9002
	ErrCodePanic             Code = 9003
	ErrCodeResourceExhausted Code = 9004
)

// String returns the error code as a string.
func (c Code) String() string {
	return fmt.Sprintf("E%04d", c)
}

// Category returns the category for this code.
func (c Code) Category() string {
	switch {
	case c >= 1000 && c < 2000:
		return "configuration"
	case c >= 2000 && c < 3000:
		return "connection"
	case c >= 3000 && c < 4000:
		return "procedure"
	case c >= 4000 && c < 5000:
		return "execution"
	case c >= 5000 && c < 6000:
		return "storage"
	case c >= 6000 && c < 7000:
		return "jit"
	case c >= 9000:
		return "internal"
	default:
		return "unknown"
	}
}

// Severity indicates error severity.
type Severity int

const (
	SeverityWarning  Severity = iota // Recoverable, operation may continue
	SeverityError                    // Operation failed, but system is healthy
	SeverityCritical                 // System may be in degraded state
	SeverityFatal                    // System cannot continue
)

func (s Severity) String() string {
	switch s {
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityCritical:
		return "critical"
	case SeverityFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Error is a structured error with code, context, and optional cause.
type Error struct {
	// Core error information
	Code     Code
	Message  string
	Severity Severity

	// Context
	Fields map[string]interface{}

	// Error chain
	Cause error

	// Debug information
	Stack  []Frame
	Time   time.Time
	OpName string // Operation that failed (e.g., "JIT.Compile", "Procedure.Load")
}

// Frame represents a stack frame.
type Frame struct {
	Function string
	File     string
	Line     int
}

// Error implements the error interface.
func (e *Error) Error() string {
	var buf strings.Builder

	buf.WriteString(e.Code.String())
	buf.WriteString(": ")
	buf.WriteString(e.Message)

	if e.Cause != nil {
		buf.WriteString(": ")
		buf.WriteString(e.Cause.Error())
	}

	return buf.String()
}

// Unwrap returns the underlying cause for errors.Is/As support.
func (e *Error) Unwrap() error {
	return e.Cause
}

// Format implements fmt.Formatter for detailed output.
func (e *Error) Format(f fmt.State, verb rune) {
	switch verb {
	case 'v':
		if f.Flag('+') {
			// Detailed format with stack trace
			fmt.Fprintf(f, "%s [%s] %s: %s\n",
				e.Time.Format(time.RFC3339),
				e.Severity,
				e.Code.String(),
				e.Message)

			if e.OpName != "" {
				fmt.Fprintf(f, "  Operation: %s\n", e.OpName)
			}

			if len(e.Fields) > 0 {
				fmt.Fprintf(f, "  Context:\n")
				for k, v := range e.Fields {
					fmt.Fprintf(f, "    %s: %v\n", k, v)
				}
			}

			if e.Cause != nil {
				fmt.Fprintf(f, "  Caused by: %v\n", e.Cause)
			}

			if len(e.Stack) > 0 {
				fmt.Fprintf(f, "  Stack:\n")
				for _, frame := range e.Stack {
					fmt.Fprintf(f, "    %s\n      %s:%d\n",
						frame.Function, frame.File, frame.Line)
				}
			}
			return
		}
		fallthrough
	case 's':
		fmt.Fprint(f, e.Error())
	case 'q':
		fmt.Fprintf(f, "%q", e.Error())
	}
}

// WithField adds a context field to the error.
func (e *Error) WithField(key string, value interface{}) *Error {
	if e.Fields == nil {
		e.Fields = make(map[string]interface{})
	}
	e.Fields[key] = value
	return e
}

// WithFields adds multiple context fields to the error.
func (e *Error) WithFields(fields map[string]interface{}) *Error {
	if e.Fields == nil {
		e.Fields = make(map[string]interface{})
	}
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// WithOp sets the operation name.
func (e *Error) WithOp(op string) *Error {
	e.OpName = op
	return e
}

// Builder helps construct errors fluently.
type Builder struct {
	code     Code
	message  string
	severity Severity
	cause    error
	fields   map[string]interface{}
	op       string
	stack    bool
}

// New starts building a new error with the given code.
func New(code Code, message string) *Builder {
	return &Builder{
		code:     code,
		message:  message,
		severity: SeverityError,
	}
}

// Newf starts building a new error with a formatted message.
func Newf(code Code, format string, args ...interface{}) *Builder {
	return &Builder{
		code:     code,
		message:  fmt.Sprintf(format, args...),
		severity: SeverityError,
	}
}

// Wrap wraps an existing error with a code and message.
func Wrap(cause error, code Code, message string) *Builder {
	return &Builder{
		code:     code,
		message:  message,
		severity: SeverityError,
		cause:    cause,
	}
}

// Wrapf wraps an existing error with a formatted message.
func Wrapf(cause error, code Code, format string, args ...interface{}) *Builder {
	return &Builder{
		code:     code,
		message:  fmt.Sprintf(format, args...),
		severity: SeverityError,
		cause:    cause,
	}
}

// Severity sets the error severity.
func (b *Builder) Severity(s Severity) *Builder {
	b.severity = s
	return b
}

// Warning sets severity to warning.
func (b *Builder) Warning() *Builder {
	b.severity = SeverityWarning
	return b
}

// Critical sets severity to critical.
func (b *Builder) Critical() *Builder {
	b.severity = SeverityCritical
	return b
}

// Fatal sets severity to fatal.
func (b *Builder) Fatal() *Builder {
	b.severity = SeverityFatal
	return b
}

// WithCause adds a cause to the error.
func (b *Builder) WithCause(err error) *Builder {
	b.cause = err
	return b
}

// WithField adds a context field.
func (b *Builder) WithField(key string, value interface{}) *Builder {
	if b.fields == nil {
		b.fields = make(map[string]interface{})
	}
	b.fields[key] = value
	return b
}

// WithFields adds multiple context fields.
func (b *Builder) WithFields(fields map[string]interface{}) *Builder {
	if b.fields == nil {
		b.fields = make(map[string]interface{})
	}
	for k, v := range fields {
		b.fields[k] = v
	}
	return b
}

// WithOp sets the operation name.
func (b *Builder) WithOp(op string) *Builder {
	b.op = op
	return b
}

// WithStack captures a stack trace.
func (b *Builder) WithStack() *Builder {
	b.stack = true
	return b
}

// Build creates the Error.
func (b *Builder) Build() *Error {
	e := &Error{
		Code:     b.code,
		Message:  b.message,
		Severity: b.severity,
		Cause:    b.cause,
		Fields:   b.fields,
		OpName:   b.op,
		Time:     time.Now(),
	}

	if b.stack {
		e.Stack = captureStack(2) // Skip Build and caller
	}

	return e
}

// Err is a shorthand for Build() that returns error interface.
func (b *Builder) Err() error {
	return b.Build()
}

// captureStack captures the current stack trace.
func captureStack(skip int) []Frame {
	var frames []Frame
	pcs := make([]uintptr, 32)
	n := runtime.Callers(skip+1, pcs)
	pcs = pcs[:n]

	callersFrames := runtime.CallersFrames(pcs)
	for {
		frame, more := callersFrames.Next()
		if !more {
			break
		}

		// Skip runtime internals
		if strings.Contains(frame.Function, "runtime.") {
			continue
		}

		frames = append(frames, Frame{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
		})

		if len(frames) >= 10 {
			break
		}
	}

	return frames
}

// Helper functions for common error types

// NotFound creates a "not found" error for the given entity.
func NotFound(entity, identifier string) *Builder {
	return Newf(ErrCodeProcNotFound, "%s not found: %s", entity, identifier).
		WithField("entity", entity).
		WithField("identifier", identifier)
}

// AlreadyExists creates an "already exists" error.
func AlreadyExists(entity, identifier string) *Builder {
	return Newf(ErrCodeProcAlreadyExists, "%s already exists: %s", entity, identifier).
		WithField("entity", entity).
		WithField("identifier", identifier)
}

// InvalidInput creates an invalid input error.
func InvalidInput(field, reason string) *Builder {
	return Newf(ErrCodeProcInvalidParam, "invalid %s: %s", field, reason).
		WithField("field", field).
		WithField("reason", reason)
}

// Timeout creates a timeout error.
func Timeout(operation string, duration time.Duration) *Builder {
	return Newf(ErrCodeExecTimeout, "operation %s timed out after %v", operation, duration).
		WithField("operation", operation).
		WithField("timeout", duration)
}

// NotImplemented creates a "not implemented" error.
func NotImplemented(feature string) *Builder {
	return Newf(ErrCodeNotImplemented, "%s not yet implemented", feature).
		WithField("feature", feature)
}

// Internal creates an internal error (for unexpected conditions).
func Internal(msg string) *Builder {
	return New(ErrCodeInternal, msg).Critical().WithStack()
}

// Extraction helpers

// GetCode extracts the error code from an error, or returns ErrCodeInternal.
func GetCode(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ErrCodeInternal
}

// GetSeverity extracts the severity from an error.
func GetSeverity(err error) Severity {
	var e *Error
	if errors.As(err, &e) {
		return e.Severity
	}
	return SeverityError
}

// GetFields extracts context fields from an error.
func GetFields(err error) map[string]interface{} {
	var e *Error
	if errors.As(err, &e) {
		return e.Fields
	}
	return nil
}

// IsCode checks if an error has a specific code.
func IsCode(err error, code Code) bool {
	return GetCode(err) == code
}

// IsCategory checks if an error belongs to a category.
func IsCategory(err error, category string) bool {
	return GetCode(err).Category() == category
}

// IsSevere checks if an error is critical or fatal.
func IsSevere(err error) bool {
	s := GetSeverity(err)
	return s >= SeverityCritical
}

// Standard library compatibility

// Is reports whether any error in err's chain matches target.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As finds the first error in err's chain that matches target.
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Join combines multiple errors.
func Join(errs ...error) error {
	return errors.Join(errs...)
}
