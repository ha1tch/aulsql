package tsqlruntime

import (
	"fmt"
	"runtime"
	"strings"
)

// SQLError represents a T-SQL error
type SQLError struct {
	Number    int
	Severity  int
	State     int
	Message   string
	Procedure string
	Line      int
}

func (e *SQLError) Error() string {
	if e.Procedure != "" {
		return fmt.Sprintf("Msg %d, Level %d, State %d, Procedure %s, Line %d: %s",
			e.Number, e.Severity, e.State, e.Procedure, e.Line, e.Message)
	}
	return fmt.Sprintf("Msg %d, Level %d, State %d, Line %d: %s",
		e.Number, e.Severity, e.State, e.Line, e.Message)
}

// Common SQL Server error numbers
const (
	ErrDivideByZero        = 8134
	ErrConversionFailed    = 245
	ErrArithmeticOverflow  = 8115
	ErrNullNotAllowed      = 515
	ErrConstraintViolation = 547
	ErrDuplicateKey        = 2627
	ErrDeadlock            = 1205
	ErrTimeout             = -2
	ErrInvalidObject       = 208
	ErrInvalidColumn       = 207
	ErrSyntaxError         = 102
	ErrPermissionDenied    = 229
	ErrRaiseError          = 50000
)

// NewSQLError creates a new SQL error
func NewSQLError(number int, message string) *SQLError {
	severity := 16 // Default severity
	if number == ErrDeadlock || number == ErrTimeout {
		severity = 13
	}
	return &SQLError{
		Number:   number,
		Severity: severity,
		State:    1,
		Message:  message,
		Line:     1,
	}
}

// ErrorContext holds error state for TRY/CATCH
type ErrorContext struct {
	HasError    bool
	LastError   *SQLError
	ErrorNumber int
	ErrorMsg    string
	ErrorLine   int
	ErrorProc   string
	ErrorState  int
	ErrorSev    int
	XactState   int // -1 = uncommittable, 0 = no transaction, 1 = committable
}

// NewErrorContext creates a new error context
func NewErrorContext() *ErrorContext {
	return &ErrorContext{
		XactState: 0,
	}
}

// SetError sets the current error
func (ec *ErrorContext) SetError(err *SQLError) {
	ec.HasError = true
	ec.LastError = err
	ec.ErrorNumber = err.Number
	ec.ErrorMsg = err.Message
	ec.ErrorLine = err.Line
	ec.ErrorProc = err.Procedure
	ec.ErrorState = err.State
	ec.ErrorSev = err.Severity
}

// Clear clears the error state
func (ec *ErrorContext) Clear() {
	ec.HasError = false
	ec.LastError = nil
	ec.ErrorNumber = 0
	ec.ErrorMsg = ""
	ec.ErrorLine = 0
	ec.ErrorProc = ""
	ec.ErrorState = 0
	ec.ErrorSev = 0
}

// TryCatchHandler handles TRY/CATCH block execution
type TryCatchHandler struct {
	errorCtx    *ErrorContext
	inTryBlock  bool
	inCatchBlock bool
}

// NewTryCatchHandler creates a new TRY/CATCH handler
func NewTryCatchHandler() *TryCatchHandler {
	return &TryCatchHandler{
		errorCtx: NewErrorContext(),
	}
}

// EnterTry marks entry into a TRY block
func (h *TryCatchHandler) EnterTry() {
	h.inTryBlock = true
	h.inCatchBlock = false
	h.errorCtx.Clear()
}

// ExitTry marks exit from a TRY block
func (h *TryCatchHandler) ExitTry() {
	h.inTryBlock = false
}

// EnterCatch marks entry into a CATCH block
func (h *TryCatchHandler) EnterCatch() {
	h.inCatchBlock = true
	h.inTryBlock = false
}

// ExitCatch marks exit from a CATCH block
func (h *TryCatchHandler) ExitCatch() {
	h.inCatchBlock = false
	h.errorCtx.Clear()
}

// HandleError processes an error during TRY block execution
// Returns true if error was caught, false if it should propagate
func (h *TryCatchHandler) HandleError(err error) bool {
	if !h.inTryBlock {
		return false
	}

	// Convert to SQLError if needed
	var sqlErr *SQLError
	if se, ok := err.(*SQLError); ok {
		sqlErr = se
	} else {
		sqlErr = &SQLError{
			Number:   50000,
			Severity: 16,
			State:    1,
			Message:  err.Error(),
			Line:     1,
		}
	}

	h.errorCtx.SetError(sqlErr)
	return true
}

// HasCaughtError returns true if an error was caught
func (h *TryCatchHandler) HasCaughtError() bool {
	return h.errorCtx.HasError
}

// GetErrorNumber returns ERROR_NUMBER()
func (h *TryCatchHandler) GetErrorNumber() int {
	return h.errorCtx.ErrorNumber
}

// GetErrorMessage returns ERROR_MESSAGE()
func (h *TryCatchHandler) GetErrorMessage() string {
	return h.errorCtx.ErrorMsg
}

// GetErrorLine returns ERROR_LINE()
func (h *TryCatchHandler) GetErrorLine() int {
	return h.errorCtx.ErrorLine
}

// GetErrorProcedure returns ERROR_PROCEDURE()
func (h *TryCatchHandler) GetErrorProcedure() string {
	return h.errorCtx.ErrorProc
}

// GetErrorState returns ERROR_STATE()
func (h *TryCatchHandler) GetErrorState() int {
	return h.errorCtx.ErrorState
}

// GetErrorSeverity returns ERROR_SEVERITY()
func (h *TryCatchHandler) GetErrorSeverity() int {
	return h.errorCtx.ErrorSev
}

// GetXactState returns XACT_STATE()
func (h *TryCatchHandler) GetXactState() int {
	return h.errorCtx.XactState
}

// SetXactState sets the transaction state
func (h *TryCatchHandler) SetXactState(state int) {
	h.errorCtx.XactState = state
}

// RaiseError creates a RAISERROR
func RaiseError(msg string, severity, state int, args ...interface{}) *SQLError {
	// Format message with arguments
	formattedMsg := msg
	if len(args) > 0 {
		// Simple printf-style formatting
		formattedMsg = formatRaiseErrorMsg(msg, args)
	}

	return &SQLError{
		Number:   ErrRaiseError,
		Severity: severity,
		State:    state,
		Message:  formattedMsg,
		Line:     getCallerLine(),
	}
}

// ThrowError creates a THROW error
func ThrowError(number int, message string, state int) *SQLError {
	return &SQLError{
		Number:   number,
		Severity: 16,
		State:    state,
		Message:  message,
		Line:     getCallerLine(),
	}
}

// formatRaiseErrorMsg formats a RAISERROR message
func formatRaiseErrorMsg(msg string, args []interface{}) string {
	result := msg
	for i, arg := range args {
		placeholder := fmt.Sprintf("%%%d", i+1)
		var replacement string
		switch v := arg.(type) {
		case string:
			replacement = v
		case int, int32, int64:
			replacement = fmt.Sprintf("%d", v)
		case float32, float64:
			replacement = fmt.Sprintf("%f", v)
		default:
			replacement = fmt.Sprintf("%v", v)
		}
		result = strings.Replace(result, placeholder, replacement, 1)
	}
	return result
}

// getCallerLine attempts to get the caller's line number
func getCallerLine() int {
	_, _, line, ok := runtime.Caller(2)
	if ok {
		return line
	}
	return 0
}

// WrapError wraps a Go error as a SQLError
func WrapError(err error) *SQLError {
	if err == nil {
		return nil
	}
	if sqlErr, ok := err.(*SQLError); ok {
		return sqlErr
	}

	msg := err.Error()

	// Try to detect error type from message
	number := 50000
	if strings.Contains(msg, "divide by zero") || strings.Contains(msg, "division by zero") {
		number = ErrDivideByZero
	} else if strings.Contains(msg, "overflow") {
		number = ErrArithmeticOverflow
	} else if strings.Contains(msg, "null") && strings.Contains(msg, "not allowed") {
		number = ErrNullNotAllowed
	} else if strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique constraint") {
		number = ErrDuplicateKey
	} else if strings.Contains(msg, "deadlock") {
		number = ErrDeadlock
	} else if strings.Contains(msg, "timeout") {
		number = ErrTimeout
	} else if strings.Contains(msg, "invalid object") || strings.Contains(msg, "does not exist") {
		number = ErrInvalidObject
	} else if strings.Contains(msg, "invalid column") {
		number = ErrInvalidColumn
	}

	return &SQLError{
		Number:   number,
		Severity: 16,
		State:    1,
		Message:  msg,
	}
}

// IsCriticalError returns true if the error should abort the batch
func IsCriticalError(err *SQLError) bool {
	if err == nil {
		return false
	}
	// Severity 20+ are fatal
	if err.Severity >= 20 {
		return true
	}
	// Certain errors are always critical
	switch err.Number {
	case ErrDeadlock:
		return true
	}
	return false
}

// ShouldRollback returns true if the error should trigger a rollback
func ShouldRollback(err *SQLError, xactAbort bool) bool {
	if err == nil {
		return false
	}
	// With XACT_ABORT ON, most errors cause rollback
	if xactAbort && err.Severity >= 16 {
		return true
	}
	// Always rollback on deadlock
	if err.Number == ErrDeadlock {
		return true
	}
	return false
}
