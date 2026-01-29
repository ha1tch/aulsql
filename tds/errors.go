package tds

// SQL Server error numbers and severity levels.
// Reference: https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors

// Error severity classes
const (
	SeverityInfo       uint8 = 0  // Informational
	SeveritySuccess    uint8 = 1  // Success with info
	SeverityWarning    uint8 = 10 // Warning
	SeverityUserError  uint8 = 11 // User correctable error
	SeverityMissing    uint8 = 12 // Missing object
	SeverityDeadlock   uint8 = 13 // Deadlock victim
	SeverityPermission uint8 = 14 // Permission denied
	SeveritySyntax     uint8 = 15 // Syntax error
	SeverityGeneral    uint8 = 16 // General error (most common)
	SeverityResource   uint8 = 17 // Resource error
	SeverityInternal   uint8 = 18 // Non-fatal internal error
	SeverityLimit      uint8 = 19 // Resource limit
	SeverityFatal      uint8 = 20 // Fatal error (connection closed)
	SeverityDB         uint8 = 21 // Database-level fatal
	SeverityTable      uint8 = 22 // Table integrity error
	SeverityDevice     uint8 = 23 // Device/media error
	SeverityHardware   uint8 = 24 // Hardware error
	SeveritySystem     uint8 = 25 // System error
)

// Common SQL Server error numbers
const (
	// Login/auth errors (severity 14)
	ErrLoginFailed      int32 = 18456 // Login failed for user '%s'
	ErrDatabaseNotExist int32 = 4060  // Cannot open database '%s'
	ErrPermissionDenied int32 = 229   // Permission denied on object

	// Syntax errors (severity 15)
	ErrSyntax           int32 = 102   // Incorrect syntax near '%s'
	ErrInvalidColumn    int32 = 207   // Invalid column name '%s'
	ErrInvalidObject    int32 = 208   // Invalid object name '%s'
	ErrAmbiguousColumn  int32 = 209   // Ambiguous column name '%s'

	// General errors (severity 16)
	ErrGeneral          int32 = 50000 // User-defined error
	ErrDivideByZero     int32 = 8134  // Divide by zero
	ErrOverflow         int32 = 8115  // Arithmetic overflow
	ErrConversion       int32 = 245   // Conversion failed
	ErrNullNotAllowed   int32 = 515   // Cannot insert NULL
	ErrDuplicateKey     int32 = 2627  // Duplicate key violation
	ErrForeignKey       int32 = 547   // Foreign key violation
	ErrCheckConstraint  int32 = 547   // Check constraint violation
	ErrTruncation       int32 = 8152  // String or binary data truncated
	ErrTimeout          int32 = -2    // Timeout expired

	// Transaction errors (severity 16)
	ErrDeadlock         int32 = 1205  // Transaction deadlock victim
	ErrTxnAborted       int32 = 3998  // Uncommittable transaction
	ErrTxnNotStarted    int32 = 3902  // COMMIT without BEGIN TRANSACTION

	// Procedure errors (severity 16)
	ErrProcNotFound     int32 = 2812  // Could not find stored procedure '%s'
	ErrParamMissing     int32 = 201   // Procedure expects parameter '%s'
	ErrParamTooMany     int32 = 8144  // Too many arguments specified

	// Resource errors (severity 17-19)
	ErrTempDBFull       int32 = 1105  // Could not allocate space in tempdb
	ErrLockTimeout      int32 = 1222  // Lock request time out
)

// ErrorInfo contains structured error information.
type ErrorInfo struct {
	Number   int32
	State    uint8
	Severity uint8
	Message  string
	ProcName string
	LineNo   int32
}

// NewError creates a new ErrorInfo with defaults.
func NewError(number int32, message string) *ErrorInfo {
	return &ErrorInfo{
		Number:   number,
		State:    1,
		Severity: SeverityGeneral,
		Message:  message,
	}
}

// WithSeverity sets the severity level.
func (e *ErrorInfo) WithSeverity(sev uint8) *ErrorInfo {
	e.Severity = sev
	return e
}

// WithState sets the state code.
func (e *ErrorInfo) WithState(state uint8) *ErrorInfo {
	e.State = state
	return e
}

// WithProc sets the procedure name and line number.
func (e *ErrorInfo) WithProc(procName string, lineNo int32) *ErrorInfo {
	e.ProcName = procName
	e.LineNo = lineNo
	return e
}

// Write writes the error to a TokenWriter.
func (e *ErrorInfo) Write(tw *TokenWriter, serverName string) {
	tw.WriteError(e.Number, e.State, e.Severity, e.Message, serverName, e.ProcName, e.LineNo)
}

// Common error constructors

// LoginFailedError creates a login failed error.
func LoginFailedError(username, reason string) *ErrorInfo {
	msg := "Login failed for user '" + username + "'."
	if reason != "" {
		msg += " " + reason
	}
	return &ErrorInfo{
		Number:   ErrLoginFailed,
		State:    1,
		Severity: SeverityPermission,
		Message:  msg,
	}
}

// SyntaxError creates a syntax error.
func SyntaxError(near string, lineNo int32) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrSyntax,
		State:    1,
		Severity: SeveritySyntax,
		Message:  "Incorrect syntax near '" + near + "'.",
		LineNo:   lineNo,
	}
}

// InvalidObjectError creates an invalid object name error.
func InvalidObjectError(objectName string) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrInvalidObject,
		State:    1,
		Severity: SeverityGeneral,
		Message:  "Invalid object name '" + objectName + "'.",
	}
}

// InvalidColumnError creates an invalid column name error.
func InvalidColumnError(columnName string) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrInvalidColumn,
		State:    1,
		Severity: SeverityGeneral,
		Message:  "Invalid column name '" + columnName + "'.",
	}
}

// ProcNotFoundError creates a procedure not found error.
func ProcNotFoundError(procName string) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrProcNotFound,
		State:    62, // Standard state for this error
		Severity: SeverityGeneral,
		Message:  "Could not find stored procedure '" + procName + "'.",
	}
}

// ConversionError creates a type conversion error.
func ConversionError(fromType, toType string) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrConversion,
		State:    1,
		Severity: SeverityGeneral,
		Message:  "Conversion failed when converting " + fromType + " to " + toType + ".",
	}
}

// DivideByZeroError creates a divide by zero error.
func DivideByZeroError() *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrDivideByZero,
		State:    1,
		Severity: SeverityGeneral,
		Message:  "Divide by zero error encountered.",
	}
}

// TimeoutError creates a query timeout error.
func TimeoutError() *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrTimeout,
		State:    0,
		Severity: SeverityGeneral,
		Message:  "Timeout expired. The timeout period elapsed prior to completion of the operation or the server is not responding.",
	}
}

// DeadlockError creates a deadlock victim error.
func DeadlockError() *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrDeadlock,
		State:    45,
		Severity: SeverityDeadlock,
		Message:  "Transaction was deadlocked on resources with another process and has been chosen as the deadlock victim. Rerun the transaction.",
	}
}

// GeneralError creates a general user error.
func GeneralError(message string) *ErrorInfo {
	return &ErrorInfo{
		Number:   ErrGeneral,
		State:    1,
		Severity: SeverityGeneral,
		Message:  message,
	}
}
