// Package tsqlruntime provides runtime support for transpiled T-SQL procedures.
//
// SPLogger provides structured logging for stored procedure errors, matching
// the common T-SQL pattern of logging errors to a table in CATCH blocks.
package tsqlruntime

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// SPError represents an error captured in a stored procedure's CATCH block.
type SPError struct {
	// ProcedureName is the name of the stored procedure where the error occurred.
	ProcedureName string `json:"procedure_name" xml:"ProcedureName"`

	// Parameters contains the input parameters at the time of the error.
	Parameters map[string]interface{} `json:"parameters" xml:"Parameters"`

	// ErrorMessage is the error message (equivalent to ERROR_MESSAGE()).
	ErrorMessage string `json:"error_message" xml:"ErrorMessage"`

	// ErrorNumber is the error number (equivalent to ERROR_NUMBER()).
	// In Go, this is typically 0 unless parsed from a specific error type.
	ErrorNumber int `json:"error_number" xml:"ErrorNumber"`

	// Severity is the error severity (equivalent to ERROR_SEVERITY()).
	// In Go, this is typically mapped: 0=info, 10=warning, 16=error, 20+=critical.
	Severity int `json:"severity" xml:"Severity"`

	// State is the error state (equivalent to ERROR_STATE()).
	State int `json:"state" xml:"State"`

	// Line is the approximate line number where the error occurred.
	Line int `json:"line" xml:"Line"`

	// Timestamp is when the error occurred.
	Timestamp time.Time `json:"timestamp" xml:"Timestamp"`

	// StackTrace contains the Go stack trace if available.
	StackTrace string `json:"stack_trace,omitempty" xml:"StackTrace,omitempty"`

	// RecoveredValue is the raw value from recover() if this came from a panic.
	RecoveredValue interface{} `json:"-" xml:"-"`
}

// ToXML returns the error as XML, matching the T-SQL FOR XML PATH pattern.
func (e SPError) ToXML() string {
	var sb strings.Builder
	sb.WriteString("<SPError>")
	sb.WriteString(fmt.Sprintf("<ProcedureName>%s</ProcedureName>", escapeXMLText(e.ProcedureName)))
	sb.WriteString("<Parameters>")
	for k, v := range e.Parameters {
		sb.WriteString(fmt.Sprintf("<%s>%v</%s>", escapeXMLText(k), v, escapeXMLText(k)))
	}
	sb.WriteString("</Parameters>")
	sb.WriteString(fmt.Sprintf("<ErrorMessage>%s</ErrorMessage>", escapeXMLText(e.ErrorMessage)))
	sb.WriteString(fmt.Sprintf("<ErrorNumber>%d</ErrorNumber>", e.ErrorNumber))
	sb.WriteString(fmt.Sprintf("<Severity>%d</Severity>", e.Severity))
	sb.WriteString(fmt.Sprintf("<State>%d</State>", e.State))
	sb.WriteString(fmt.Sprintf("<Line>%d</Line>", e.Line))
	sb.WriteString(fmt.Sprintf("<Timestamp>%s</Timestamp>", e.Timestamp.Format(time.RFC3339)))
	if e.StackTrace != "" {
		sb.WriteString(fmt.Sprintf("<StackTrace>%s</StackTrace>", escapeXMLText(e.StackTrace)))
	}
	sb.WriteString("</SPError>")
	return sb.String()
}

// ToJSON returns the error as JSON.
func (e SPError) ToJSON() string {
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf(`{"error":"failed to marshal: %v"}`, err)
	}
	return string(b)
}

// escapeXMLText escapes special XML characters in text.
func escapeXMLText(s string) string {
	var sb strings.Builder
	xml.EscapeText(&sb, []byte(s))
	return sb.String()
}

// SPLogger is the interface for logging stored procedure errors.
type SPLogger interface {
	// LogError logs an error from a CATCH block.
	LogError(ctx context.Context, err SPError) error

	// LogEntry logs procedure entry (optional, for tracing).
	LogEntry(ctx context.Context, procName string, params map[string]interface{})

	// LogExit logs procedure exit (optional, for tracing).
	LogExit(ctx context.Context, procName string, duration time.Duration, err error)
}

// CaptureError creates an SPError from a recovered panic value.
// This is the primary helper for use in generated CATCH blocks.
func CaptureError(procName string, recovered interface{}, params map[string]interface{}) SPError {
	_, _, line, _ := runtime.Caller(1)

	// Build a short stack trace
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])

	return SPError{
		ProcedureName:  procName,
		Parameters:     params,
		ErrorMessage:   fmt.Sprintf("%v", recovered),
		ErrorNumber:    0,
		Severity:       16, // Error level
		State:          1,
		Line:           line,
		Timestamp:      time.Now(),
		StackTrace:     stack,
		RecoveredValue: recovered,
	}
}

// CaptureErrorWithCaller creates an SPError with a custom caller skip level.
func CaptureErrorWithCaller(procName string, recovered interface{}, params map[string]interface{}, skip int) SPError {
	_, _, line, _ := runtime.Caller(skip)

	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])

	return SPError{
		ProcedureName:  procName,
		Parameters:     params,
		ErrorMessage:   fmt.Sprintf("%v", recovered),
		ErrorNumber:    0,
		Severity:       16,
		State:          1,
		Line:           line,
		Timestamp:      time.Now(),
		StackTrace:     stack,
		RecoveredValue: recovered,
	}
}

// =============================================================================
// DatabaseSPLogger - Logs to a database table
// =============================================================================

// DatabaseSPLogger logs stored procedure errors to a database table,
// matching the common T-SQL pattern of INSERT INTO ErrorLog in CATCH blocks.
type DatabaseSPLogger struct {
	db        *sql.DB
	tableName string
	dialect   string // postgres, mysql, sqlserver, sqlite

	// Column mappings (customisable)
	columns DatabaseLoggerColumns
}

// DatabaseLoggerColumns defines the column names for the error log table.
type DatabaseLoggerColumns struct {
	ProcedureName string
	Parameters    string
	ErrorMessage  string
	ErrorNumber   string
	Severity      string
	State         string
	Line          string
	Timestamp     string
	StackTrace    string
}

// DefaultDatabaseLoggerColumns returns the default column names.
func DefaultDatabaseLoggerColumns() DatabaseLoggerColumns {
	return DatabaseLoggerColumns{
		ProcedureName: "StoreProcedure",
		Parameters:    "XmlParameters",
		ErrorMessage:  "Message",
		ErrorNumber:   "Number",
		Severity:      "Severity",
		State:         "State",
		Line:          "Line",
		Timestamp:     "ErrorDate",
		StackTrace:    "StackTrace",
	}
}

// NewDatabaseSPLogger creates a new database logger.
func NewDatabaseSPLogger(db *sql.DB, tableName, dialect string) *DatabaseSPLogger {
	return &DatabaseSPLogger{
		db:        db,
		tableName: tableName,
		dialect:   dialect,
		columns:   DefaultDatabaseLoggerColumns(),
	}
}

// WithColumns sets custom column names.
func (l *DatabaseSPLogger) WithColumns(cols DatabaseLoggerColumns) *DatabaseSPLogger {
	l.columns = cols
	return l
}

// LogError inserts the error into the database table.
func (l *DatabaseSPLogger) LogError(ctx context.Context, err SPError) error {
	query := l.buildInsertQuery()
	args := l.buildArgs(err)

	_, execErr := l.db.ExecContext(ctx, query, args...)
	return execErr
}

// LogEntry is a no-op for the database logger (can be extended).
func (l *DatabaseSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
	// Optional: could insert entry record for tracing
}

// LogExit is a no-op for the database logger (can be extended).
func (l *DatabaseSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
	// Optional: could update/insert exit record for tracing
}

func (l *DatabaseSPLogger) buildInsertQuery() string {
	cols := []string{
		l.columns.ProcedureName,
		l.columns.Parameters,
		l.columns.ErrorMessage,
		l.columns.ErrorNumber,
		l.columns.Severity,
		l.columns.State,
		l.columns.Line,
		l.columns.Timestamp,
	}

	// Add StackTrace if column is defined
	if l.columns.StackTrace != "" {
		cols = append(cols, l.columns.StackTrace)
	}

	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = l.placeholder(i + 1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		l.tableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "))
}

func (l *DatabaseSPLogger) placeholder(n int) string {
	switch l.dialect {
	case "postgres":
		return fmt.Sprintf("$%d", n)
	case "sqlserver":
		return fmt.Sprintf("@p%d", n)
	case "oracle":
		return fmt.Sprintf(":p%d", n)
	default: // mysql, sqlite
		return "?"
	}
}

func (l *DatabaseSPLogger) buildArgs(err SPError) []interface{} {
	args := []interface{}{
		err.ProcedureName,
		err.ToXML(), // Parameters as XML
		err.ErrorMessage,
		err.ErrorNumber,
		err.Severity,
		err.State,
		err.Line,
		err.Timestamp,
	}

	if l.columns.StackTrace != "" {
		args = append(args, err.StackTrace)
	}

	return args
}

// =============================================================================
// SlogSPLogger - Uses Go's structured logging (slog)
// =============================================================================

// SlogSPLogger logs stored procedure errors using Go's slog package.
type SlogSPLogger struct {
	logger *slog.Logger
}

// NewSlogSPLogger creates a new slog-based logger.
func NewSlogSPLogger(logger *slog.Logger) *SlogSPLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogSPLogger{logger: logger}
}

// NewSlogSPLoggerWithHandler creates a logger with a custom handler.
func NewSlogSPLoggerWithHandler(handler slog.Handler) *SlogSPLogger {
	return &SlogSPLogger{logger: slog.New(handler)}
}

// LogError logs the error using slog.
func (l *SlogSPLogger) LogError(ctx context.Context, err SPError) error {
	l.logger.ErrorContext(ctx, "stored procedure error",
		slog.String("procedure", err.ProcedureName),
		slog.Any("parameters", err.Parameters),
		slog.String("message", err.ErrorMessage),
		slog.Int("error_number", err.ErrorNumber),
		slog.Int("severity", err.Severity),
		slog.Int("state", err.State),
		slog.Int("line", err.Line),
		slog.Time("timestamp", err.Timestamp),
	)
	return nil
}

// LogEntry logs procedure entry.
func (l *SlogSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
	l.logger.DebugContext(ctx, "stored procedure entry",
		slog.String("procedure", procName),
		slog.Any("parameters", params),
	)
}

// LogExit logs procedure exit.
func (l *SlogSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
	if err != nil {
		l.logger.ErrorContext(ctx, "stored procedure exit with error",
			slog.String("procedure", procName),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
	} else {
		l.logger.DebugContext(ctx, "stored procedure exit",
			slog.String("procedure", procName),
			slog.Duration("duration", duration),
		)
	}
}

// =============================================================================
// MultiSPLogger - Logs to multiple destinations
// =============================================================================

// MultiSPLogger logs to multiple loggers simultaneously.
type MultiSPLogger struct {
	loggers []SPLogger
}

// NewMultiSPLogger creates a logger that writes to multiple destinations.
func NewMultiSPLogger(loggers ...SPLogger) *MultiSPLogger {
	return &MultiSPLogger{loggers: loggers}
}

// LogError logs to all configured loggers.
func (l *MultiSPLogger) LogError(ctx context.Context, err SPError) error {
	var firstErr error
	for _, logger := range l.loggers {
		if e := logger.LogError(ctx, err); e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}

// LogEntry logs to all configured loggers.
func (l *MultiSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
	for _, logger := range l.loggers {
		logger.LogEntry(ctx, procName, params)
	}
}

// LogExit logs to all configured loggers.
func (l *MultiSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
	for _, logger := range l.loggers {
		logger.LogExit(ctx, procName, duration, err)
	}
}

// =============================================================================
// BufferedSPLogger - Buffers errors for batch insert
// =============================================================================

// BufferedSPLogger buffers errors and flushes them in batches.
type BufferedSPLogger struct {
	inner     SPLogger
	buffer    []SPError
	bufferMu  sync.Mutex
	batchSize int
	flushChan chan struct{}
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// NewBufferedSPLogger creates a buffered logger.
func NewBufferedSPLogger(inner SPLogger, batchSize int, flushInterval time.Duration) *BufferedSPLogger {
	l := &BufferedSPLogger{
		inner:     inner,
		buffer:    make([]SPError, 0, batchSize),
		batchSize: batchSize,
		flushChan: make(chan struct{}, 1),
		closeChan: make(chan struct{}),
	}

	l.wg.Add(1)
	go l.flushLoop(flushInterval)

	return l
}

// LogError adds the error to the buffer.
func (l *BufferedSPLogger) LogError(ctx context.Context, err SPError) error {
	l.bufferMu.Lock()
	l.buffer = append(l.buffer, err)
	shouldFlush := len(l.buffer) >= l.batchSize
	l.bufferMu.Unlock()

	if shouldFlush {
		select {
		case l.flushChan <- struct{}{}:
		default:
		}
	}

	return nil
}

// LogEntry delegates to the inner logger.
func (l *BufferedSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
	l.inner.LogEntry(ctx, procName, params)
}

// LogExit delegates to the inner logger.
func (l *BufferedSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
	l.inner.LogExit(ctx, procName, duration, err)
}

// Flush immediately flushes all buffered errors.
func (l *BufferedSPLogger) Flush(ctx context.Context) error {
	l.bufferMu.Lock()
	errors := l.buffer
	l.buffer = make([]SPError, 0, l.batchSize)
	l.bufferMu.Unlock()

	var firstErr error
	for _, err := range errors {
		if e := l.inner.LogError(ctx, err); e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}

// Close flushes remaining errors and stops the flush loop.
func (l *BufferedSPLogger) Close(ctx context.Context) error {
	close(l.closeChan)
	l.wg.Wait()
	return l.Flush(ctx)
}

func (l *BufferedSPLogger) flushLoop(interval time.Duration) {
	defer l.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.closeChan:
			return
		case <-ticker.C:
			_ = l.Flush(context.Background())
		case <-l.flushChan:
			_ = l.Flush(context.Background())
		}
	}
}

// =============================================================================
// NopSPLogger - No-op logger for testing/disabled logging
// =============================================================================

// NopSPLogger is a no-op logger that discards all log entries.
type NopSPLogger struct{}

// NewNopSPLogger creates a no-op logger.
func NewNopSPLogger() *NopSPLogger {
	return &NopSPLogger{}
}

// LogError does nothing.
func (l *NopSPLogger) LogError(ctx context.Context, err SPError) error {
	return nil
}

// LogEntry does nothing.
func (l *NopSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
}

// LogExit does nothing.
func (l *NopSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
}

// =============================================================================
// FileSPLogger - Logs to a file
// =============================================================================

// FileSPLogger logs errors to a file in JSON or text format.
type FileSPLogger struct {
	file   *os.File
	mu     sync.Mutex
	format string // "json" or "text"
}

// NewFileSPLogger creates a file-based logger.
func NewFileSPLogger(path string, format string) (*FileSPLogger, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	if format != "json" && format != "text" {
		format = "json"
	}
	return &FileSPLogger{file: f, format: format}, nil
}

// LogError writes the error to the file.
func (l *FileSPLogger) LogError(ctx context.Context, err SPError) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var line string
	if l.format == "json" {
		line = err.ToJSON() + "\n"
	} else {
		line = fmt.Sprintf("[%s] %s: %s (params: %v)\n",
			err.Timestamp.Format(time.RFC3339),
			err.ProcedureName,
			err.ErrorMessage,
			err.Parameters)
	}

	_, writeErr := l.file.WriteString(line)
	return writeErr
}

// LogEntry writes entry to the file.
func (l *FileSPLogger) LogEntry(ctx context.Context, procName string, params map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.file.WriteString(fmt.Sprintf("[%s] ENTRY %s: %v\n", time.Now().Format(time.RFC3339), procName, params))
}

// LogExit writes exit to the file.
func (l *FileSPLogger) LogExit(ctx context.Context, procName string, duration time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.file.WriteString(fmt.Sprintf("[%s] EXIT %s: duration=%v error=%v\n", time.Now().Format(time.RFC3339), procName, duration, err))
	} else {
		l.file.WriteString(fmt.Sprintf("[%s] EXIT %s: duration=%v\n", time.Now().Format(time.RFC3339), procName, duration))
	}
}

// Close closes the file.
func (l *FileSPLogger) Close() error {
	return l.file.Close()
}

// =============================================================================
// Global default logger
// =============================================================================

var (
	defaultLogger   SPLogger = NewNopSPLogger()
	defaultLoggerMu sync.RWMutex
)

// SetDefaultSPLogger sets the global default logger.
func SetDefaultSPLogger(logger SPLogger) {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	defaultLogger = logger
}

// GetDefaultSPLogger returns the global default logger.
func GetDefaultSPLogger() SPLogger {
	defaultLoggerMu.RLock()
	defer defaultLoggerMu.RUnlock()
	return defaultLogger
}

// LogSPError logs an error using the default logger.
func LogSPError(ctx context.Context, err SPError) error {
	return GetDefaultSPLogger().LogError(ctx, err)
}
