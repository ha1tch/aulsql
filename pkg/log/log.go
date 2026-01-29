// Package log provides structured logging for aul.
//
// The logging system supports multiple categories:
//   - System: Server lifecycle, configuration, resource management
//   - Execution: Procedure calls, query execution, JIT compilation
//   - Application: Business logic, procedure loading, protocol handling
//   - Audit: Security-relevant events (authentication, authorisation)
//   - Performance: Timing, throughput, resource utilisation
//
// Each category can be configured independently with its own level and output.
package log

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Level represents a logging severity level.
type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelOff // Disable logging entirely
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	case LevelOff:
		return "OFF"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a level string.
func ParseLevel(s string) (Level, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return LevelDebug, nil
	case "INFO":
		return LevelInfo, nil
	case "WARN", "WARNING":
		return LevelWarn, nil
	case "ERROR", "ERR":
		return LevelError, nil
	case "FATAL":
		return LevelFatal, nil
	case "OFF", "NONE":
		return LevelOff, nil
	default:
		return LevelInfo, fmt.Errorf("unknown log level: %s", s)
	}
}

// Category identifies the logging category.
type Category string

const (
	CategorySystem      Category = "system"      // Server lifecycle, config, resources
	CategoryExecution   Category = "execution"   // Procedure/query execution
	CategoryApplication Category = "application" // Business logic, protocol handling
	CategoryAudit       Category = "audit"       // Security events
	CategoryPerformance Category = "performance" // Timing and metrics
)

// Format specifies the output format.
type Format int

const (
	FormatText Format = iota // Human-readable text
	FormatJSON               // Structured JSON
)

// Entry represents a single log entry.
type Entry struct {
	Time      time.Time              `json:"time"`
	Level     Level                  `json:"level"`
	Category  Category               `json:"category"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Error     error                  `json:"-"`
	ErrorStr  string                 `json:"error,omitempty"`
	Caller    string                 `json:"caller,omitempty"`
	RequestID string                 `json:"request_id,omitempty"`
	SessionID string                 `json:"session_id,omitempty"`
}

// Logger is the main logging interface.
type Logger struct {
	mu sync.RWMutex

	// Per-category configuration
	levels  map[Category]Level
	outputs map[Category]io.Writer

	// Global settings
	format       Format
	includeCaller bool
	minLevel     Level

	// Async writing
	asyncEnabled bool
	entryChan    chan *Entry
	wg           sync.WaitGroup
	closed       int32

	// Metrics
	entriesLogged int64
	entriesDropped int64
}

// Config holds logger configuration.
type Config struct {
	// Default level for all categories
	DefaultLevel Level

	// Per-category level overrides
	CategoryLevels map[Category]Level

	// Output configuration
	Output io.Writer // Default output (os.Stderr if nil)
	Format Format

	// Optional features
	IncludeCaller bool // Include file:line in log entries
	AsyncBuffer   int  // Async buffer size (0 = sync logging)
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() Config {
	return Config{
		DefaultLevel:  LevelInfo,
		Output:        os.Stderr,
		Format:        FormatText,
		IncludeCaller: false,
		AsyncBuffer:   0,
	}
}

// New creates a new logger with the given configuration.
func New(cfg Config) *Logger {
	if cfg.Output == nil {
		cfg.Output = os.Stderr
	}

	l := &Logger{
		levels:        make(map[Category]Level),
		outputs:       make(map[Category]io.Writer),
		format:        cfg.Format,
		includeCaller: cfg.IncludeCaller,
		minLevel:      cfg.DefaultLevel,
	}

	// Set default level for all categories
	categories := []Category{
		CategorySystem,
		CategoryExecution,
		CategoryApplication,
		CategoryAudit,
		CategoryPerformance,
	}
	for _, cat := range categories {
		l.levels[cat] = cfg.DefaultLevel
		l.outputs[cat] = cfg.Output
	}

	// Apply per-category overrides
	for cat, level := range cfg.CategoryLevels {
		l.levels[cat] = level
	}

	// Set up async logging if configured
	if cfg.AsyncBuffer > 0 {
		l.asyncEnabled = true
		l.entryChan = make(chan *Entry, cfg.AsyncBuffer)
		l.wg.Add(1)
		go l.asyncWriter()
	}

	return l
}

// SetLevel sets the log level for a category.
func (l *Logger) SetLevel(cat Category, level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.levels[cat] = level
}

// SetOutput sets the output writer for a category.
func (l *Logger) SetOutput(cat Category, w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outputs[cat] = w
}

// SetFormat sets the output format.
func (l *Logger) SetFormat(f Format) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.format = f
}

// Close shuts down the logger, flushing any buffered entries.
func (l *Logger) Close() error {
	if !l.asyncEnabled {
		return nil
	}

	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return nil // Already closed
	}

	close(l.entryChan)
	l.wg.Wait()
	return nil
}

// Stats returns logging statistics.
func (l *Logger) Stats() (logged, dropped int64) {
	return atomic.LoadInt64(&l.entriesLogged), atomic.LoadInt64(&l.entriesDropped)
}

// Log logs an entry at the specified level and category.
func (l *Logger) Log(level Level, cat Category, msg string, fields ...interface{}) {
	l.log(level, cat, msg, nil, fields...)
}

// LogError logs an entry with an associated error.
func (l *Logger) LogError(level Level, cat Category, msg string, err error, fields ...interface{}) {
	l.log(level, cat, msg, err, fields...)
}

// Convenience methods for each level

func (l *Logger) Debug(cat Category, msg string, fields ...interface{}) {
	l.log(LevelDebug, cat, msg, nil, fields...)
}

func (l *Logger) Info(cat Category, msg string, fields ...interface{}) {
	l.log(LevelInfo, cat, msg, nil, fields...)
}

func (l *Logger) Warn(cat Category, msg string, fields ...interface{}) {
	l.log(LevelWarn, cat, msg, nil, fields...)
}

func (l *Logger) Error(cat Category, msg string, err error, fields ...interface{}) {
	l.log(LevelError, cat, msg, err, fields...)
}

func (l *Logger) Fatal(cat Category, msg string, err error, fields ...interface{}) {
	l.log(LevelFatal, cat, msg, err, fields...)
}

// Category-specific loggers

// System returns a category logger for system events.
func (l *Logger) System() *CategoryLogger {
	return &CategoryLogger{logger: l, category: CategorySystem}
}

// Execution returns a category logger for execution events.
func (l *Logger) Execution() *CategoryLogger {
	return &CategoryLogger{logger: l, category: CategoryExecution}
}

// Application returns a category logger for application events.
func (l *Logger) Application() *CategoryLogger {
	return &CategoryLogger{logger: l, category: CategoryApplication}
}

// Audit returns a category logger for audit events.
func (l *Logger) Audit() *CategoryLogger {
	return &CategoryLogger{logger: l, category: CategoryAudit}
}

// Performance returns a category logger for performance events.
func (l *Logger) Performance() *CategoryLogger {
	return &CategoryLogger{logger: l, category: CategoryPerformance}
}

// log is the internal logging implementation.
func (l *Logger) log(level Level, cat Category, msg string, err error, fields ...interface{}) {
	l.mu.RLock()
	catLevel := l.levels[cat]
	output := l.outputs[cat]
	format := l.format
	includeCaller := l.includeCaller
	l.mu.RUnlock()

	// Check if this level is enabled
	if level < catLevel {
		return
	}

	entry := &Entry{
		Time:     time.Now(),
		Level:    level,
		Category: cat,
		Message:  msg,
		Error:    err,
	}

	if err != nil {
		entry.ErrorStr = err.Error()
	}

	// Parse fields (key-value pairs)
	if len(fields) > 0 {
		entry.Fields = make(map[string]interface{})
		for i := 0; i < len(fields)-1; i += 2 {
			if key, ok := fields[i].(string); ok {
				entry.Fields[key] = fields[i+1]
			}
		}
	}

	// Add caller information if enabled
	if includeCaller {
		if _, file, line, ok := runtime.Caller(3); ok {
			// Trim to just filename
			if idx := strings.LastIndex(file, "/"); idx >= 0 {
				file = file[idx+1:]
			}
			entry.Caller = fmt.Sprintf("%s:%d", file, line)
		}
	}

	// Write entry
	if l.asyncEnabled && atomic.LoadInt32(&l.closed) == 0 {
		select {
		case l.entryChan <- entry:
			atomic.AddInt64(&l.entriesLogged, 1)
		default:
			atomic.AddInt64(&l.entriesDropped, 1)
		}
	} else {
		l.writeEntry(output, format, entry)
		atomic.AddInt64(&l.entriesLogged, 1)
	}
}

// writeEntry formats and writes an entry.
func (l *Logger) writeEntry(w io.Writer, format Format, entry *Entry) {
	var line string

	switch format {
	case FormatJSON:
		data, _ := json.Marshal(entry)
		line = string(data) + "\n"
	default:
		line = l.formatText(entry)
	}

	w.Write([]byte(line))
}

// formatText formats an entry as human-readable text.
func (l *Logger) formatText(entry *Entry) string {
	var buf strings.Builder

	// Timestamp
	buf.WriteString(entry.Time.Format("2006-01-02 15:04:05.000"))
	buf.WriteString(" ")

	// Level with padding
	buf.WriteString(fmt.Sprintf("%-5s", entry.Level.String()))
	buf.WriteString(" ")

	// Category
	buf.WriteString("[")
	buf.WriteString(string(entry.Category))
	buf.WriteString("] ")

	// Caller if present
	if entry.Caller != "" {
		buf.WriteString(entry.Caller)
		buf.WriteString(" ")
	}

	// Message
	buf.WriteString(entry.Message)

	// Error if present
	if entry.ErrorStr != "" {
		buf.WriteString(" error=\"")
		buf.WriteString(entry.ErrorStr)
		buf.WriteString("\"")
	}

	// Fields
	if len(entry.Fields) > 0 {
		for k, v := range entry.Fields {
			buf.WriteString(" ")
			buf.WriteString(k)
			buf.WriteString("=")
			buf.WriteString(fmt.Sprintf("%v", v))
		}
	}

	buf.WriteString("\n")
	return buf.String()
}

// asyncWriter processes the entry channel.
func (l *Logger) asyncWriter() {
	defer l.wg.Done()

	for entry := range l.entryChan {
		l.mu.RLock()
		output := l.outputs[entry.Category]
		format := l.format
		l.mu.RUnlock()

		l.writeEntry(output, format, entry)
	}
}

// CategoryLogger is a logger bound to a specific category.
type CategoryLogger struct {
	logger   *Logger
	category Category
}

func (cl *CategoryLogger) Debug(msg string, fields ...interface{}) {
	cl.logger.log(LevelDebug, cl.category, msg, nil, fields...)
}

func (cl *CategoryLogger) Info(msg string, fields ...interface{}) {
	cl.logger.log(LevelInfo, cl.category, msg, nil, fields...)
}

func (cl *CategoryLogger) Warn(msg string, fields ...interface{}) {
	cl.logger.log(LevelWarn, cl.category, msg, nil, fields...)
}

func (cl *CategoryLogger) Error(msg string, err error, fields ...interface{}) {
	cl.logger.log(LevelError, cl.category, msg, err, fields...)
}

func (cl *CategoryLogger) Fatal(msg string, err error, fields ...interface{}) {
	cl.logger.log(LevelFatal, cl.category, msg, err, fields...)
}

// WithFields returns a FieldLogger with preset fields.
func (cl *CategoryLogger) WithFields(fields ...interface{}) *FieldLogger {
	return &FieldLogger{
		categoryLogger: cl,
		fields:         fields,
	}
}

// FieldLogger is a category logger with preset fields.
type FieldLogger struct {
	categoryLogger *CategoryLogger
	fields         []interface{}
}

func (fl *FieldLogger) Debug(msg string, extraFields ...interface{}) {
	fl.categoryLogger.logger.log(LevelDebug, fl.categoryLogger.category, msg, nil, append(fl.fields, extraFields...)...)
}

func (fl *FieldLogger) Info(msg string, extraFields ...interface{}) {
	fl.categoryLogger.logger.log(LevelInfo, fl.categoryLogger.category, msg, nil, append(fl.fields, extraFields...)...)
}

func (fl *FieldLogger) Warn(msg string, extraFields ...interface{}) {
	fl.categoryLogger.logger.log(LevelWarn, fl.categoryLogger.category, msg, nil, append(fl.fields, extraFields...)...)
}

func (fl *FieldLogger) Error(msg string, err error, extraFields ...interface{}) {
	fl.categoryLogger.logger.log(LevelError, fl.categoryLogger.category, msg, err, append(fl.fields, extraFields...)...)
}

// Context keys for request/session tracking
type contextKey int

const (
	contextKeyRequestID contextKey = iota
	contextKeySessionID
	contextKeyLogger
)

// WithRequestID adds a request ID to the context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, contextKeyRequestID, requestID)
}

// WithSessionID adds a session ID to the context.
func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, contextKeySessionID, sessionID)
}

// WithLogger adds a logger to the context.
func WithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, logger)
}

// FromContext retrieves the logger from context, or returns the default logger.
func FromContext(ctx context.Context) *Logger {
	if l, ok := ctx.Value(contextKeyLogger).(*Logger); ok {
		return l
	}
	return defaultLogger
}

// RequestIDFromContext retrieves the request ID from context.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(contextKeyRequestID).(string); ok {
		return id
	}
	return ""
}

// SessionIDFromContext retrieves the session ID from context.
func SessionIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(contextKeySessionID).(string); ok {
		return id
	}
	return ""
}

// Default logger instance
var (
	defaultLogger     *Logger
	defaultLoggerOnce sync.Once
)

// Default returns the default logger instance.
func Default() *Logger {
	defaultLoggerOnce.Do(func() {
		defaultLogger = New(DefaultConfig())
	})
	return defaultLogger
}

// SetDefault sets the default logger instance.
func SetDefault(l *Logger) {
	defaultLogger = l
}

// Package-level convenience functions using default logger

func Debug(cat Category, msg string, fields ...interface{}) {
	Default().Debug(cat, msg, fields...)
}

func Info(cat Category, msg string, fields ...interface{}) {
	Default().Info(cat, msg, fields...)
}

func Warn(cat Category, msg string, fields ...interface{}) {
	Default().Warn(cat, msg, fields...)
}

func Error(cat Category, msg string, err error, fields ...interface{}) {
	Default().Error(cat, msg, err, fields...)
}

func Fatal(cat Category, msg string, err error, fields ...interface{}) {
	Default().Fatal(cat, msg, err, fields...)
}
