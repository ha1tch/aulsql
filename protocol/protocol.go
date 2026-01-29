// Package protocol provides pluggable network protocol implementations for aul.
//
// Each protocol (TDS, PostgreSQL wire protocol, MySQL protocol, HTTP/REST, gRPC)
// implements the Listener and Connection interfaces, allowing the server to
// accept connections from various database clients.
package protocol

import (
	"fmt"
	"net"
	"time"

	"github.com/ha1tch/aul/pkg/log"
)

// ProtocolType identifies a wire protocol.
type ProtocolType string

const (
	ProtocolTDS      ProtocolType = "tds"      // SQL Server Tabular Data Stream
	ProtocolPostgres ProtocolType = "postgres" // PostgreSQL wire protocol
	ProtocolMySQL    ProtocolType = "mysql"    // MySQL wire protocol
	ProtocolHTTP     ProtocolType = "http"     // HTTP/REST API
	ProtocolGRPC     ProtocolType = "grpc"     // gRPC
)

func (p ProtocolType) String() string {
	return string(p)
}

// DefaultPort returns the default port for a protocol.
func (p ProtocolType) DefaultPort() int {
	switch p {
	case ProtocolTDS:
		return 1433
	case ProtocolPostgres:
		return 5432
	case ProtocolMySQL:
		return 3306
	case ProtocolHTTP:
		return 8080
	case ProtocolGRPC:
		return 50051
	default:
		return 0
	}
}

// Listener accepts client connections for a specific protocol.
type Listener interface {
	// Protocol returns the protocol type.
	Protocol() ProtocolType

	// Listen starts listening on the configured address.
	Listen() error

	// Accept waits for and returns the next connection.
	Accept() (Connection, error)

	// Close stops the listener.
	Close() error

	// Addr returns the listener's network address.
	Addr() net.Addr

	// ConnectionCount returns the number of active connections.
	ConnectionCount() int
}

// Connection represents a client connection.
type Connection interface {
	// ReadRequest reads the next request from the client.
	ReadRequest() (Request, error)

	// SendResult sends a result to the client.
	SendResult(result Result) error

	// Close closes the connection.
	Close() error

	// RemoteAddr returns the remote address.
	RemoteAddr() net.Addr

	// SetDeadline sets the read/write deadline.
	SetDeadline(t time.Time) error

	// Properties returns connection properties (e.g., TDS login properties).
	// Used for tenant identification and other session metadata.
	Properties() map[string]string
}

// ListenerConfig configures a protocol listener.
type ListenerConfig struct {
	// Listener identification
	Name     string
	Protocol ProtocolType

	// Network configuration
	Host string
	Port int

	// TLS configuration
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string

	// Connection limits
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration

	// Protocol-specific options
	Options map[string]interface{}
}

// DefaultListenerConfig returns a ListenerConfig with sensible defaults.
func DefaultListenerConfig(proto ProtocolType) ListenerConfig {
	return ListenerConfig{
		Name:           string(proto),
		Protocol:       proto,
		Host:           "0.0.0.0",
		Port:           proto.DefaultPort(),
		MaxConnections: 1000,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    5 * time.Minute,
		Options:        make(map[string]interface{}),
	}
}

// Address returns the full listen address.
func (c ListenerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// RequestType identifies the type of client request.
type RequestType int

const (
	RequestUnknown RequestType = iota
	RequestExec                // Execute stored procedure
	RequestQuery               // Execute ad-hoc SQL
	RequestPrepare             // Prepare a statement
	RequestCall                // Call procedure (with result sets)
	RequestBeginTxn            // Begin transaction
	RequestCommit              // Commit transaction
	RequestRollback            // Rollback transaction
	RequestPing                // Connection health check
	RequestCancel              // Cancel running query
	RequestClose               // Close prepared statement
)

func (r RequestType) String() string {
	switch r {
	case RequestExec:
		return "EXEC"
	case RequestQuery:
		return "QUERY"
	case RequestPrepare:
		return "PREPARE"
	case RequestCall:
		return "CALL"
	case RequestBeginTxn:
		return "BEGIN"
	case RequestCommit:
		return "COMMIT"
	case RequestRollback:
		return "ROLLBACK"
	case RequestPing:
		return "PING"
	case RequestCancel:
		return "CANCEL"
	case RequestClose:
		return "CLOSE"
	default:
		return "UNKNOWN"
	}
}

// Request represents a client request.
type Request struct {
	Type          RequestType
	SQL           string                 // For ad-hoc queries
	ProcedureName string                 // For EXEC/CALL
	Parameters    map[string]interface{} // Named parameters
	Options       RequestOptions
}

// RequestOptions holds optional request settings.
type RequestOptions struct {
	Timeout       time.Duration
	NoCount       bool   // Suppress row count messages
	RowsToFetch   int    // Limit rows returned
	CursorType    string // Cursor type for scrollable results
	StatementID   string // For prepared statements
}

// ResultType identifies the type of result.
type ResultType int

const (
	ResultOK ResultType = iota
	ResultError
	ResultRows
	ResultInfo
	ResultWarning
	ResultCancel // Query was cancelled via attention
)

func (r ResultType) String() string {
	switch r {
	case ResultOK:
		return "OK"
	case ResultError:
		return "ERROR"
	case ResultRows:
		return "ROWS"
	case ResultInfo:
		return "INFO"
	case ResultWarning:
		return "WARNING"
	case ResultCancel:
		return "CANCEL"
	default:
		return "UNKNOWN"
	}
}

// Result represents a result sent to the client.
type Result struct {
	Type         ResultType
	Error        error
	Message      string
	RowsAffected int64
	ResultSets   []ResultSet
	ReturnValue  interface{}
	OutputParams map[string]interface{}
}

// ResultSet represents a tabular result set.
type ResultSet struct {
	Columns []ColumnInfo
	Rows    [][]interface{}
}

// ColumnInfo describes a column in a result set.
type ColumnInfo struct {
	Name     string
	Type     string // SQL type name
	GoType   string // Go type name
	Nullable bool
	Length   int
	Scale    int
	Ordinal  int
}

// NewListener creates a listener for the specified protocol.
func NewListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	switch cfg.Protocol {
	case ProtocolTDS:
		return newTDSListener(cfg, logger)
	case ProtocolPostgres:
		return newPostgresListener(cfg, logger)
	case ProtocolMySQL:
		return newMySQLListener(cfg, logger)
	case ProtocolHTTP:
		return newHTTPListener(cfg, logger)
	case ProtocolGRPC:
		return newGRPCListener(cfg, logger)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}
}

// Placeholder implementations - each will be in its own file

func newTDSListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	if tdsListenerFactory == nil {
		return nil, fmt.Errorf("TDS protocol not registered")
	}
	return tdsListenerFactory(cfg, logger)
}

func newPostgresListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	// Import cycle prevention: use a factory function set by postgres package
	if postgresListenerFactory == nil {
		return nil, fmt.Errorf("PostgreSQL protocol not registered")
	}
	return postgresListenerFactory(cfg, logger)
}

func newMySQLListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	return nil, fmt.Errorf("MySQL protocol not yet implemented")
}

func newHTTPListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	// Import cycle prevention: use a factory function set by http package
	if httpListenerFactory == nil {
		return nil, fmt.Errorf("HTTP protocol not registered")
	}
	return httpListenerFactory(cfg, logger)
}

func newGRPCListener(cfg ListenerConfig, logger *log.Logger) (Listener, error) {
	return nil, fmt.Errorf("gRPC protocol not yet implemented")
}

// ListenerFactory is a function that creates a new listener.
type ListenerFactory func(cfg ListenerConfig, logger *log.Logger) (Listener, error)

var (
	tdsListenerFactory      ListenerFactory
	postgresListenerFactory ListenerFactory
	httpListenerFactory     ListenerFactory
)

// RegisterTDSFactory registers the TDS listener factory.
func RegisterTDSFactory(f ListenerFactory) {
	tdsListenerFactory = f
}

// RegisterPostgresFactory registers the PostgreSQL listener factory.
func RegisterPostgresFactory(f ListenerFactory) {
	postgresListenerFactory = f
}

// RegisterHTTPFactory registers the HTTP listener factory.
func RegisterHTTPFactory(f ListenerFactory) {
	httpListenerFactory = f
}
