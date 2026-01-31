// Package postgres implements the PostgreSQL wire protocol (v3) for aul.
//
// This allows aul to accept connections from any PostgreSQL client (psql, pgAdmin,
// any language driver) and execute stored procedures through the aul runtime.
//
// The implementation uses jackc/pgx's pgproto3 for protocol encoding/decoding.
package postgres

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/pkg/protocol"
)

// Listener implements protocol.Listener for the PostgreSQL wire protocol.
type Listener struct {
	mu sync.RWMutex

	cfg      protocol.ListenerConfig
	logger   *log.Logger
	listener net.Listener

	// Connection tracking
	connections map[*Conn]struct{}
	connCount   int64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
}

// NewListener creates a new PostgreSQL protocol listener.
func NewListener(cfg protocol.ListenerConfig, logger *log.Logger) (*Listener, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Listener{
		cfg:         cfg,
		logger:      logger,
		connections: make(map[*Conn]struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Protocol returns the protocol type.
func (l *Listener) Protocol() protocol.ProtocolType {
	return protocol.ProtocolPostgres
}

// Listen starts listening on the configured address.
func (l *Listener) Listen() error {
	addr := l.cfg.Address()

	var err error
	if l.cfg.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(l.cfg.TLSCertFile, l.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("loading TLS certificate: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		l.listener, err = tls.Listen("tcp", addr, tlsCfg)
	} else {
		l.listener, err = net.Listen("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	return nil
}

// Accept waits for and returns the next connection.
func (l *Listener) Accept() (protocol.Connection, error) {
	if l.listener == nil {
		return nil, fmt.Errorf("listener not started")
	}

	netConn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}

	conn := newConn(netConn, l.cfg)

	// Perform PostgreSQL handshake
	if err := conn.handshake(l.ctx); err != nil {
		netConn.Close()
		return nil, fmt.Errorf("handshake failed: %w", err)
	}

	l.mu.Lock()
	l.connections[conn] = struct{}{}
	atomic.AddInt64(&l.connCount, 1)
	l.mu.Unlock()

	return conn, nil
}

// Close stops the listener.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	l.cancel()

	// Close all connections
	for conn := range l.connections {
		conn.Close()
	}

	if l.listener != nil {
		return l.listener.Close()
	}
	return nil
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	if l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

// ConnectionCount returns the number of active connections.
func (l *Listener) ConnectionCount() int {
	return int(atomic.LoadInt64(&l.connCount))
}

// removeConnection removes a connection from tracking.
func (l *Listener) removeConnection(conn *Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.connections, conn)
	atomic.AddInt64(&l.connCount, -1)
}

// Conn implements protocol.Connection for PostgreSQL.
type Conn struct {
	mu sync.Mutex

	netConn  net.Conn
	cfg      protocol.ListenerConfig
	backend  *pgproto3.Backend
	frontend *pgproto3.Frontend

	// Session state
	user     string
	database string
	params   map[string]string

	// State
	closed bool
}

// newConn creates a new PostgreSQL connection wrapper.
func newConn(netConn net.Conn, cfg protocol.ListenerConfig) *Conn {
	return &Conn{
		netConn: netConn,
		cfg:     cfg,
		backend: pgproto3.NewBackend(netConn, netConn),
		params:  make(map[string]string),
	}
}

// handshake performs the PostgreSQL startup handshake.
func (c *Conn) handshake(ctx context.Context) error {
	// Set deadline for handshake
	if c.cfg.ReadTimeout > 0 {
		c.netConn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		defer c.netConn.SetReadDeadline(time.Time{})
	}

	// Receive startup message
	startupMsg, err := c.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("receiving startup message: %w", err)
	}

	switch msg := startupMsg.(type) {
	case *pgproto3.StartupMessage:
		c.user = msg.Parameters["user"]
		c.database = msg.Parameters["database"]
		for k, v := range msg.Parameters {
			c.params[k] = v
		}

		// Send AuthenticationOk (no authentication for now)
		// In production, implement proper authentication here
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)

		// Send parameter status messages
		buf = (&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0.0 (aul)"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"}).Encode(buf)

		// Send BackendKeyData (process ID and secret key for cancel requests)
		buf = (&pgproto3.BackendKeyData{ProcessID: uint32(time.Now().UnixNano() & 0xFFFFFFFF), SecretKey: 0}).Encode(buf)

		// Send ReadyForQuery
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf) // 'I' = idle (not in transaction)

		_, err = c.netConn.Write(buf)
		return err

	case *pgproto3.SSLRequest:
		// Deny SSL for now (send 'N')
		// To support SSL, send 'S' and upgrade connection
		_, err := c.netConn.Write([]byte{'N'})
		if err != nil {
			return err
		}
		// Client should retry with regular startup
		return c.handshake(ctx)

	case *pgproto3.CancelRequest:
		// Cancel request - handle separately
		return fmt.Errorf("cancel request not supported")

	default:
		return fmt.Errorf("unexpected startup message type: %T", msg)
	}
}

// ReadRequest reads the next request from the client.
func (c *Conn) ReadRequest() (protocol.Request, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return protocol.Request{}, io.EOF
	}

	// Set read deadline
	if c.cfg.ReadTimeout > 0 {
		c.netConn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
	}

	msg, err := c.backend.Receive()
	if err != nil {
		return protocol.Request{}, err
	}

	switch m := msg.(type) {
	case *pgproto3.Query:
		return c.parseQuery(m.String)

	case *pgproto3.Parse:
		// Extended query protocol - Parse message
		return protocol.Request{
			Type: protocol.RequestPrepare,
			SQL:  m.Query,
			Options: protocol.RequestOptions{
				StatementID: m.Name,
			},
		}, nil

	case *pgproto3.Bind:
		// Extended query protocol - Bind message
		// Convert parameters
		params := make(map[string]interface{})
		for i, p := range m.Parameters {
			params[fmt.Sprintf("$%d", i+1)] = string(p)
		}
		return protocol.Request{
			Type:       protocol.RequestExec,
			Parameters: params,
			Options: protocol.RequestOptions{
				StatementID: m.PreparedStatement,
			},
		}, nil

	case *pgproto3.Execute:
		// Extended query protocol - Execute message
		return protocol.Request{
			Type: protocol.RequestExec,
			Options: protocol.RequestOptions{
				StatementID: m.Portal,
				RowsToFetch: int(m.MaxRows),
			},
		}, nil

	case *pgproto3.Describe:
		// Describe statement or portal
		return protocol.Request{
			Type: protocol.RequestQuery,
			SQL:  fmt.Sprintf("DESCRIBE %s", m.Name),
		}, nil

	case *pgproto3.Sync:
		// Sync - end of extended query
		return protocol.Request{
			Type: protocol.RequestPing,
		}, nil

	case *pgproto3.Terminate:
		c.closed = true
		return protocol.Request{}, io.EOF

	case *pgproto3.Close:
		return protocol.Request{
			Type: protocol.RequestClose,
			Options: protocol.RequestOptions{
				StatementID: m.Name,
			},
		}, nil

	default:
		return protocol.Request{}, fmt.Errorf("unsupported message type: %T", msg)
	}
}

// parseQuery parses a SQL query string into a Request.
func (c *Conn) parseQuery(sql string) (protocol.Request, error) {
	// Simple detection of query type
	// In a real implementation, use a proper SQL parser

	// Trim and uppercase for detection
	trimmed := sql
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n') {
		trimmed = trimmed[1:]
	}

	upper := ""
	for i := 0; i < len(trimmed) && i < 20; i++ {
		ch := trimmed[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		upper += string(ch)
	}

	switch {
	case startsWith(upper, "EXEC "), startsWith(upper, "EXECUTE "), startsWith(upper, "CALL "):
		// Stored procedure call
		return protocol.Request{
			Type:          protocol.RequestExec,
			SQL:           sql,
			ProcedureName: extractProcName(sql),
		}, nil

	case startsWith(upper, "BEGIN"):
		return protocol.Request{
			Type: protocol.RequestBeginTxn,
			SQL:  sql,
		}, nil

	case startsWith(upper, "COMMIT"):
		return protocol.Request{
			Type: protocol.RequestCommit,
			SQL:  sql,
		}, nil

	case startsWith(upper, "ROLLBACK"):
		return protocol.Request{
			Type: protocol.RequestRollback,
			SQL:  sql,
		}, nil

	default:
		// Treat as regular query
		return protocol.Request{
			Type: protocol.RequestQuery,
			SQL:  sql,
		}, nil
	}
}

// SendResult sends a result to the client.
func (c *Conn) SendResult(result protocol.Result) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.EOF
	}

	var buf []byte

	switch result.Type {
	case protocol.ResultError:
		// Send ErrorResponse
		errMsg := "ERROR"
		if result.Error != nil {
			errMsg = result.Error.Error()
		}
		buf = (&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000", // internal error
			Message:  errMsg,
		}).Encode(buf)

	case protocol.ResultOK:
		// Send CommandComplete
		tag := "OK"
		if result.Message != "" {
			tag = result.Message
		}
		if result.RowsAffected > 0 {
			tag = fmt.Sprintf("UPDATE %d", result.RowsAffected)
		}
		buf = (&pgproto3.CommandComplete{CommandTag: []byte(tag)}).Encode(buf)

	case protocol.ResultRows:
		// Send RowDescription + DataRows + CommandComplete
		for _, rs := range result.ResultSets {
			// RowDescription
			fields := make([]pgproto3.FieldDescription, len(rs.Columns))
			for i, col := range rs.Columns {
				fields[i] = pgproto3.FieldDescription{
					Name:                 []byte(col.Name),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          pgTypeOID(col.Type),
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0, // text format
				}
			}
			buf = (&pgproto3.RowDescription{Fields: fields}).Encode(buf)

			// DataRows
			for _, row := range rs.Rows {
				values := make([][]byte, len(row))
				for i, val := range row {
					if val == nil {
						values[i] = nil
					} else {
						values[i] = []byte(fmt.Sprintf("%v", val))
					}
				}
				buf = (&pgproto3.DataRow{Values: values}).Encode(buf)
			}

			// CommandComplete
			buf = (&pgproto3.CommandComplete{
				CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rs.Rows))),
			}).Encode(buf)
		}

	case protocol.ResultInfo, protocol.ResultWarning:
		// Send NoticeResponse
		severity := "INFO"
		if result.Type == protocol.ResultWarning {
			severity = "WARNING"
		}
		buf = (&pgproto3.NoticeResponse{
			Severity: severity,
			Message:  result.Message,
		}).Encode(buf)
	}

	// Always end with ReadyForQuery
	txStatus := byte('I') // idle
	buf = (&pgproto3.ReadyForQuery{TxStatus: txStatus}).Encode(buf)

	_, err := c.netConn.Write(buf)
	return err
}

// Close closes the connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	return c.netConn.Close()
}

// RemoteAddr returns the remote address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// SetDeadline sets the read/write deadline.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.netConn.SetDeadline(t)
}

// Properties returns connection properties for tenant identification.
func (c *Conn) Properties() map[string]string {
	props := make(map[string]string)
	if c.user != "" {
		props["user"] = c.user
	}
	if c.database != "" {
		props["database"] = c.database
	}
	// Include startup parameters (application_name, etc.)
	for k, v := range c.params {
		props[k] = v
	}
	return props
}

// Helper functions

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func extractProcName(sql string) string {
	// Simple extraction - skip EXEC/EXECUTE/CALL and get next word
	words := splitWords(sql)
	if len(words) < 2 {
		return ""
	}
	return words[1]
}

func splitWords(s string) []string {
	var words []string
	var word string
	for _, ch := range s {
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '(' || ch == ')' || ch == ',' {
			if word != "" {
				words = append(words, word)
				word = ""
			}
		} else {
			word += string(ch)
		}
	}
	if word != "" {
		words = append(words, word)
	}
	return words
}

// pgTypeOID returns the PostgreSQL type OID for a given SQL type.
func pgTypeOID(sqlType string) uint32 {
	// Common PostgreSQL type OIDs
	switch sqlType {
	case "int", "integer", "int4":
		return 23 // INT4OID
	case "bigint", "int8":
		return 20 // INT8OID
	case "smallint", "int2":
		return 21 // INT2OID
	case "text", "varchar", "nvarchar", "char", "nchar":
		return 25 // TEXTOID
	case "bool", "boolean", "bit":
		return 16 // BOOLOID
	case "float4", "real":
		return 700 // FLOAT4OID
	case "float8", "double precision", "float":
		return 701 // FLOAT8OID
	case "numeric", "decimal", "money":
		return 1700 // NUMERICOID
	case "date":
		return 1082 // DATEOID
	case "time":
		return 1083 // TIMEOID
	case "timestamp", "datetime", "datetime2":
		return 1114 // TIMESTAMPOID
	case "timestamptz", "datetimeoffset":
		return 1184 // TIMESTAMPTZOID
	case "bytea", "binary", "varbinary", "image":
		return 17 // BYTEAOID
	case "uuid", "uniqueidentifier":
		return 2950 // UUIDOID
	case "json":
		return 114 // JSONOID
	case "jsonb":
		return 3802 // JSONBOID
	case "xml":
		return 142 // XMLOID
	default:
		return 25 // Default to text
	}
}
