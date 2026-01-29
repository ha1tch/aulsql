// Package http implements an HTTP REST API for aul.
//
// This provides a simple JSON-based API for executing stored procedures
// and ad-hoc SQL queries, useful for testing and lightweight integrations.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
)

// Listener implements protocol.Listener for HTTP REST API.
type Listener struct {
	mu sync.RWMutex

	cfg        protocol.ListenerConfig
	logger     *log.Logger
	httpServer *http.Server
	listener   net.Listener

	// Request queue for the Accept pattern
	reqChan chan *httpRequest
	
	// Connection tracking
	connCount int64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
}

// httpRequest wraps an HTTP request/response for the Accept pattern.
type httpRequest struct {
	req      *http.Request
	respChan chan protocol.Result
	done     chan struct{}
}

// NewListener creates a new HTTP protocol listener.
func NewListener(cfg protocol.ListenerConfig, logger *log.Logger) (*Listener, error) {
	ctx, cancel := context.WithCancel(context.Background())

	l := &Listener{
		cfg:     cfg,
		logger:  logger,
		reqChan: make(chan *httpRequest, 100),
		ctx:     ctx,
		cancel:  cancel,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", l.handleRequest)
	mux.HandleFunc("/health", l.handleHealth)
	mux.HandleFunc("/exec", l.handleExec)
	mux.HandleFunc("/query", l.handleQuery)
	mux.HandleFunc("/procedures", l.handleProcedures)

	l.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return l, nil
}

// Protocol returns the protocol type.
func (l *Listener) Protocol() protocol.ProtocolType {
	return protocol.ProtocolHTTP
}

// Listen starts listening on the configured address.
func (l *Listener) Listen() error {
	addr := l.cfg.Address()

	var err error
	l.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	l.logger.System().Info("HTTP listener started",
		"address", addr,
	)

	// Start HTTP server in background
	go func() {
		if err := l.httpServer.Serve(l.listener); err != nil && err != http.ErrServerClosed {
			l.logger.System().Error("HTTP server error", err)
		}
	}()

	return nil
}

// Accept waits for and returns the next connection.
// For HTTP, this returns a pseudo-connection for each request.
func (l *Listener) Accept() (protocol.Connection, error) {
	for {
		select {
		case <-l.ctx.Done():
			return nil, io.EOF
		case req := <-l.reqChan:
			atomic.AddInt64(&l.connCount, 1)
			return &httpConn{
				req:      req,
				listener: l,
			}, nil
		}
	}
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return l.httpServer.Shutdown(ctx)
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

// HTTP handlers

func (l *Listener) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"server": "aul",
	})
}

func (l *Listener) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		l.handleHealth(w, r)
		return
	}
	http.NotFound(w, r)
}

func (l *Listener) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Create request and wait for response
	req := &httpRequest{
		req:      r,
		respChan: make(chan protocol.Result, 1),
		done:     make(chan struct{}),
	}

	select {
	case l.reqChan <- req:
		// Wait for response
		select {
		case result := <-req.respChan:
			l.writeResult(w, result)
		case <-time.After(30 * time.Second):
			http.Error(w, "Timeout", http.StatusGatewayTimeout)
		}
		close(req.done)
	case <-time.After(5 * time.Second):
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
	}
}

func (l *Listener) handleQuery(w http.ResponseWriter, r *http.Request) {
	// Same as exec for now
	l.handleExec(w, r)
}

func (l *Listener) handleProcedures(w http.ResponseWriter, r *http.Request) {
	// This would list available procedures
	// For now, return empty list
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"procedures": []string{},
	})
}

func (l *Listener) writeResult(w http.ResponseWriter, result protocol.Result) {
	w.Header().Set("Content-Type", "application/json")

	resp := APIResponse{
		Success: result.Type != protocol.ResultError,
	}

	if result.Error != nil {
		resp.Error = result.Error.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	if result.Message != "" {
		resp.Message = result.Message
	}

	resp.RowsAffected = result.RowsAffected

	if len(result.ResultSets) > 0 {
		resp.Results = make([]ResultSetJSON, len(result.ResultSets))
		for i, rs := range result.ResultSets {
			resp.Results[i] = ResultSetJSON{
				Columns: make([]string, len(rs.Columns)),
				Rows:    rs.Rows,
			}
			for j, col := range rs.Columns {
				resp.Results[i].Columns[j] = col.Name
			}
		}
	}

	if result.OutputParams != nil {
		resp.OutputParams = result.OutputParams
	}

	json.NewEncoder(w).Encode(resp)
}

// APIResponse is the JSON response structure.
type APIResponse struct {
	Success      bool                   `json:"success"`
	Error        string                 `json:"error,omitempty"`
	Message      string                 `json:"message,omitempty"`
	RowsAffected int64                  `json:"rows_affected,omitempty"`
	Results      []ResultSetJSON        `json:"results,omitempty"`
	OutputParams map[string]interface{} `json:"output_params,omitempty"`
}

// ResultSetJSON is a JSON-serializable result set.
type ResultSetJSON struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// APIRequest is the JSON request structure.
type APIRequest struct {
	Procedure  string                 `json:"procedure,omitempty"`
	SQL        string                 `json:"sql,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	Timeout    string                 `json:"timeout,omitempty"`
}

// httpConn implements protocol.Connection for HTTP requests.
type httpConn struct {
	mu       sync.Mutex
	req      *httpRequest
	listener *Listener
	closed   bool
	gotReq   bool
}

// ReadRequest reads the next request from the client.
func (c *httpConn) ReadRequest() (protocol.Request, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return protocol.Request{}, io.EOF
	}

	// Only return one request per connection
	if c.gotReq {
		return protocol.Request{}, io.EOF
	}
	c.gotReq = true

	// Parse request body
	var apiReq APIRequest
	if err := json.NewDecoder(c.req.req.Body).Decode(&apiReq); err != nil {
		return protocol.Request{}, fmt.Errorf("invalid request body: %w", err)
	}

	// Determine request type
	reqType := protocol.RequestQuery
	if apiReq.Procedure != "" {
		reqType = protocol.RequestExec
	}

	// Parse timeout
	var timeout time.Duration
	if apiReq.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(apiReq.Timeout)
		if err != nil {
			timeout = 30 * time.Second
		}
	}

	return protocol.Request{
		Type:          reqType,
		SQL:           apiReq.SQL,
		ProcedureName: apiReq.Procedure,
		Parameters:    apiReq.Parameters,
		Options: protocol.RequestOptions{
			Timeout: timeout,
		},
	}, nil
}

// SendResult sends a result to the client.
func (c *httpConn) SendResult(result protocol.Result) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.EOF
	}

	// Convert protocol.ResultSet to the expected type
	convertedResult := protocol.Result{
		Type:         result.Type,
		Error:        result.Error,
		Message:      result.Message,
		RowsAffected: result.RowsAffected,
		ReturnValue:  result.ReturnValue,
		OutputParams: result.OutputParams,
	}

	// Convert result sets
	for _, rs := range result.ResultSets {
		convertedResult.ResultSets = append(convertedResult.ResultSets, protocol.ResultSet{
			Columns: rs.Columns,
			Rows:    rs.Rows,
		})
	}

	select {
	case c.req.respChan <- convertedResult:
		return nil
	case <-c.req.done:
		return io.EOF
	}
}

// Close closes the connection.
func (c *httpConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	atomic.AddInt64(&c.listener.connCount, -1)
	return nil
}

// RemoteAddr returns the remote address.
func (c *httpConn) RemoteAddr() net.Addr {
	// Parse from X-Forwarded-For or RemoteAddr
	addr := c.req.req.RemoteAddr
	if xff := c.req.req.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			addr = strings.TrimSpace(parts[0])
		}
	}
	// Return a fake addr since we don't have a real one
	return &httpAddr{addr: addr}
}

// SetDeadline sets the read/write deadline.
func (c *httpConn) SetDeadline(t time.Time) error {
	// HTTP doesn't support per-connection deadlines this way
	return nil
}

// Properties returns connection properties for tenant identification.
func (c *httpConn) Properties() map[string]string {
	props := make(map[string]string)
	// HTTP connections can use headers for tenant identification
	// The server will use TenantSources to extract from headers directly
	return props
}

// httpAddr implements net.Addr for HTTP connections.
type httpAddr struct {
	addr string
}

func (a *httpAddr) Network() string { return "http" }
func (a *httpAddr) String() string  { return a.addr }
