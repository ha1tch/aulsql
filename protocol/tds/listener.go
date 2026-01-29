package tds

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/tds"
)

// Listener implements protocol.Listener for the TDS protocol.
type Listener struct {
	cfg         protocol.ListenerConfig
	logger      *log.Logger
	listener    net.Listener
	connections sync.Map // map[uint16]*Connection (keyed by SPID)
	connCount   int32
	nextSPID    uint16
	spidMu      sync.Mutex
	closed      int32

	// Server identity for login response
	serverName    string
	serverVersion tds.ServerVersion

	// TLS configuration (nil means no TLS support)
	tlsConfig *tls.Config
}

// New creates a new TDS listener.
func New(cfg protocol.ListenerConfig, logger *log.Logger) (protocol.Listener, error) {
	serverName := "aul"
	if name, ok := cfg.Options["server_name"].(string); ok && name != "" {
		serverName = name
	}

	l := &Listener{
		cfg:           cfg,
		logger:        logger,
		nextSPID:      51, // SPIDs 1-50 are reserved for system
		serverName:    serverName,
		serverVersion: tds.DefaultServerVersion(),
	}

	// Load TLS configuration if enabled
	if cfg.TLSEnabled {
		tlsConfig, err := loadTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("loading TLS config: %w", err)
		}
		l.tlsConfig = tlsConfig
		logger.Application().Info("TLS enabled for TDS listener")
	}

	return l, nil
}

// loadTLSConfig creates a tls.Config from the listener configuration.
func loadTLSConfig(cfg protocol.ListenerConfig) (*tls.Config, error) {
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("TLS enabled but TLSCertFile or TLSKeyFile not specified")
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Protocol returns the protocol type.
func (l *Listener) Protocol() protocol.ProtocolType {
	return protocol.ProtocolTDS
}

// Listen starts the TDS listener.
func (l *Listener) Listen() error {
	addr := l.cfg.Address()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	l.listener = ln
	l.logger.Application().Info("TDS listener started", "address", addr)
	return nil
}

// Accept waits for and returns the next TDS connection.
func (l *Listener) Accept() (protocol.Connection, error) {
	if atomic.LoadInt32(&l.closed) == 1 {
		return nil, net.ErrClosed
	}

	netConn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}

	// Check connection limit
	if l.cfg.MaxConnections > 0 && int(atomic.LoadInt32(&l.connCount)) >= l.cfg.MaxConnections {
		netConn.Close()
		return nil, fmt.Errorf("maximum connections (%d) reached", l.cfg.MaxConnections)
	}

	// Allocate SPID
	spid := l.allocateSPID()

	// Create TDS connection wrapper
	tdsConn := tds.NewConn(netConn,
		tds.WithSPID(spid),
		tds.WithReadTimeout(l.cfg.ReadTimeout),
		tds.WithWriteTimeout(l.cfg.WriteTimeout),
	)

	conn := &Connection{
		listener:    l,
		tdsConn:     tdsConn,
		logger:      l.logger,
		spid:        spid,
		serverName:  l.serverName,
		tlsConfig:   l.tlsConfig,
		phase3:      DefaultPhase3Handlers(),
		phase3State: NewConnectionPhase3State(),
	}

	// Perform TDS handshake
	if err := conn.handshake(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("TDS handshake failed: %w", err)
	}

	// Track connection
	l.connections.Store(spid, conn)
	atomic.AddInt32(&l.connCount, 1)

	l.logger.Application().Debug("TDS connection established",
		"spid", spid,
		"remote", netConn.RemoteAddr(),
		"user", conn.user,
		"database", conn.database,
		"app", conn.appName,
	)

	return conn, nil
}

// Close stops the listener and closes all connections.
func (l *Listener) Close() error {
	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return nil
	}

	// Close all connections
	l.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*Connection); ok {
			conn.Close()
		}
		return true
	})

	// Close listener
	if l.listener != nil {
		return l.listener.Close()
	}
	return nil
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	if l.listener != nil {
		return l.listener.Addr()
	}
	return nil
}

// ConnectionCount returns the number of active connections.
func (l *Listener) ConnectionCount() int {
	return int(atomic.LoadInt32(&l.connCount))
}

// allocateSPID allocates a unique SPID for a new connection.
func (l *Listener) allocateSPID() uint16 {
	l.spidMu.Lock()
	defer l.spidMu.Unlock()

	spid := l.nextSPID
	l.nextSPID++
	if l.nextSPID == 0 {
		l.nextSPID = 51 // Wrap around, skip reserved range
	}
	return spid
}

// removeConnection removes a connection from tracking.
func (l *Listener) removeConnection(spid uint16) {
	if _, loaded := l.connections.LoadAndDelete(spid); loaded {
		atomic.AddInt32(&l.connCount, -1)
	}
}
