package tds

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/pkg/tlsutil"
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
		tlsConfig, err := loadTLSConfig(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("loading TLS config: %w", err)
		}
		l.tlsConfig = tlsConfig
		logger.Application().Info("TLS enabled for TDS listener")
	} else {
		// Auto-generate TLS certificate for development use
		// This allows JDBC and other clients that require TLS to connect
		tlsConfig, err := tlsutil.GenerateSelfSignedCert()
		if err != nil {
			logger.Application().Warn("failed to auto-generate TLS certificate", "error", err)
		} else {
			l.tlsConfig = tlsConfig
			logger.Application().Info("auto-generated self-signed TLS certificate for development")
		}
	}

	return l, nil
}

// loadTLSConfig creates a tls.Config from the listener configuration.
func loadTLSConfig(cfg protocol.ListenerConfig, logger *log.Logger) (*tls.Config, error) {
	// If cert files are specified, load them
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading certificate: %w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			MaxVersion:   tls.VersionTLS12, // Force TLS 1.2 for JDBC/TDS compatibility
		}, nil
	}

	// Auto-generate a self-signed certificate
	logger.Application().Info("no TLS certificate specified, generating self-signed certificate")
	return tlsutil.GenerateSelfSignedCert()
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

	// Detect connection type by peeking at first byte:
	// - 0x16 = TLS ClientHello (TDS 8.0 strict mode or direct TLS)
	// - 0x12 = TDS PRELOGIN (TDS 7.x mode, TLS negotiated later)
	var actualConn net.Conn = netConn
	isTDS8Strict := false

	if l.tlsConfig != nil {
		// Set a short read deadline for the peek
		netConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		
		// Peek at first byte to detect connection type
		peekConn := &peekableConn{Conn: netConn}
		firstByte, err := peekConn.Peek(1)
		
		// Clear the deadline
		netConn.SetReadDeadline(time.Time{})
		
		if err != nil {
			// EOF or timeout - client disconnected or is probing
			netConn.Close()
			return nil, fmt.Errorf("peeking first byte: %w", err)
		}

		l.logger.Application().Debug("connection first byte", "byte", fmt.Sprintf("0x%02X", firstByte[0]))

		if firstByte[0] == 0x16 { // TLS record type: Handshake
			// TDS 8.0 strict mode - client initiates TLS immediately
			l.logger.Application().Debug("detected TDS 8.0 strict mode (TLS-first)")
			isTDS8Strict = true

			tlsConn := tls.Server(peekConn, l.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				netConn.Close()
				return nil, fmt.Errorf("TDS 8.0 TLS handshake: %w", err)
			}
			actualConn = tlsConn
			l.logger.Application().Debug("TDS 8.0 TLS handshake completed")
		} else if firstByte[0] == 0x12 { // TDS PRELOGIN
			// TDS 7.x mode - PRELOGIN first, TLS wrapped in TDS packets later
			l.logger.Application().Debug("detected TDS 7.x mode (PRELOGIN-first)")
			actualConn = peekConn
		} else {
			l.logger.Application().Warn("unexpected first byte", "byte", fmt.Sprintf("0x%02X", firstByte[0]))
			actualConn = peekConn
		}
	}

	// Allocate SPID
	spid := l.allocateSPID()

	// Create TDS connection wrapper
	tdsConn := tds.NewConn(actualConn,
		tds.WithSPID(spid),
		tds.WithReadTimeout(l.cfg.ReadTimeout),
		tds.WithWriteTimeout(l.cfg.WriteTimeout),
	)

	conn := &Connection{
		listener:     l,
		tdsConn:      tdsConn,
		logger:       l.logger,
		spid:         spid,
		serverName:   l.serverName,
		tlsConfig:    l.tlsConfig,
		isTDS8Strict: isTDS8Strict,
		phase3:       DefaultPhase3Handlers(),
		phase3State:  NewConnectionPhase3State(),
	}

	// Perform TDS handshake (PRELOGIN/LOGIN7)
	// In TDS 8.0 strict mode, TLS is already done, so handshake skips TLS negotiation
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

// peekableConn wraps a net.Conn to allow peeking at data without consuming it.
// This is used to detect TDS 8.0 strict mode vs TDS 7.x by looking at the first byte.
type peekableConn struct {
	net.Conn
	reader *bufio.Reader
}

// Peek reads n bytes without advancing the reader.
func (p *peekableConn) Peek(n int) ([]byte, error) {
	if p.reader == nil {
		p.reader = bufio.NewReader(p.Conn)
	}
	return p.reader.Peek(n)
}

// Read implements net.Conn.Read, using buffered reader if initialized.
func (p *peekableConn) Read(b []byte) (int, error) {
	if p.reader != nil {
		return p.reader.Read(b)
	}
	return p.Conn.Read(b)
}

// Ensure peekableConn implements net.Conn
var _ net.Conn = (*peekableConn)(nil)

// LocalAddr implements net.Conn.
func (p *peekableConn) LocalAddr() net.Addr {
	return p.Conn.LocalAddr()
}

// RemoteAddr implements net.Conn.
func (p *peekableConn) RemoteAddr() net.Addr {
	return p.Conn.RemoteAddr()
}

// SetDeadline implements net.Conn.
func (p *peekableConn) SetDeadline(t time.Time) error {
	return p.Conn.SetDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (p *peekableConn) SetReadDeadline(t time.Time) error {
	return p.Conn.SetReadDeadline(t)
}

// SetWriteDeadline implements net.Conn.
func (p *peekableConn) SetWriteDeadline(t time.Time) error {
	return p.Conn.SetWriteDeadline(t)
}

// Close implements net.Conn.
func (p *peekableConn) Close() error {
	return p.Conn.Close()
}
