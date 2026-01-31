package tds

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"
)

// TLSConfig holds TLS configuration for the server.
type TLSConfig struct {
	// Certificate and key for the server
	CertFile string
	KeyFile  string

	// Or provide directly
	Certificate *tls.Certificate

	// Minimum TLS version (default TLS 1.2)
	MinVersion uint16

	// Client authentication
	ClientAuth tls.ClientAuthType
}

// DefaultTLSConfig returns a TLSConfig with sensible defaults.
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.NoClientCert,
	}
}

// tlsHandshakeConn wraps a TDS connection to perform TLS handshake
// with TLS messages either wrapped inside TDS PRELOGIN packets OR sent raw.
//
// Some clients (go-mssqldb) wrap TLS in TDS PRELOGIN packets.
// Other clients (Microsoft JDBC) send raw TLS after PRELOGIN exchange.
// This implementation auto-detects which mode the client is using.
type tlsHandshakeConn struct {
	conn      *Conn
	readBuf   []byte
	readPos   int
	rawTLS    bool // true if client sends raw TLS (not wrapped in TDS)
}

// newTLSHandshakeConn creates a wrapper for TLS handshake.
func newTLSHandshakeConn(conn *Conn) *tlsHandshakeConn {
	return &tlsHandshakeConn{
		conn:    conn,
		readBuf: make([]byte, 0),
	}
}

// Read implements io.Reader for the TLS handshake.
// It reads TLS records from TDS PRELOGIN packets OR directly from the wire.
// Some clients wrap TLS in TDS packets, others send raw TLS.
func (c *tlsHandshakeConn) Read(b []byte) (int, error) {
	// If we have buffered data, return it
	if c.readPos < len(c.readBuf) {
		n := copy(b, c.readBuf[c.readPos:])
		c.readPos += n
		return n, nil
	}

	// Peek at the first byte to determine if this is a TDS packet or raw TLS
	// TDS PRELOGIN packet starts with 0x12
	// TLS ClientHello starts with 0x16 (TLS record type: Handshake)
	peekBuf := make([]byte, 1)
	n, err := c.conn.netConn.Read(peekBuf)
	if err != nil {
		return 0, fmt.Errorf("peeking first byte: %w", err)
	}
	if n == 0 {
		return 0, fmt.Errorf("no data available")
	}

	firstByte := peekBuf[0]
	
	if firstByte == 0x16 {
		// Raw TLS - client is sending TLS directly, not wrapped in TDS
		// Read directly from the network connection
		// First, we need to read the rest of the TLS record
		// TLS record format: type(1) + version(2) + length(2) + data(length)
		header := make([]byte, 5)
		header[0] = firstByte
		_, err := io.ReadFull(c.conn.netConn, header[1:])
		if err != nil {
			return 0, fmt.Errorf("reading TLS header: %w", err)
		}
		
		recordLen := int(header[3])<<8 | int(header[4])
		record := make([]byte, 5+recordLen)
		copy(record, header)
		_, err = io.ReadFull(c.conn.netConn, record[5:])
		if err != nil {
			return 0, fmt.Errorf("reading TLS record: %w", err)
		}
		
		// Mark that we're in raw TLS mode for future reads
		c.rawTLS = true
		
		// Buffer and return
		c.readBuf = record
		c.readPos = 0
		n := copy(b, c.readBuf)
		c.readPos = n
		return n, nil
	}
	
	if firstByte == 0x12 {
		// TDS PRELOGIN packet - read as TDS packet
		// We need to "unread" the peeked byte by including it in the packet read
		// Actually, we need to read the full TDS header and then the payload
		// TDS header is 8 bytes total
		header := make([]byte, 8)
		header[0] = firstByte
		_, err := io.ReadFull(c.conn.netConn, header[1:])
		if err != nil {
			return 0, fmt.Errorf("reading TDS header: %w", err)
		}
		
		// Parse TDS header
		pktType := header[0]
		pktLen := int(header[2])<<8 | int(header[3])
		
		if pktType != 0x12 && pktType != 0x04 { // PRELOGIN or REPLY
			return 0, fmt.Errorf("unexpected TDS packet type 0x%02x during TLS handshake", pktType)
		}
		
		// Read payload (pktLen includes header)
		payloadLen := pktLen - 8
		if payloadLen > 0 {
			payload := make([]byte, payloadLen)
			_, err = io.ReadFull(c.conn.netConn, payload)
			if err != nil {
				return 0, fmt.Errorf("reading TDS payload: %w", err)
			}
			c.readBuf = payload
		} else {
			c.readBuf = nil
		}
		c.readPos = 0
		
		n := copy(b, c.readBuf)
		c.readPos = n
		return n, nil
	}
	
	return 0, fmt.Errorf("unexpected first byte 0x%02x during TLS handshake (expected 0x12 TDS or 0x16 TLS)", firstByte)
}

// Write implements io.Writer for the TLS handshake.
// In raw TLS mode, writes go directly to the network.
// In wrapped mode, TLS records are sent inside TDS PRELOGIN packets.
func (c *tlsHandshakeConn) Write(b []byte) (int, error) {
	if c.rawTLS {
		// Raw TLS mode - write directly to network
		return c.conn.netConn.Write(b)
	}
	
	// Wrapped mode - send as PRELOGIN packet (type 18)
	err := c.conn.WritePacket(PacketPrelogin, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close implements io.Closer.
func (c *tlsHandshakeConn) Close() error {
	return nil // Don't close underlying connection
}

// LocalAddr implements net.Conn.
func (c *tlsHandshakeConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr implements net.Conn.
func (c *tlsHandshakeConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline implements net.Conn.
func (c *tlsHandshakeConn) SetDeadline(t time.Time) error {
	return c.conn.netConn.SetDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (c *tlsHandshakeConn) SetReadDeadline(t time.Time) error {
	return c.conn.netConn.SetReadDeadline(t)
}

// SetWriteDeadline implements net.Conn.
func (c *tlsHandshakeConn) SetWriteDeadline(t time.Time) error {
	return c.conn.netConn.SetWriteDeadline(t)
}

// UpgradeToTLS performs a TLS handshake on the connection.
// This should be called after PRELOGIN negotiation indicates encryption.
//
// The TDS protocol uses a hybrid approach for TLS:
// 1. During handshake: TLS records are wrapped in TDS PRELOGIN packets
// 2. After handshake: TLS records go directly on the wire (not wrapped)
//
// This matches how SQL Server and go-mssqldb handle TLS.
func (c *Conn) UpgradeToTLS(config *tls.Config) error {
	// Peek at the first byte to see what the client is sending
	// This helps debug whether the client is sending wrapped TLS or raw TLS
	peekBuf := make([]byte, 1)
	c.netConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := c.netConn.Read(peekBuf)
	c.netConn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("peeking first TLS byte: %w", err)
	}
	if n > 0 {
		// Put the byte back by creating a multi-reader situation
		// Actually, we can't unread from net.Conn easily, so let's use a different approach
		firstByte := peekBuf[0]
		
		// Check if this is TDS (0x12 = PRELOGIN) or raw TLS (0x16 = TLS Handshake)
		if firstByte == 0x16 {
			// Client is sending raw TLS, not wrapped in TDS
			// This happens with some JDBC drivers
			// We need to do raw TLS handshake
			
			// Create a conn that prepends the peeked byte
			prependConn := &prependConn{
				Conn:    c.netConn,
				prepend: peekBuf[:n],
			}
			
			tlsConn := tls.Server(prependConn, config)
			if err := tlsConn.Handshake(); err != nil {
				return fmt.Errorf("raw TLS handshake failed: %w", err)
			}
			
			// Update the connection to use TLS
			c.mu.Lock()
			c.tlsConn = tlsConn
			c.reader = bufio.NewReaderSize(tlsConn, MaxPacketSize)
			c.writer = bufio.NewWriterSize(tlsConn, MaxPacketSize)
			c.mu.Unlock()
			
			return nil
		}
		
		// It's TDS-wrapped (0x12), proceed with wrapped handshake
		// But we already consumed the first byte, so we need to handle that
		
		// Create prependConn for the wrapped case too
		prependConn := &prependConn{
			Conn:    c.netConn,
			prepend: peekBuf[:n],
		}
		
		// Temporarily replace netConn
		origNetConn := c.netConn
		c.netConn = prependConn
		c.reader = bufio.NewReaderSize(prependConn, MaxPacketSize)
		
		// Now do the wrapped handshake
		handshakeConn := newTLSHandshakeConn(c)
		passthrough := &switchableConn{conn: handshakeConn}
		
		tlsConn := tls.Server(passthrough, config)
		
		c.netConn.SetDeadline(time.Now().Add(30 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			c.netConn = origNetConn
			return fmt.Errorf("TLS handshake failed: %w", err)
		}
		c.netConn.SetDeadline(time.Time{})
		
		// After handshake, switch to direct mode
		passthrough.conn = origNetConn
		
		c.mu.Lock()
		c.netConn = origNetConn
		c.tlsConn = tlsConn
		c.reader = bufio.NewReaderSize(tlsConn, MaxPacketSize)
		c.writer = bufio.NewWriterSize(tlsConn, MaxPacketSize)
		c.mu.Unlock()
		
		return nil
	}
	
	return fmt.Errorf("no data from client for TLS handshake")
}

// UpgradeToTLSWithInitialData performs a TLS handshake when we already have the first TLS message.
// This is used for "login-only encryption" where the client sends TLS in a PRELOGIN packet
// even though we responded with EncryptOff.
// The TLS messages continue to be wrapped in TDS PRELOGIN packets.
func (c *Conn) UpgradeToTLSWithInitialData(config *tls.Config, initialData []byte) error {
	if len(initialData) == 0 {
		return fmt.Errorf("no initial data for TLS handshake")
	}
	
	// Check if the initial data is TLS (starts with 0x16)
	firstByte := initialData[0]
	if firstByte != 0x16 {
		return fmt.Errorf("unexpected initial data byte 0x%02x (expected 0x16 TLS)", firstByte)
	}
	
	// Create a TLS handshake conn that has the initial data pre-buffered
	// First message (ClientHello) is pre-buffered, subsequent reads are raw from network
	// First write (ServerHello) is wrapped in TDS, subsequent writes are raw
	handshakeConn := &tlsHandshakeConnWithInitial{
		conn:        c,
		initialData: initialData,
		initialUsed: false,
	}
	
	// The handshakeConn handles the mixed wrapped/raw protocol
	// After handshake, TLS continues to use this conn which reads/writes raw
	tlsConn := tls.Server(handshakeConn, config)
	
	c.netConn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		c.netConn.SetDeadline(time.Time{})
		return fmt.Errorf("wrapped TLS handshake with initial data failed: %w", err)
	}
	c.netConn.SetDeadline(time.Time{})
	
	// Mark handshake complete - subsequent reads/writes go raw to network
	handshakeConn.handshakeComplete = true
	
	// Update the connection to use TLS
	// The tlsConn wraps handshakeConn which now operates in raw mode
	c.mu.Lock()
	c.tlsConn = tlsConn
	c.reader = bufio.NewReaderSize(tlsConn, MaxPacketSize)
	c.writer = bufio.NewWriterSize(tlsConn, MaxPacketSize)
	c.mu.Unlock()
	
	return nil
}

// tlsHandshakeConnWithInitial wraps TLS handshake with pre-buffered initial data.
// During handshake: reads initial data first, then TLS messages from TDS PRELOGIN packets.
// After handshake: reads/writes raw TLS records directly from/to network.
type tlsHandshakeConnWithInitial struct {
	conn              *Conn
	initialData       []byte
	initialUsed       bool
	readBuf           []byte
	readPos           int
	handshakeComplete bool // set to true after handshake to switch to raw mode
}

func (c *tlsHandshakeConnWithInitial) Read(b []byte) (int, error) {
	// After handshake is complete, read raw from network (for TLS records)
	if c.handshakeComplete {
		return c.conn.netConn.Read(b)
	}
	
	// First, return the initial data (ClientHello)
	if !c.initialUsed {
		n := copy(b, c.initialData)
		if n < len(c.initialData) {
			// Didn't fit all of it, buffer the rest
			c.readBuf = c.initialData[n:]
			c.readPos = 0
		}
		c.initialUsed = true
		return n, nil
	}
	
	// Return any buffered data
	if c.readPos < len(c.readBuf) {
		n := copy(b, c.readBuf[c.readPos:])
		c.readPos += n
		return n, nil
	}
	
	// For TLS 1.2, ALL handshake messages are wrapped in TDS PRELOGIN packets
	pktType, data, err := c.conn.ReadPacket()
	if err != nil {
		return 0, fmt.Errorf("reading TDS packet for TLS: %w", err)
	}
	
	// Should be PRELOGIN containing TLS data
	if pktType != PacketPrelogin {
		return 0, fmt.Errorf("expected PRELOGIN packet during TLS handshake, got %s", pktType)
	}
	
	// Buffer the data and return what fits
	c.readBuf = data
	c.readPos = 0
	
	n := copy(b, c.readBuf)
	c.readPos = n
	
	return n, nil
}

func (c *tlsHandshakeConnWithInitial) Write(b []byte) (int, error) {
	// After handshake is complete, write raw to network (for TLS records)
	if c.handshakeComplete {
		return c.conn.netConn.Write(b)
	}
	
	// For TLS 1.2, ALL handshake messages are wrapped in TDS PRELOGIN packets
	err := c.conn.WritePacket(PacketPrelogin, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *tlsHandshakeConnWithInitial) Close() error {
	return nil
}

func (c *tlsHandshakeConnWithInitial) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *tlsHandshakeConnWithInitial) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *tlsHandshakeConnWithInitial) SetDeadline(t time.Time) error {
	return c.conn.netConn.SetDeadline(t)
}

func (c *tlsHandshakeConnWithInitial) SetReadDeadline(t time.Time) error {
	return c.conn.netConn.SetReadDeadline(t)
}

func (c *tlsHandshakeConnWithInitial) SetWriteDeadline(t time.Time) error {
	return c.conn.netConn.SetWriteDeadline(t)
}

// prependConn wraps a net.Conn and prepends some bytes to the read stream.
type prependConn struct {
	net.Conn
	prepend []byte
	offset  int
}

func (p *prependConn) Read(b []byte) (int, error) {
	if p.offset < len(p.prepend) {
		n := copy(b, p.prepend[p.offset:])
		p.offset += n
		return n, nil
	}
	return p.Conn.Read(b)
}

// switchableConn is a net.Conn wrapper that can switch its underlying connection.
// This is used for TDS TLS where we start with wrapped TLS (in TDS packets)
// and then switch to direct TLS on the wire after handshake.
type switchableConn struct {
	conn io.ReadWriteCloser
}

func (s *switchableConn) Read(b []byte) (int, error) {
	return s.conn.Read(b)
}

func (s *switchableConn) Write(b []byte) (int, error) {
	return s.conn.Write(b)
}

func (s *switchableConn) Close() error {
	return s.conn.Close()
}

func (s *switchableConn) LocalAddr() net.Addr {
	if nc, ok := s.conn.(net.Conn); ok {
		return nc.LocalAddr()
	}
	return nil
}

func (s *switchableConn) RemoteAddr() net.Addr {
	if nc, ok := s.conn.(net.Conn); ok {
		return nc.RemoteAddr()
	}
	return nil
}

func (s *switchableConn) SetDeadline(t time.Time) error {
	if nc, ok := s.conn.(net.Conn); ok {
		return nc.SetDeadline(t)
	}
	return nil
}

func (s *switchableConn) SetReadDeadline(t time.Time) error {
	if nc, ok := s.conn.(net.Conn); ok {
		return nc.SetReadDeadline(t)
	}
	return nil
}

func (s *switchableConn) SetWriteDeadline(t time.Time) error {
	if nc, ok := s.conn.(net.Conn); ok {
		return nc.SetWriteDeadline(t)
	}
	return nil
}
