package tds

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Conn represents a TDS connection from a client.
type Conn struct {
	mu         sync.Mutex
	netConn    net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	packetSize int
	spid       uint16
	packetSeq  uint8

	// TLS connection (set after TLS handshake)
	tlsConn *tls.Conn

	// Connection state
	database    string
	user        string
	appName     string
	clientHost  string
	tdsVersion  uint32

	// Settings
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// ConnOption configures a TDS connection.
type ConnOption func(*Conn)

// WithPacketSize sets the TDS packet size.
func WithPacketSize(size int) ConnOption {
	return func(c *Conn) {
		if size >= MinPacketSize && size <= MaxPacketSize {
			c.packetSize = size
		}
	}
}

// WithSPID sets the server process ID for this connection.
func WithSPID(spid uint16) ConnOption {
	return func(c *Conn) {
		c.spid = spid
	}
}

// WithReadTimeout sets the read timeout.
func WithReadTimeout(d time.Duration) ConnOption {
	return func(c *Conn) {
		c.readTimeout = d
	}
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(d time.Duration) ConnOption {
	return func(c *Conn) {
		c.writeTimeout = d
	}
}

// NewConn wraps a net.Conn as a TDS connection.
func NewConn(netConn net.Conn, opts ...ConnOption) *Conn {
	c := &Conn{
		netConn:    netConn,
		reader:     bufio.NewReaderSize(netConn, MaxPacketSize),
		writer:     bufio.NewWriterSize(netConn, MaxPacketSize),
		packetSize: DefaultPacketSize,
		spid:       1,
		packetSeq:  1,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// NetConn returns the underlying net.Conn.
func (c *Conn) NetConn() net.Conn {
	return c.netConn
}

// SPID returns the server process ID.
func (c *Conn) SPID() uint16 {
	return c.spid
}

// PacketSize returns the negotiated packet size.
func (c *Conn) PacketSize() int {
	return c.packetSize
}

// SetPacketSize updates the packet size (called after negotiation).
func (c *Conn) SetPacketSize(size int) {
	if size >= MinPacketSize && size <= MaxPacketSize {
		c.packetSize = size
	}
}

// Database returns the current database.
func (c *Conn) Database() string {
	return c.database
}

// SetDatabase sets the current database.
func (c *Conn) SetDatabase(db string) {
	c.database = db
}

// User returns the authenticated user.
func (c *Conn) User() string {
	return c.user
}

// SetUser sets the authenticated user.
func (c *Conn) SetUser(user string) {
	c.user = user
}

// AppName returns the client application name.
func (c *Conn) AppName() string {
	return c.appName
}

// SetAppName sets the client application name.
func (c *Conn) SetAppName(name string) {
	c.appName = name
}

// ClientHost returns the client hostname.
func (c *Conn) ClientHost() string {
	return c.clientHost
}

// SetClientHost sets the client hostname.
func (c *Conn) SetClientHost(host string) {
	c.clientHost = host
}

// TDSVersion returns the negotiated TDS version.
func (c *Conn) TDSVersion() uint32 {
	return c.tdsVersion
}

// SetTDSVersion sets the TDS version.
func (c *Conn) SetTDSVersion(ver uint32) {
	c.tdsVersion = ver
}

// RemoteAddr returns the remote address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// LocalAddr returns the local address.
func (c *Conn) LocalAddr() net.Addr {
	return c.netConn.LocalAddr()
}

// Close closes the connection.
func (c *Conn) Close() error {
	return c.netConn.Close()
}

// ReadPacket reads a complete TDS packet (possibly spanning multiple network packets).
func (c *Conn) ReadPacket() (PacketType, []byte, error) {
	pktType, _, data, err := c.ReadPacketWithStatus()
	return pktType, data, err
}

// ReadPacketWithStatus reads a complete TDS packet and returns the status byte.
// This is needed to detect connection reset requests (StatusResetConnection).
func (c *Conn) ReadPacketWithStatus() (PacketType, PacketStatus, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.readTimeout > 0 {
		c.netConn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	// Read first header
	hdr, err := ReadHeader(c.reader)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("reading packet header: %w", err)
	}

	// Capture the status from the first packet
	status := hdr.Status

	// Validate header
	if hdr.Length < HeaderSize {
		return 0, 0, nil, fmt.Errorf("invalid packet length: %d", hdr.Length)
	}
	if hdr.Length > uint16(c.packetSize) {
		return 0, 0, nil, fmt.Errorf("packet too large: %d > %d", hdr.Length, c.packetSize)
	}

	// Allocate buffer for message
	var data []byte
	payloadLen := hdr.PayloadLength()
	if payloadLen > 0 {
		data = make([]byte, 0, payloadLen)
		chunk := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.reader, chunk); err != nil {
			return 0, 0, nil, fmt.Errorf("reading packet payload: %w", err)
		}
		data = append(data, chunk...)
	}

	// Read continuation packets if not EOM
	for !hdr.IsLastPacket() {
		if c.readTimeout > 0 {
			c.netConn.SetReadDeadline(time.Now().Add(c.readTimeout))
		}

		hdr, err = ReadHeader(c.reader)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("reading continuation header: %w", err)
		}

		payloadLen = hdr.PayloadLength()
		if payloadLen > 0 {
			chunk := make([]byte, payloadLen)
			if _, err := io.ReadFull(c.reader, chunk); err != nil {
				return 0, 0, nil, fmt.Errorf("reading continuation payload: %w", err)
			}
			data = append(data, chunk...)
		}
	}

	return hdr.Type, status, data, nil
}

// ResetConnection flag check
func (s PacketStatus) IsResetConnection() bool {
	return s&StatusResetConnection != 0
}

// ResetConnectionSkipTran flag check
func (s PacketStatus) IsResetConnectionSkipTran() bool {
	return s&StatusResetConnectionSkipTran != 0
}

// WritePacket writes a TDS packet, splitting into multiple packets if needed.
func (c *Conn) WritePacket(pktType PacketType, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.writeTimeout > 0 {
		c.netConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	maxPayload := c.packetSize - HeaderSize
	remaining := data

	for {
		isLast := len(remaining) <= maxPayload
		var chunk []byte
		if isLast {
			chunk = remaining
		} else {
			chunk = remaining[:maxPayload]
			remaining = remaining[maxPayload:]
		}

		status := StatusNormal
		if isLast {
			status = StatusEOM
		}

		hdr := Header{
			Type:     pktType,
			Status:   status,
			Length:   uint16(HeaderSize + len(chunk)),
			SPID:     c.spid,
			PacketID: c.packetSeq,
			Window:   0,
		}

		if err := hdr.Write(c.writer); err != nil {
			return fmt.Errorf("writing packet header: %w", err)
		}
		if _, err := c.writer.Write(chunk); err != nil {
			return fmt.Errorf("writing packet data: %w", err)
		}

		c.packetSeq++
		if c.packetSeq == 0 {
			c.packetSeq = 1
		}

		if isLast {
			break
		}
	}

	return c.writer.Flush()
}

// WriteTokens writes a token stream as a REPLY packet.
func (c *Conn) WriteTokens(tw *TokenWriter) error {
	return c.WritePacket(PacketReply, tw.Bytes())
}

// Flush flushes any buffered data.
func (c *Conn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writer.Flush()
}

// ResetPacketSequence resets the packet sequence number.
func (c *Conn) ResetPacketSequence() {
	c.mu.Lock()
	c.packetSeq = 1
	c.mu.Unlock()
}
