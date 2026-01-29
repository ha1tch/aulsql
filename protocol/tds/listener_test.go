package tds

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/tds"
)

func TestListenerCreation(t *testing.T) {
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0, // Let OS assign port
	}

	logger := log.New(log.DefaultConfig())

	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if listener.Protocol() != protocol.ProtocolTDS {
		t.Errorf("Expected protocol TDS, got %s", listener.Protocol())
	}
}

func TestPreloginHandshake(t *testing.T) {
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0, // Let OS assign port
		Options: map[string]interface{}{
			"server_name": "test-server",
		},
	}

	logger := log.New(log.DefaultConfig())

	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	t.Logf("Listener started on %s", addr)

	// This test just verifies the listener can accept TCP connections
	// and that the basic TDS packet structure works.
	// The full handshake is tested in TestFullLoginHandshake.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	conn.Close()
	t.Log("TCP connection established successfully")
}

func TestFullLoginHandshake(t *testing.T) {
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0,
		Options: map[string]interface{}{
			"server_name": "test-server",
		},
	}

	logger := log.New(log.DefaultConfig())

	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Run server accept in goroutine
	acceptErr := make(chan error, 1)
	acceptConn := make(chan protocol.Connection, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		acceptConn <- c
	}()

	// Connect as a client
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 1. Send PRELOGIN
	preloginData := buildPreloginRequest()
	pkt := buildTDSPacket(tds.PacketPrelogin, preloginData)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("Failed to send prelogin: %v", err)
	}

	// 2. Read PRELOGIN response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respBuf := make([]byte, 4096)
	n, err := conn.Read(respBuf)
	if err != nil {
		t.Fatalf("Failed to read prelogin response: %v", err)
	}

	if respBuf[0] != byte(tds.PacketReply) {
		t.Fatalf("Expected REPLY packet, got %d", respBuf[0])
	}

	t.Logf("PRELOGIN response received: %d bytes", n)

	// 3. Send LOGIN7
	login7Data := buildLogin7Request("testuser", "testpass", "testdb", "testapp")
	pkt = buildTDSPacket(tds.PacketLogin7, login7Data)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("Failed to send login: %v", err)
	}

	// 4. Read LOGINACK response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = conn.Read(respBuf)
	if err != nil {
		t.Fatalf("Failed to read login response: %v", err)
	}

	if respBuf[0] != byte(tds.PacketReply) {
		t.Fatalf("Expected REPLY packet, got %d", respBuf[0])
	}

	t.Logf("LOGIN response received: %d bytes", n)

	// Check for LOGINACK token in response
	// Response should contain ENVCHANGE (0xE3), INFO (0xAB), LOGINACK (0xAD), DONE (0xFD)
	payload := respBuf[8:n] // Skip TDS header
	foundLoginAck := false
	for i := 0; i < len(payload)-1; {
		token := payload[i]
		if token == byte(tds.TokenLoginAck) {
			foundLoginAck = true
			break
		}
		// Skip to next token based on token type
		i++ // Move past token byte
		if i >= len(payload) {
			break
		}
		// Most tokens have a 2-byte length prefix
		if i+2 > len(payload) {
			break
		}
		tokenLen := int(binary.LittleEndian.Uint16(payload[i : i+2]))
		i += 2 + tokenLen
	}

	if !foundLoginAck {
		t.Errorf("LOGINACK token not found in response")
	}

	// Wait for accept to complete
	select {
	case err := <-acceptErr:
		t.Fatalf("Accept failed: %v", err)
	case serverConn := <-acceptConn:
		defer serverConn.Close()
		t.Log("Server accepted connection successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for accept")
	}
}

// buildTDSPacket wraps data in a TDS packet with header.
func buildTDSPacket(pktType tds.PacketType, data []byte) []byte {
	pkt := make([]byte, 8+len(data))
	pkt[0] = byte(pktType)
	pkt[1] = byte(tds.StatusEOM)
	binary.BigEndian.PutUint16(pkt[2:4], uint16(len(pkt)))
	pkt[4] = 0 // SPID high
	pkt[5] = 0 // SPID low
	pkt[6] = 1 // Packet ID
	pkt[7] = 0 // Window
	copy(pkt[8:], data)
	return pkt
}

// buildPreloginRequest builds a minimal PRELOGIN request.
func buildPreloginRequest() []byte {
	var buf bytes.Buffer

	// Option headers (each: 1 byte token, 2 bytes offset, 2 bytes length)
	// VERSION at offset 21, length 6
	buf.WriteByte(tds.PreloginVersion)
	binary.Write(&buf, binary.BigEndian, uint16(21)) // offset
	binary.Write(&buf, binary.BigEndian, uint16(6))  // length

	// ENCRYPTION at offset 27, length 1
	buf.WriteByte(tds.PreloginEncryption)
	binary.Write(&buf, binary.BigEndian, uint16(27))
	binary.Write(&buf, binary.BigEndian, uint16(1))

	// INSTOPT at offset 28, length 1
	buf.WriteByte(tds.PreloginInstOpt)
	binary.Write(&buf, binary.BigEndian, uint16(28))
	binary.Write(&buf, binary.BigEndian, uint16(1))

	// THREADID at offset 29, length 4
	buf.WriteByte(tds.PreloginThreadID)
	binary.Write(&buf, binary.BigEndian, uint16(29))
	binary.Write(&buf, binary.BigEndian, uint16(4))

	// Terminator
	buf.WriteByte(tds.PreloginTerminator)

	// Option data
	// VERSION: 15.0.2000.0 (SQL Server 2019-like)
	buf.WriteByte(15)                                    // Major
	buf.WriteByte(0)                                     // Minor
	binary.Write(&buf, binary.BigEndian, uint16(2000))   // Build
	binary.Write(&buf, binary.BigEndian, uint16(0))      // SubBuild

	// ENCRYPTION: request off
	buf.WriteByte(tds.EncryptOff)

	// INSTOPT: empty (null terminator)
	buf.WriteByte(0)

	// THREADID
	binary.Write(&buf, binary.BigEndian, uint32(12345))

	return buf.Bytes()
}

// buildLogin7Request builds a minimal LOGIN7 request.
func buildLogin7Request(user, pass, database, appName string) []byte {
	var buf bytes.Buffer

	// Fixed header size
	const headerSize = 94

	// Convert strings to UCS-2
	userBytes := stringToUCS2(user)
	passBytes := manglePassword(pass)
	dbBytes := stringToUCS2(database)
	appBytes := stringToUCS2(appName)
	ctlIntBytes := stringToUCS2("go-test")

	// Calculate offsets (all in bytes from start)
	offset := uint16(headerSize)

	hostOffset := offset
	hostLen := uint16(0) // Empty hostname
	offset += hostLen * 2

	userOffset := offset
	userLen := uint16(len(user))
	offset += uint16(len(userBytes))

	passOffset := offset
	passLen := uint16(len(pass))
	offset += uint16(len(passBytes))

	appOffset := offset
	appLen := uint16(len(appName))
	offset += uint16(len(appBytes))

	serverOffset := offset
	serverLen := uint16(0)

	extOffset := uint16(0)
	extLen := uint16(0)

	ctlIntOffset := offset
	ctlIntLen := uint16(7) // "go-test"
	offset += uint16(len(ctlIntBytes))

	langOffset := offset
	langLen := uint16(0)

	dbOffset := offset
	dbLen := uint16(len(database))
	offset += uint16(len(dbBytes))

	// Total length
	totalLen := uint32(offset)

	// Write fixed header
	binary.Write(&buf, binary.LittleEndian, totalLen)     // Length
	binary.Write(&buf, binary.LittleEndian, tds.VerTDS74) // TDS Version
	binary.Write(&buf, binary.LittleEndian, uint32(4096)) // Packet size
	binary.Write(&buf, binary.LittleEndian, uint32(0))    // ClientProgVer
	binary.Write(&buf, binary.LittleEndian, uint32(1234)) // ClientPID
	binary.Write(&buf, binary.LittleEndian, uint32(0))    // ConnectionID
	buf.WriteByte(0xE0)                                   // OptionFlags1 (DUMPLOAD_ON, USE_DB_ON, SET_LANG_ON)
	buf.WriteByte(0x03)                                   // OptionFlags2 (LANGUAGE_FATAL, ODBC)
	buf.WriteByte(0x00)                                   // TypeFlags
	buf.WriteByte(0x00)                                   // OptionFlags3
	binary.Write(&buf, binary.LittleEndian, int32(-240))  // ClientTimeZone (UTC-4)
	binary.Write(&buf, binary.LittleEndian, uint32(1033)) // ClientLCID (English)

	// Write offset/length pairs
	binary.Write(&buf, binary.LittleEndian, hostOffset)
	binary.Write(&buf, binary.LittleEndian, hostLen)
	binary.Write(&buf, binary.LittleEndian, userOffset)
	binary.Write(&buf, binary.LittleEndian, userLen)
	binary.Write(&buf, binary.LittleEndian, passOffset)
	binary.Write(&buf, binary.LittleEndian, passLen)
	binary.Write(&buf, binary.LittleEndian, appOffset)
	binary.Write(&buf, binary.LittleEndian, appLen)
	binary.Write(&buf, binary.LittleEndian, serverOffset)
	binary.Write(&buf, binary.LittleEndian, serverLen)
	binary.Write(&buf, binary.LittleEndian, extOffset)
	binary.Write(&buf, binary.LittleEndian, extLen)
	binary.Write(&buf, binary.LittleEndian, ctlIntOffset)
	binary.Write(&buf, binary.LittleEndian, ctlIntLen)
	binary.Write(&buf, binary.LittleEndian, langOffset)
	binary.Write(&buf, binary.LittleEndian, langLen)
	binary.Write(&buf, binary.LittleEndian, dbOffset)
	binary.Write(&buf, binary.LittleEndian, dbLen)

	// ClientID (6 bytes)
	buf.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	// SSPI offset/length
	binary.Write(&buf, binary.LittleEndian, uint16(0))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// AtchDBFile offset/length
	binary.Write(&buf, binary.LittleEndian, uint16(0))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// ChangePassword offset/length
	binary.Write(&buf, binary.LittleEndian, uint16(0))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// SSPILong
	binary.Write(&buf, binary.LittleEndian, uint32(0))

	// Write variable data
	buf.Write(userBytes)
	buf.Write(passBytes)
	buf.Write(appBytes)
	buf.Write(ctlIntBytes)
	buf.Write(dbBytes)

	return buf.Bytes()
}

// stringToUCS2 converts a string to UTF-16LE bytes.
func stringToUCS2(s string) []byte {
	b := make([]byte, len(s)*2)
	for i, r := range s {
		b[i*2] = byte(r)
		b[i*2+1] = byte(r >> 8)
	}
	return b
}

// manglePassword applies TDS password obfuscation.
func manglePassword(pass string) []byte {
	ucs2 := stringToUCS2(pass)
	for i := range ucs2 {
		b := ucs2[i]
		// Swap nibbles and XOR with 0xA5
		ucs2[i] = ((b << 4) | (b >> 4)) ^ 0xA5
	}
	return ucs2
}

func TestSpExecuteSQLParsing(t *testing.T) {
	// This test verifies that sp_executesql RPC requests are correctly
	// parsed into protocol.Request with SQL and parameters extracted.
	
	cfg := protocol.ListenerConfig{
		Name:     "test-tds",
		Protocol: protocol.ProtocolTDS,
		Host:     "127.0.0.1",
		Port:     0,
		Options: map[string]interface{}{
			"server_name": "test-server",
		},
	}

	logger := log.New(log.DefaultConfig())

	listener, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	if err := listener.Listen(); err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Channel to receive accepted connection
	connChan := make(chan protocol.Connection, 1)
	errChan := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errChan <- err
			return
		}
		connChan <- conn
	}()

	// Connect and perform handshake
	tcpConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer tcpConn.Close()

	// PRELOGIN
	preloginData := buildPreloginRequest()
	pkt := buildTDSPacket(tds.PacketPrelogin, preloginData)
	tcpConn.Write(pkt)

	// Read PRELOGIN response
	tcpConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respBuf := make([]byte, 4096)
	tcpConn.Read(respBuf)

	// LOGIN7
	loginData := buildLogin7Request("testuser", "testpass", "master", "testapp")
	pkt = buildTDSPacket(tds.PacketLogin7, loginData)
	tcpConn.Write(pkt)

	// Read LOGIN response
	tcpConn.Read(respBuf)

	// Wait for accepted connection
	var serverConn protocol.Connection
	select {
	case serverConn = <-connChan:
		// Got connection
	case err := <-errChan:
		t.Fatalf("Accept error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for connection")
	}
	defer serverConn.Close()

	// Now send sp_executesql RPC request
	rpcData := buildSpExecuteSQLRequest("SELECT @id, @name", map[string]interface{}{
		"id":   int32(42),
		"name": "Alice",
	})
	pkt = buildTDSPacket(tds.PacketRPCRequest, rpcData)
	tcpConn.Write(pkt)

	// Read request on server side
	req, err := serverConn.ReadRequest()
	if err != nil {
		t.Fatalf("Failed to read request: %v", err)
	}

	// Verify request
	if req.Type != protocol.RequestQuery {
		t.Errorf("Request type = %v, want RequestQuery", req.Type)
	}
	if req.SQL != "SELECT @id, @name" {
		t.Errorf("SQL = %q, want %q", req.SQL, "SELECT @id, @name")
	}
	if req.ProcedureName != "sp_executesql" {
		t.Errorf("ProcedureName = %q, want %q", req.ProcedureName, "sp_executesql")
	}

	// Check parameters
	if v, ok := req.Parameters["id"].(int64); !ok || v != 42 {
		t.Errorf("Parameter id = %v (%T), want 42 (int64)", req.Parameters["id"], req.Parameters["id"])
	}
	if v, ok := req.Parameters["name"].(string); !ok || v != "Alice" {
		t.Errorf("Parameter name = %v, want Alice", req.Parameters["name"])
	}

	t.Log("sp_executesql request parsed successfully")
}

// buildSpExecuteSQLRequest builds an sp_executesql RPC request packet.
func buildSpExecuteSQLRequest(sql string, params map[string]interface{}) []byte {
	var buf bytes.Buffer

	// ALL_HEADERS (minimal)
	binary.Write(&buf, binary.LittleEndian, uint32(4))

	// Procedure ID = sp_executesql (10)
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(10))

	// Option flags
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// Parameter 1: @stmt NVARCHAR = sql
	buf.WriteByte(0) // no name (positional)
	buf.WriteByte(0) // not output
	buf.WriteByte(byte(tds.TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34}) // collation
	sqlBytes := stringToUCS2(sql)
	binary.Write(&buf, binary.LittleEndian, uint16(len(sqlBytes)))
	buf.Write(sqlBytes)

	// Parameter 2: @params NVARCHAR = parameter definitions
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(byte(tds.TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	paramDef := "@id INT, @name NVARCHAR(100)"
	paramDefBytes := stringToUCS2(paramDef)
	binary.Write(&buf, binary.LittleEndian, uint16(len(paramDefBytes)))
	buf.Write(paramDefBytes)

	// Parameter 3: @id INT = 42
	idName := stringToUCS2("@id")
	buf.WriteByte(byte(len("@id")))
	buf.Write(idName)
	buf.WriteByte(0) // not output
	buf.WriteByte(byte(tds.TypeIntN))
	buf.WriteByte(4) // max size
	buf.WriteByte(4) // actual size
	binary.Write(&buf, binary.LittleEndian, params["id"].(int32))

	// Parameter 4: @name NVARCHAR = 'Alice'
	nameName := stringToUCS2("@name")
	buf.WriteByte(byte(len("@name")))
	buf.Write(nameName)
	buf.WriteByte(0)
	buf.WriteByte(byte(tds.TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(200))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	nameVal := stringToUCS2(params["name"].(string))
	binary.Write(&buf, binary.LittleEndian, uint16(len(nameVal)))
	buf.Write(nameVal)

	return buf.Bytes()
}
