package tds

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/tds"
)

// Connection implements protocol.Connection for TDS clients.
type Connection struct {
	listener   *Listener
	tdsConn    *tds.Conn
	logger     *log.Logger
	spid       uint16
	serverName string

	// Connection state (set during login)
	user       string
	database   string
	appName    string
	clientHost string
	tdsVersion uint32
	packetSize int

	// TLS configuration (nil means no TLS support)
	tlsConfig *tls.Config

	// Authentication callback (can be set by application)
	Authenticator Authenticator

	// Phase 3: Advanced features
	phase3      *Phase3Handlers
	phase3State *ConnectionPhase3State

	closed bool
}

// Authenticator validates login credentials.
type Authenticator interface {
	// Authenticate validates the username and password.
	// Returns nil if authentication succeeds.
	Authenticate(username, password, database string) error
}

// DefaultAuthenticator accepts all logins (for development only).
type DefaultAuthenticator struct{}

func (d DefaultAuthenticator) Authenticate(username, password, database string) error {
	return nil
}

// handshake performs the TDS connection handshake.
// Flow: PRELOGIN → (optional TLS) → LOGIN7 → LOGINACK
func (c *Connection) handshake() error {
	// Step 1: Read PRELOGIN
	pktType, data, err := c.tdsConn.ReadPacket()
	if err != nil {
		return fmt.Errorf("reading prelogin: %w", err)
	}
	if pktType != tds.PacketPrelogin {
		return fmt.Errorf("expected PRELOGIN packet, got %s", pktType)
	}

	prelogin, err := tds.ParsePrelogin(data)
	if err != nil {
		return fmt.Errorf("parsing prelogin: %w", err)
	}

	c.logger.Application().Debug("PRELOGIN received",
		"spid", c.spid,
		"encryption", prelogin.Encryption,
		"mars", prelogin.MARS,
		"instance", prelogin.Instance,
	)

	// Step 2: Send PRELOGIN response
	encryptResp := c.negotiateEncryption(prelogin.Encryption)
	preloginResp := &tds.PreloginResponse{
		Version:    c.listener.serverVersion,
		Encryption: encryptResp,
		Instance:   "",
		ThreadID:   uint32(c.spid),
		MARS:       0, // MARS not supported yet
	}

	if err := c.sendPreloginResponse(preloginResp); err != nil {
		return fmt.Errorf("sending prelogin response: %w", err)
	}

	// Step 3: If encryption was negotiated, perform TLS handshake
	if encryptResp == tds.EncryptOn || encryptResp == tds.EncryptReq {
		if err := c.performTLSHandshake(); err != nil {
			return fmt.Errorf("TLS handshake: %w", err)
		}
		c.logger.Application().Debug("TLS handshake completed", "spid", c.spid)
	}

	// Step 4: Read LOGIN7
	pktType, data, err = c.tdsConn.ReadPacket()
	if err != nil {
		return fmt.Errorf("reading login: %w", err)
	}
	if pktType != tds.PacketLogin7 {
		return fmt.Errorf("expected LOGIN7 packet, got %s", pktType)
	}

	login, err := tds.ParseLogin7(data)
	if err != nil {
		return fmt.Errorf("parsing login: %w", err)
	}

	c.logger.Application().Debug("LOGIN7 received",
		"spid", c.spid,
		"user", login.UserName,
		"database", login.Database,
		"app", login.AppName,
		"host", login.HostName,
		"tds_version", fmt.Sprintf("0x%08X", login.Header.TDSVersion),
	)

	// Step 5: Authenticate
	auth := c.Authenticator
	if auth == nil {
		auth = DefaultAuthenticator{}
	}

	if err := auth.Authenticate(login.UserName, login.Password, login.Database); err != nil {
		// Send login failed error
		if sendErr := c.sendLoginError(err.Error()); sendErr != nil {
			c.logger.Application().Error("failed to send login error", sendErr, "original_error", err)
		}
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Store connection state
	c.user = login.UserName
	c.database = login.Database
	if c.database == "" {
		c.database = "master" // Default database
	}
	c.appName = login.AppName
	c.clientHost = login.HostName
	c.tdsVersion = login.Header.TDSVersion
	c.packetSize = int(login.Header.PacketSize)
	if c.packetSize < tds.MinPacketSize {
		c.packetSize = tds.DefaultPacketSize
	}

	// Update TDS connection state
	c.tdsConn.SetUser(c.user)
	c.tdsConn.SetDatabase(c.database)
	c.tdsConn.SetAppName(c.appName)
	c.tdsConn.SetClientHost(c.clientHost)
	c.tdsConn.SetTDSVersion(c.tdsVersion)
	c.tdsConn.SetPacketSize(c.packetSize)

	// Step 6: Send LOGINACK response
	if err := c.sendLoginAck(); err != nil {
		return fmt.Errorf("sending login ack: %w", err)
	}

	return nil
}

// negotiateEncryption determines the encryption level based on client request and server config.
func (c *Connection) negotiateEncryption(clientEncrypt uint8) uint8 {
	// If we have TLS configured, we can support encryption
	if c.tlsConfig != nil {
		switch clientEncrypt {
		case tds.EncryptOff:
			// Client doesn't want encryption, but we might require it
			// For now, allow unencrypted if client requests it
			return tds.EncryptOff
		case tds.EncryptOn, tds.EncryptReq:
			// Client wants/requires encryption, and we support it
			return tds.EncryptOn
		default:
			return tds.EncryptNotSup
		}
	}

	// No TLS configured - we don't support encryption
	switch clientEncrypt {
	case tds.EncryptOff:
		return tds.EncryptOff
	case tds.EncryptOn, tds.EncryptReq:
		// Client requires encryption, but we don't support it
		return tds.EncryptNotSup
	default:
		return tds.EncryptNotSup
	}
}

// performTLSHandshake performs TLS handshake wrapped in TDS packets.
func (c *Connection) performTLSHandshake() error {
	if c.tlsConfig == nil {
		return fmt.Errorf("TLS not configured")
	}

	// Perform TDS-wrapped TLS handshake
	if err := c.tdsConn.UpgradeToTLS(c.tlsConfig); err != nil {
		return err
	}

	return nil
}

// sendPreloginResponse sends the PRELOGIN response packet.
func (c *Connection) sendPreloginResponse(resp *tds.PreloginResponse) error {
	data := resp.Encode()
	return c.tdsConn.WritePacket(tds.PacketReply, data)
}

// sendLoginAck sends the LOGINACK response with environment changes.
func (c *Connection) sendLoginAck() error {
	tw := tds.NewTokenWriter()

	// Send ENVCHANGE for database
	tw.WriteEnvChange(tds.EnvDatabase, c.database, "master")

	// Send ENVCHANGE for packet size
	tw.WriteEnvChange(tds.EnvPacketSize, fmt.Sprintf("%d", c.packetSize), fmt.Sprintf("%d", tds.DefaultPacketSize))

	// Send ENVCHANGE for collation
	tw.WriteEnvChangeCollation(tds.DefaultCollation, []byte{})

	// Send INFO message (optional welcome message)
	tw.WriteInfo(
		5701, // Standard "changed database context" message
		2,
		0,
		fmt.Sprintf("Changed database context to '%s'.", c.database),
		c.serverName,
		"",
		1,
	)

	// Send LOGINACK
	// TDS version in LOGINACK should match what client sent (or lower if we don't support it)
	loginTDSVersion := c.tdsVersion
	if loginTDSVersion > tds.VerTDS74 {
		loginTDSVersion = tds.VerTDS74 // Cap at TDS 7.4
	}

	tw.WriteLoginAck(
		tds.LoginAckSQL2012, // Interface type
		loginTDSVersion,
		c.serverName,
		0x0F000000, // Version 15.0.0.0 (SQL Server 2019-like)
	)

	// Send DONE
	tw.WriteDone(tds.DoneFinal, 0, 0)

	return c.tdsConn.WriteTokens(tw)
}

// sendLoginError sends a login failure response.
func (c *Connection) sendLoginError(message string) error {
	tw := tds.NewTokenWriter()

	// Send ERROR token
	tw.WriteError(
		18456, // SQL Server login failed error
		1,
		14, // Severity 14 = permission denied
		fmt.Sprintf("Login failed for user '%s'. %s", c.user, message),
		c.serverName,
		"",
		1,
	)

	// Send DONE with error flag
	tw.WriteDone(tds.DoneError|tds.DoneFinal, 0, 0)

	return c.tdsConn.WriteTokens(tw)
}

// ReadRequest reads the next request from the client.
func (c *Connection) ReadRequest() (protocol.Request, error) {
	pktType, status, data, err := c.tdsConn.ReadPacketWithStatus()
	if err != nil {
		return protocol.Request{}, err
	}

	// Check for connection reset request
	if status.IsResetConnection() {
		c.resetSession(status.IsResetConnectionSkipTran())
	}

	switch pktType {
	case tds.PacketSQLBatch:
		return c.parseSQLBatch(data)
	case tds.PacketRPCRequest:
		return c.parseRPCRequest(data)
	case tds.PacketAttention:
		return protocol.Request{Type: protocol.RequestCancel}, nil
	default:
		return protocol.Request{}, fmt.Errorf("unsupported packet type: %s", pktType)
	}
}

// resetSession resets the connection state (called on StatusResetConnection).
func (c *Connection) resetSession(skipTran bool) {
	// Reset session settings to defaults
	// In a full implementation, this would reset SET options, temp tables, etc.
	
	// Send ENVCHANGE to confirm reset if needed
	// For now, we just reset our internal state
	
	// Note: skipTran means preserve the current transaction state
	// Without skipTran, we would also rollback any active transaction
}

// parseSQLBatch parses a SQL_BATCH packet.
func (c *Connection) parseSQLBatch(data []byte) (protocol.Request, error) {
	// SQL_BATCH format for TDS 7.2+:
	// - ALL_HEADERS (optional, variable length)
	// - SQLText (UNICODESTREAM)

	// Skip headers if present (TDS 7.2+)
	offset := 0
	if c.tdsVersion >= tds.VerTDS72 && len(data) >= 4 {
		// Total length of ALL_HEADERS
		totalLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
		if totalLen >= 4 && totalLen <= len(data) {
			offset = totalLen
		}
	}

	// Remaining data is the SQL text (UTF-16LE)
	sqlText := ucs2ToString(data[offset:])

	return protocol.Request{
		Type: protocol.RequestQuery,
		SQL:  sqlText,
	}, nil
}

// parseRPCRequest parses an RPC_REQUEST packet.
func (c *Connection) parseRPCRequest(data []byte) (protocol.Request, error) {
	rpcReq, err := tds.ParseRPCRequest(data, c.tdsVersion)
	if err != nil {
		return protocol.Request{}, fmt.Errorf("parsing RPC request: %w", err)
	}

	// Convert parameters to map
	params := make(map[string]interface{})
	for i, p := range rpcReq.Parameters {
		name := p.Name
		if name == "" {
			// Positional parameter
			name = fmt.Sprintf("p%d", i+1)
		}
		if !p.IsNull {
			params[name] = p.Value
		}
	}

	// Determine request type
	reqType := protocol.RequestExec
	sql := ""

	// Handle sp_executesql specially - extract SQL and params
	if rpcReq.ProcID == tds.ProcIDExecuteSQL {
		reqType = protocol.RequestQuery
		// First parameter is the SQL statement
		if len(rpcReq.Parameters) > 0 && !rpcReq.Parameters[0].IsNull {
			if s, ok := rpcReq.Parameters[0].Value.(string); ok {
				sql = s
			}
		}
		// Remove @stmt and @params from the parameter map
		// The actual parameters start at index 2
		actualParams := make(map[string]interface{})
		for i := 2; i < len(rpcReq.Parameters); i++ {
			p := rpcReq.Parameters[i]
			name := p.Name
			if name == "" {
				name = fmt.Sprintf("p%d", i-1) // Adjust numbering
			}
			if !p.IsNull {
				actualParams[name] = p.Value
			}
		}
		params = actualParams
	} else if rpcReq.ProcID > 0 {
		// Other system RPC
		reqType = protocol.RequestCall
	}

	return protocol.Request{
		Type:          reqType,
		SQL:           sql,
		ProcedureName: rpcReq.ProcName,
		Parameters:    params,
	}, nil
}

// SendResult sends a result to the client.
func (c *Connection) SendResult(result protocol.Result) error {
	tw := tds.NewTokenWriter()

	switch result.Type {
	case protocol.ResultError:
		// Send ERROR token
		errMsg := "An error occurred"
		if result.Error != nil {
			errMsg = result.Error.Error()
		}
		tw.WriteError(
			50000, // User-defined error
			1,
			16, // Severity 16 = general error
			errMsg,
			c.serverName,
			"",
			1,
		)
		tw.WriteDone(tds.DoneError|tds.DoneFinal, 0, 0)

	case protocol.ResultOK:
		// Send output parameters if present
		if len(result.OutputParams) > 0 {
			c.writeOutputParams(tw, result.OutputParams)
		}
		
		// Send DONE with row count
		status := tds.DoneFinal
		if result.RowsAffected > 0 {
			status |= tds.DoneCount
		}
		tw.WriteDone(status, 0, uint64(result.RowsAffected))

	case protocol.ResultInfo:
		// Send INFO message
		tw.WriteInfo(
			0,
			0,
			0,
			result.Message,
			c.serverName,
			"",
			1,
		)
		tw.WriteDone(tds.DoneFinal, 0, 0)

	case protocol.ResultRows:
		// Send result sets
		for _, rs := range result.ResultSets {
			if err := c.writeResultSet(tw, rs); err != nil {
				return err
			}
		}
		
		// Send output parameters if present
		if len(result.OutputParams) > 0 {
			c.writeOutputParams(tw, result.OutputParams)
		}
		
		tw.WriteDone(tds.DoneFinal, 0, uint64(result.RowsAffected))

	case protocol.ResultCancel:
		// Acknowledge attention/cancellation
		tw.WriteDone(tds.DoneAttn|tds.DoneFinal, 0, 0)

	default:
		tw.WriteDone(tds.DoneFinal, 0, 0)
	}

	return c.tdsConn.WriteTokens(tw)
}

// writeOutputParams writes RETURNVALUE tokens for output parameters.
func (c *Connection) writeOutputParams(tw *tds.TokenWriter, params map[string]interface{}) {
	ordinal := uint16(0)
	for name, value := range params {
		col := inferColumnFromValue(value)
		col.Name = name
		
		// Status: 0x01 = output parameter
		tw.WriteReturnValue(ordinal, "@"+name, 0x01, 0, col, value)
		ordinal++
	}
}

// inferColumnFromValue creates a tds.Column based on the Go value type.
func inferColumnFromValue(val interface{}) tds.Column {
	col := tds.Column{
		Nullable: true,
		Flags:    tds.ColFlagNullable,
	}
	
	if val == nil {
		col.Type = tds.TypeIntN
		col.Length = 4
		return col
	}
	
	switch v := val.(type) {
	case int, int32:
		col.Type = tds.TypeIntN
		col.Length = 4
	case int64:
		col.Type = tds.TypeIntN
		col.Length = 8
	case int16:
		col.Type = tds.TypeIntN
		col.Length = 2
	case int8:
		col.Type = tds.TypeIntN
		col.Length = 1
	case bool:
		col.Type = tds.TypeBitN
		col.Length = 1
	case float32:
		col.Type = tds.TypeFloatN
		col.Length = 4
	case float64:
		col.Type = tds.TypeFloatN
		col.Length = 8
	case string:
		col.Type = tds.TypeNVarChar
		length := len(v) * 2
		if length < 2 {
			length = 2
		}
		if length > 8000 {
			length = 8000
		}
		col.Length = uint32(length)
		col.Collation = tds.DefaultCollation
	case []byte:
		col.Type = tds.TypeBigVarBin
		length := len(v)
		if length < 1 {
			length = 1
		}
		if length > 8000 {
			length = 8000
		}
		col.Length = uint32(length)
	default:
		// Default to NVARCHAR for unknown types
		col.Type = tds.TypeNVarChar
		col.Length = 8000
		col.Collation = tds.DefaultCollation
		_ = v // silence unused warning
	}
	
	return col
}

// writeResultSet writes a single result set to the token stream.
func (c *Connection) writeResultSet(tw *tds.TokenWriter, rs protocol.ResultSet) error {
	// Convert protocol columns to TDS columns
	columns := make([]tds.Column, len(rs.Columns))
	for i, col := range rs.Columns {
		columns[i] = convertColumn(col)
	}

	// Create result set writer
	rsw := tds.NewResultSetWriter(tw, columns)

	// Write column metadata
	rsw.WriteColMetadata()

	// Write rows
	for _, row := range rs.Rows {
		if err := rsw.WriteRow(row); err != nil {
			return err
		}
	}

	// Write DONEINPROC
	rsw.WriteDoneInProc(uint64(len(rs.Rows)))

	return nil
}

// convertColumn converts a protocol.ColumnInfo to tds.Column.
func convertColumn(col protocol.ColumnInfo) tds.Column {
	tdsCol := tds.Column{
		Name:      col.Name,
		Nullable:  col.Nullable,
		Length:    uint32(col.Length),
		Scale:     uint8(col.Scale),
		Collation: tds.DefaultCollation,
	}

	// Map SQL type name to TDS type
	switch col.Type {
	case "INT", "int":
		tdsCol.Type = tds.TypeIntN
		tdsCol.Length = 4
	case "BIGINT", "bigint":
		tdsCol.Type = tds.TypeIntN
		tdsCol.Length = 8
	case "SMALLINT", "smallint":
		tdsCol.Type = tds.TypeIntN
		tdsCol.Length = 2
	case "TINYINT", "tinyint":
		tdsCol.Type = tds.TypeIntN
		tdsCol.Length = 1
	case "BIT", "bit":
		tdsCol.Type = tds.TypeBitN
		tdsCol.Length = 1
	case "FLOAT", "float":
		tdsCol.Type = tds.TypeFloatN
		tdsCol.Length = 8
	case "REAL", "real":
		tdsCol.Type = tds.TypeFloatN
		tdsCol.Length = 4
	case "VARCHAR", "varchar":
		tdsCol.Type = tds.TypeBigVarChar
		if col.Length == 0 {
			tdsCol.Length = 8000
		}
	case "NVARCHAR", "nvarchar":
		tdsCol.Type = tds.TypeNVarChar
		if col.Length == 0 {
			tdsCol.Length = 8000
		}
	case "CHAR", "char":
		tdsCol.Type = tds.TypeBigChar
		if col.Length == 0 {
			tdsCol.Length = 1
		}
	case "NCHAR", "nchar":
		tdsCol.Type = tds.TypeNChar
		if col.Length == 0 {
			tdsCol.Length = 1
		}
	case "DATETIME", "datetime":
		tdsCol.Type = tds.TypeDateTimeN
		tdsCol.Length = 8
	case "DATE", "date":
		tdsCol.Type = tds.TypeDateN
	case "UNIQUEIDENTIFIER", "uniqueidentifier":
		tdsCol.Type = tds.TypeGUID
		tdsCol.Length = 16
	case "VARBINARY", "varbinary":
		tdsCol.Type = tds.TypeBigVarBin
		if col.Length == 0 {
			tdsCol.Length = 8000
		}
	default:
		// Default to NVARCHAR for unknown types
		tdsCol.Type = tds.TypeNVarChar
		tdsCol.Length = 8000
	}

	return tdsCol
}

// Close closes the connection.
func (c *Connection) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	// Remove from listener tracking
	if c.listener != nil {
		c.listener.removeConnection(c.spid)
	}

	// Close TDS connection
	if c.tdsConn != nil {
		return c.tdsConn.Close()
	}
	return nil
}

// RemoteAddr returns the remote address.
func (c *Connection) RemoteAddr() net.Addr {
	if c.tdsConn != nil {
		return c.tdsConn.RemoteAddr()
	}
	return nil
}

// SetDeadline sets the read/write deadline.
func (c *Connection) SetDeadline(t time.Time) error {
	if c.tdsConn != nil && c.tdsConn.NetConn() != nil {
		return c.tdsConn.NetConn().SetDeadline(t)
	}
	return nil
}

// Properties returns connection properties for tenant identification.
func (c *Connection) Properties() map[string]string {
	props := make(map[string]string)
	if c.user != "" {
		props["user"] = c.user
	}
	if c.database != "" {
		props["database"] = c.database
	}
	if c.appName != "" {
		props["app_name"] = c.appName
	}
	if c.clientHost != "" {
		props["client_host"] = c.clientHost
	}
	return props
}

// ucs2ToString converts UTF-16LE bytes to a Go string.
func ucs2ToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}

	runes := make([]rune, len(b)/2)
	for i := 0; i < len(runes); i++ {
		runes[i] = rune(uint16(b[i*2]) | uint16(b[i*2+1])<<8)
	}
	return string(runes)
}
