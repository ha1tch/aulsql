package tds

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// Token types for TDS response stream.
type TokenType uint8

const (
	TokenReturnStatus  TokenType = 0x79 // 121
	TokenColMetadata   TokenType = 0x81 // 129
	TokenOrder         TokenType = 0xA9 // 169
	TokenError         TokenType = 0xAA // 170
	TokenInfo          TokenType = 0xAB // 171
	TokenReturnValue   TokenType = 0xAC // 172
	TokenLoginAck      TokenType = 0xAD // 173
	TokenFeatureExtAck TokenType = 0xAE // 174
	TokenRow           TokenType = 0xD1 // 209
	TokenNBCRow        TokenType = 0xD2 // 210
	TokenEnvChange     TokenType = 0xE3 // 227
	TokenSSPI          TokenType = 0xED // 237
	TokenFedAuthInfo   TokenType = 0xEE // 238
	TokenDone          TokenType = 0xFD // 253
	TokenDoneProc      TokenType = 0xFE // 254
	TokenDoneInProc    TokenType = 0xFF // 255
)

func (t TokenType) String() string {
	switch t {
	case TokenReturnStatus:
		return "RETURNSTATUS"
	case TokenColMetadata:
		return "COLMETADATA"
	case TokenOrder:
		return "ORDER"
	case TokenError:
		return "ERROR"
	case TokenInfo:
		return "INFO"
	case TokenReturnValue:
		return "RETURNVALUE"
	case TokenLoginAck:
		return "LOGINACK"
	case TokenFeatureExtAck:
		return "FEATUREEXTACK"
	case TokenRow:
		return "ROW"
	case TokenNBCRow:
		return "NBCROW"
	case TokenEnvChange:
		return "ENVCHANGE"
	case TokenSSPI:
		return "SSPI"
	case TokenFedAuthInfo:
		return "FEDAUTHINFO"
	case TokenDone:
		return "DONE"
	case TokenDoneProc:
		return "DONEPROC"
	case TokenDoneInProc:
		return "DONEINPROC"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(t))
	}
}

// Done status flags.
const (
	DoneFinal    uint16 = 0x0000
	DoneMore     uint16 = 0x0001
	DoneError    uint16 = 0x0002
	DoneInxact   uint16 = 0x0004 // Transaction in progress
	DoneCount    uint16 = 0x0010 // Row count valid
	DoneAttn     uint16 = 0x0020 // Acknowledging attention
	DoneSrvError uint16 = 0x0100 // Server error
)

// ENVCHANGE types.
const (
	EnvDatabase           uint8 = 1
	EnvLanguage           uint8 = 2
	EnvCharset            uint8 = 3
	EnvPacketSize         uint8 = 4
	EnvSortID             uint8 = 5
	EnvSortFlags          uint8 = 6
	EnvSQLCollation       uint8 = 7
	EnvBeginTran          uint8 = 8
	EnvCommitTran         uint8 = 9
	EnvRollbackTran       uint8 = 10
	EnvEnlistDTC          uint8 = 11
	EnvDefectTran         uint8 = 12
	EnvMirrorPartner      uint8 = 13
	EnvPromoteTran        uint8 = 15
	EnvTranMgrAddr        uint8 = 16
	EnvTranEnded          uint8 = 17
	EnvResetConnAck       uint8 = 18
	EnvStartedInstanceName uint8 = 19
	EnvRouting            uint8 = 20
)

// LoginAckInterface represents the interface type in LOGINACK.
type LoginAckInterface uint8

const (
	LoginAckSQL70    LoginAckInterface = 0x70
	LoginAckSQL2000  LoginAckInterface = 0x71
	LoginAckSQL2005  LoginAckInterface = 0x72
	LoginAckSQL2008  LoginAckInterface = 0x73
	LoginAckSQL2012  LoginAckInterface = 0x74
)

// TokenWriter helps build TDS token streams.
type TokenWriter struct {
	buf bytes.Buffer
}

// NewTokenWriter creates a new token writer.
func NewTokenWriter() *TokenWriter {
	return &TokenWriter{}
}

// Bytes returns the accumulated token stream bytes.
func (w *TokenWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// Reset clears the buffer.
func (w *TokenWriter) Reset() {
	w.buf.Reset()
}

// WriteEnvChange writes an ENVCHANGE token.
func (w *TokenWriter) WriteEnvChange(envType uint8, newValue, oldValue string) {
	newBytes := stringToUCS2(newValue)
	oldBytes := stringToUCS2(oldValue)

	// Token format:
	// BYTE TokenType (0xE3)
	// USHORT Length
	// BYTE Type
	// BYTE NewValueLength (in characters)
	// USHORT[] NewValue
	// BYTE OldValueLength (in characters)
	// USHORT[] OldValue

	newLen := len(newValue)
	oldLen := len(oldValue)
	tokenLen := 1 + 1 + len(newBytes) + 1 + len(oldBytes)

	w.buf.WriteByte(byte(TokenEnvChange))
	binary.Write(&w.buf, binary.LittleEndian, uint16(tokenLen))
	w.buf.WriteByte(envType)
	w.buf.WriteByte(byte(newLen))
	w.buf.Write(newBytes)
	w.buf.WriteByte(byte(oldLen))
	w.buf.Write(oldBytes)
}

// WriteEnvChangeCollation writes an ENVCHANGE token for SQL collation.
func (w *TokenWriter) WriteEnvChangeCollation(newCollation, oldCollation []byte) {
	// Collation is 5 bytes: 4 bytes LCID + sort flags, 1 byte sort ID
	tokenLen := 1 + 1 + len(newCollation) + 1 + len(oldCollation)

	w.buf.WriteByte(byte(TokenEnvChange))
	binary.Write(&w.buf, binary.LittleEndian, uint16(tokenLen))
	w.buf.WriteByte(EnvSQLCollation)
	w.buf.WriteByte(byte(len(newCollation)))
	w.buf.Write(newCollation)
	w.buf.WriteByte(byte(len(oldCollation)))
	w.buf.Write(oldCollation)
}

// WriteLoginAck writes a LOGINACK token.
func (w *TokenWriter) WriteLoginAck(iface LoginAckInterface, tdsVersion uint32, progName string, progVersion uint32) {
	progNameBytes := stringToUCS2(progName)

	// Token format:
	// BYTE TokenType (0xAD)
	// USHORT Length
	// BYTE Interface
	// DWORD TDSVersion
	// BYTE ProgNameLength (in characters)
	// USHORT[] ProgName
	// DWORD ProgVersion (major.minor.build as bytes)

	tokenLen := 1 + 4 + 1 + len(progNameBytes) + 4

	w.buf.WriteByte(byte(TokenLoginAck))
	binary.Write(&w.buf, binary.LittleEndian, uint16(tokenLen))
	w.buf.WriteByte(byte(iface))
	binary.Write(&w.buf, binary.BigEndian, tdsVersion) // TDS version is big-endian here
	w.buf.WriteByte(byte(len(progName)))
	w.buf.Write(progNameBytes)
	binary.Write(&w.buf, binary.BigEndian, progVersion)
}

// WriteDone writes a DONE token.
func (w *TokenWriter) WriteDone(status uint16, curCmd uint16, rowCount uint64) {
	w.buf.WriteByte(byte(TokenDone))
	binary.Write(&w.buf, binary.LittleEndian, status)
	binary.Write(&w.buf, binary.LittleEndian, curCmd)
	binary.Write(&w.buf, binary.LittleEndian, rowCount)
}

// WriteDoneProc writes a DONEPROC token.
func (w *TokenWriter) WriteDoneProc(status uint16, curCmd uint16, rowCount uint64) {
	w.buf.WriteByte(byte(TokenDoneProc))
	binary.Write(&w.buf, binary.LittleEndian, status)
	binary.Write(&w.buf, binary.LittleEndian, curCmd)
	binary.Write(&w.buf, binary.LittleEndian, rowCount)
}

// WriteDoneInProc writes a DONEINPROC token.
func (w *TokenWriter) WriteDoneInProc(status uint16, curCmd uint16, rowCount uint64) {
	w.buf.WriteByte(byte(TokenDoneInProc))
	binary.Write(&w.buf, binary.LittleEndian, status)
	binary.Write(&w.buf, binary.LittleEndian, curCmd)
	binary.Write(&w.buf, binary.LittleEndian, rowCount)
}

// WriteError writes an ERROR token.
func (w *TokenWriter) WriteError(number int32, state, class uint8, message, serverName, procName string, lineNumber int32) {
	msgBytes := stringToUCS2(message)
	serverBytes := stringToUCS2(serverName)
	procBytes := stringToUCS2(procName)

	// Token format:
	// BYTE TokenType (0xAA)
	// USHORT Length
	// LONG Number
	// BYTE State
	// BYTE Class
	// USHORT MsgLength
	// USHORT[] Message
	// BYTE ServerNameLength
	// USHORT[] ServerName
	// BYTE ProcNameLength
	// USHORT[] ProcName
	// LONG LineNumber (USHORT for older TDS)

	tokenLen := 4 + 1 + 1 + 2 + len(msgBytes) + 1 + len(serverBytes) + 1 + len(procBytes) + 4

	w.buf.WriteByte(byte(TokenError))
	binary.Write(&w.buf, binary.LittleEndian, uint16(tokenLen))
	binary.Write(&w.buf, binary.LittleEndian, number)
	w.buf.WriteByte(state)
	w.buf.WriteByte(class)
	binary.Write(&w.buf, binary.LittleEndian, uint16(len(message)))
	w.buf.Write(msgBytes)
	w.buf.WriteByte(byte(len(serverName)))
	w.buf.Write(serverBytes)
	w.buf.WriteByte(byte(len(procName)))
	w.buf.Write(procBytes)
	binary.Write(&w.buf, binary.LittleEndian, lineNumber)
}

// WriteInfo writes an INFO token (same format as ERROR but different token type).
func (w *TokenWriter) WriteInfo(number int32, state, class uint8, message, serverName, procName string, lineNumber int32) {
	msgBytes := stringToUCS2(message)
	serverBytes := stringToUCS2(serverName)
	procBytes := stringToUCS2(procName)

	tokenLen := 4 + 1 + 1 + 2 + len(msgBytes) + 1 + len(serverBytes) + 1 + len(procBytes) + 4

	w.buf.WriteByte(byte(TokenInfo))
	binary.Write(&w.buf, binary.LittleEndian, uint16(tokenLen))
	binary.Write(&w.buf, binary.LittleEndian, number)
	w.buf.WriteByte(state)
	w.buf.WriteByte(class)
	binary.Write(&w.buf, binary.LittleEndian, uint16(len(message)))
	w.buf.Write(msgBytes)
	w.buf.WriteByte(byte(len(serverName)))
	w.buf.Write(serverBytes)
	w.buf.WriteByte(byte(len(procName)))
	w.buf.Write(procBytes)
	binary.Write(&w.buf, binary.LittleEndian, lineNumber)
}

// WriteReturnStatus writes a RETURNSTATUS token.
func (w *TokenWriter) WriteReturnStatus(value int32) {
	w.buf.WriteByte(byte(TokenReturnStatus))
	binary.Write(&w.buf, binary.LittleEndian, value)
}

// WriteReturnValue writes a RETURNVALUE token for output parameters.
// This is used to return the value of output parameters after procedure execution.
func (w *TokenWriter) WriteReturnValue(ordinal uint16, paramName string, status uint8, userType uint32, col Column, value interface{}) {
	w.buf.WriteByte(byte(TokenReturnValue))

	// Build the token payload first to calculate length
	var payload bytes.Buffer

	// ParamOrdinal (USHORT)
	binary.Write(&payload, binary.LittleEndian, ordinal)

	// ParamName (B_VARCHAR: 1-byte length in chars, UTF-16LE string)
	paramNameBytes := stringToUCS2(paramName)
	payload.WriteByte(byte(len(paramName)))
	payload.Write(paramNameBytes)

	// Status (BYTE) - 0x01 if output param, 0x02 if user-defined type
	payload.WriteByte(status)

	// UserType (ULONG for TDS 7.2+)
	binary.Write(&payload, binary.LittleEndian, userType)

	// Flags (USHORT)
	flags := col.Flags
	if col.Nullable {
		flags |= ColFlagNullable
	}
	binary.Write(&payload, binary.LittleEndian, flags)

	// TYPE_INFO - write type and metadata
	writeTypeInfo(&payload, col)

	// Value
	writeValue(&payload, value, col)

	// Write length and payload
	binary.Write(&w.buf, binary.LittleEndian, uint16(payload.Len()))
	w.buf.Write(payload.Bytes())
}

// writeTypeInfo writes the TYPE_INFO portion for a return value.
func writeTypeInfo(buf *bytes.Buffer, col Column) {
	buf.WriteByte(byte(col.Type))

	switch col.Type {
	case TypeNull:
		// No additional info

	case TypeInt1, TypeBit, TypeInt2, TypeInt4, TypeInt8,
		TypeFloat4, TypeFloat8, TypeMoney, TypeMoney4,
		TypeDateTime, TypeDateTime4:
		// Fixed-length types: no additional info

	case TypeIntN:
		buf.WriteByte(byte(col.Length)) // 1, 2, 4, or 8

	case TypeBitN:
		buf.WriteByte(byte(col.Length)) // 1

	case TypeFloatN:
		buf.WriteByte(byte(col.Length)) // 4 or 8

	case TypeMoneyN:
		buf.WriteByte(byte(col.Length)) // 4 or 8

	case TypeDateTimeN:
		buf.WriteByte(byte(col.Length)) // 4 or 8

	case TypeDateN:
		// No additional info

	case TypeTimeN, TypeDateTime2N, TypeDateTimeOffsetN:
		buf.WriteByte(col.Scale) // Scale (0-7)

	case TypeDecimalN, TypeNumericN:
		buf.WriteByte(byte(col.Length)) // Max length
		buf.WriteByte(col.Precision)
		buf.WriteByte(col.Scale)

	case TypeGUID:
		buf.WriteByte(byte(col.Length)) // 16 or 0 for nullable

	case TypeChar, TypeVarChar, TypeBinary, TypeVarBinary:
		buf.WriteByte(byte(col.Length))
		if col.Type == TypeChar || col.Type == TypeVarChar {
			if len(col.Collation) >= 5 {
				buf.Write(col.Collation[:5])
			} else {
				buf.Write(DefaultCollation)
			}
		}

	case TypeBigVarChar, TypeBigChar, TypeBigVarBin, TypeBigBinary:
		binary.Write(buf, binary.LittleEndian, uint16(col.Length))
		if col.Type == TypeBigVarChar || col.Type == TypeBigChar {
			if len(col.Collation) >= 5 {
				buf.Write(col.Collation[:5])
			} else {
				buf.Write(DefaultCollation)
			}
		}

	case TypeNVarChar, TypeNChar:
		binary.Write(buf, binary.LittleEndian, uint16(col.Length))
		if len(col.Collation) >= 5 {
			buf.Write(col.Collation[:5])
		} else {
			buf.Write(DefaultCollation)
		}
	}
}

// writeValue writes a value for a return value token.
func writeValue(buf *bytes.Buffer, val interface{}, col Column) {
	if val == nil {
		writeNullValue(buf, col)
		return
	}

	switch col.Type {
	case TypeIntN:
		v, _ := toInt64(val)
		buf.WriteByte(byte(col.Length))
		switch col.Length {
		case 1:
			buf.WriteByte(byte(v))
		case 2:
			binary.Write(buf, binary.LittleEndian, int16(v))
		case 4:
			binary.Write(buf, binary.LittleEndian, int32(v))
		case 8:
			binary.Write(buf, binary.LittleEndian, v)
		}

	case TypeBitN:
		v, _ := toBool(val)
		buf.WriteByte(1)
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

	case TypeFloatN:
		v, _ := toFloat64(val)
		buf.WriteByte(byte(col.Length))
		if col.Length == 4 {
			binary.Write(buf, binary.LittleEndian, float32(v))
		} else {
			binary.Write(buf, binary.LittleEndian, v)
		}

	case TypeNVarChar, TypeNChar:
		s := toString(val)
		data := stringToUCS2(s)
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	case TypeBigVarChar, TypeBigChar:
		s := toString(val)
		data := []byte(s)
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	case TypeBigVarBin, TypeBigBinary:
		data, _ := toBytes(val)
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	case TypeGUID:
		s := toString(val)
		// Parse GUID string and write bytes
		buf.WriteByte(16)
		buf.Write(parseGUIDString(s))

	case TypeDecimalN, TypeNumericN:
		s := toString(val)
		writeDecimalValue(buf, s, col.Precision, col.Scale)

	default:
		// For other types, write null
		writeNullValue(buf, col)
	}
}

// writeNullValue writes a NULL value for the given column type.
func writeNullValue(buf *bytes.Buffer, col Column) {
	switch col.Type {
	case TypeIntN, TypeBitN, TypeFloatN, TypeMoneyN, TypeDateTimeN, TypeGUID,
		TypeDecimalN, TypeNumericN, TypeDateN, TypeTimeN, TypeDateTime2N, TypeDateTimeOffsetN:
		buf.WriteByte(0) // 0 length = NULL

	case TypeNVarChar, TypeNChar, TypeBigVarChar, TypeBigChar,
		TypeBigVarBin, TypeBigBinary:
		binary.Write(buf, binary.LittleEndian, uint16(0xFFFF)) // -1 = NULL

	default:
		buf.WriteByte(0)
	}
}

// parseGUIDString parses a GUID string into bytes.
func parseGUIDString(s string) []byte {
	// Remove dashes and parse hex
	result := make([]byte, 16)
	// Simple implementation - in production would need better parsing
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return result
	}
	for i := 0; i < 16; i++ {
		fmt.Sscanf(s[i*2:i*2+2], "%02x", &result[i])
	}
	// Swap bytes for SQL Server byte order
	result[0], result[3] = result[3], result[0]
	result[1], result[2] = result[2], result[1]
	result[4], result[5] = result[5], result[4]
	result[6], result[7] = result[7], result[6]
	return result
}

// writeDecimalValue writes a decimal/numeric value.
func writeDecimalValue(buf *bytes.Buffer, s string, precision, scale uint8) {
	// Parse the decimal string
	negative := false
	if len(s) > 0 && s[0] == '-' {
		negative = true
		s = s[1:]
	}

	// Remove decimal point and track position
	dotPos := strings.Index(s, ".")
	if dotPos >= 0 {
		s = s[:dotPos] + s[dotPos+1:]
	}

	// Parse as integer
	var val uint64
	fmt.Sscanf(s, "%d", &val)

	// Calculate byte length needed
	byteLen := byte(5) // minimum: 1 sign + 4 bytes for value
	if precision > 9 {
		byteLen = 9
	}
	if precision > 19 {
		byteLen = 13
	}
	if precision > 28 {
		byteLen = 17
	}

	buf.WriteByte(byteLen)
	if negative {
		buf.WriteByte(0) // negative
	} else {
		buf.WriteByte(1) // positive
	}

	// Write value as little-endian bytes
	for i := byte(0); i < byteLen-1; i++ {
		buf.WriteByte(byte(val & 0xFF))
		val >>= 8
	}
}

// WritePacket writes the token stream as a TDS packet.
func (w *TokenWriter) WritePacket(writer io.Writer, spid uint16, packetID uint8) error {
	data := w.Bytes()

	hdr := Header{
		Type:     PacketReply,
		Status:   StatusEOM,
		Length:   uint16(HeaderSize + len(data)),
		SPID:     spid,
		PacketID: packetID,
		Window:   0,
	}

	if err := hdr.Write(writer); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}
