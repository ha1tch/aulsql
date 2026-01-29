package tds

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
)

// Login7 option flags.
const (
	// OptionFlags1
	FlagByteOrder   uint8 = 0x01 // Byte order (0=little endian)
	FlagChar        uint8 = 0x02 // Character set (0=ASCII)
	FlagFloat       uint8 = 0x0C // Float representation
	FlagDumpLoad    uint8 = 0x10 // Dump/load off
	FlagUseDB       uint8 = 0x20 // USE DATABASE in login
	FlagDatabase    uint8 = 0x40 // Initial database fatal
	FlagSetLang     uint8 = 0x80 // SET LANGUAGE in login

	// OptionFlags2
	FlagLanguage     uint8 = 0x01 // Language fatal
	FlagODBC         uint8 = 0x02 // ODBC driver
	FlagTransBoundary uint8 = 0x04 // Transaction boundary
	FlagCacheConnect uint8 = 0x08 // Cache connect
	FlagUserType     uint8 = 0x70 // User type
	FlagIntSecurity  uint8 = 0x80 // Integrated security (SSPI)

	// OptionFlags3
	FlagChangePassword uint8 = 0x01 // Change password
	FlagBinaryXML      uint8 = 0x02 // Send Yukon binary XML
	FlagUserInstance   uint8 = 0x04 // User instance
	FlagUnknownCollation uint8 = 0x08 // Unknown collation handling
	FlagExtension      uint8 = 0x10 // Feature extension

	// TypeFlags
	FlagSQLType        uint8 = 0x0F // SQL type (4 bits)
	FlagOLEDB          uint8 = 0x10 // OLE DB
	FlagReadOnlyIntent uint8 = 0x20 // Read-only intent
)

// Login7HeaderSize is the fixed size of the LOGIN7 header.
const Login7HeaderSize = 94

// Login7Header represents the fixed portion of the LOGIN7 packet.
type Login7Header struct {
	Length               uint32
	TDSVersion           uint32
	PacketSize           uint32
	ClientProgVer        uint32
	ClientPID            uint32
	ConnectionID         uint32
	OptionFlags1         uint8
	OptionFlags2         uint8
	TypeFlags            uint8
	OptionFlags3         uint8
	ClientTimeZone       int32
	ClientLCID           uint32
	HostNameOffset       uint16
	HostNameLength       uint16
	UserNameOffset       uint16
	UserNameLength       uint16
	PasswordOffset       uint16
	PasswordLength       uint16
	AppNameOffset        uint16
	AppNameLength        uint16
	ServerNameOffset     uint16
	ServerNameLength     uint16
	ExtensionOffset      uint16
	ExtensionLength      uint16
	CtlIntNameOffset     uint16
	CtlIntNameLength     uint16
	LanguageOffset       uint16
	LanguageLength       uint16
	DatabaseOffset       uint16
	DatabaseLength       uint16
	ClientID             [6]byte
	SSPIOffset           uint16
	SSPILength           uint16
	AtchDBFileOffset     uint16
	AtchDBFileLength     uint16
	ChangePasswordOffset uint16
	ChangePasswordLength uint16
	SSPILongLength       uint32
}

// Login7 represents a parsed LOGIN7 packet.
type Login7 struct {
	Header Login7Header

	// Parsed string fields
	HostName       string
	UserName       string
	Password       string
	AppName        string
	ServerName     string
	CtlIntName     string // Client interface name (e.g., "go-mssqldb")
	Language       string
	Database       string
	AtchDBFile     string
	ChangePassword string

	// SSPI data for integrated authentication
	SSPI []byte

	// Feature extensions
	FeatureExt []byte
}

// ParseLogin7 parses a LOGIN7 packet from raw bytes.
func ParseLogin7(data []byte) (*Login7, error) {
	if len(data) < Login7HeaderSize {
		return nil, fmt.Errorf("login7 data too short: %d < %d", len(data), Login7HeaderSize)
	}

	l := &Login7{}

	// Parse fixed header
	l.Header.Length = binary.LittleEndian.Uint32(data[0:4])
	l.Header.TDSVersion = binary.LittleEndian.Uint32(data[4:8])
	l.Header.PacketSize = binary.LittleEndian.Uint32(data[8:12])
	l.Header.ClientProgVer = binary.LittleEndian.Uint32(data[12:16])
	l.Header.ClientPID = binary.LittleEndian.Uint32(data[16:20])
	l.Header.ConnectionID = binary.LittleEndian.Uint32(data[20:24])
	l.Header.OptionFlags1 = data[24]
	l.Header.OptionFlags2 = data[25]
	l.Header.TypeFlags = data[26]
	l.Header.OptionFlags3 = data[27]
	l.Header.ClientTimeZone = int32(binary.LittleEndian.Uint32(data[28:32]))
	l.Header.ClientLCID = binary.LittleEndian.Uint32(data[32:36])

	l.Header.HostNameOffset = binary.LittleEndian.Uint16(data[36:38])
	l.Header.HostNameLength = binary.LittleEndian.Uint16(data[38:40])
	l.Header.UserNameOffset = binary.LittleEndian.Uint16(data[40:42])
	l.Header.UserNameLength = binary.LittleEndian.Uint16(data[42:44])
	l.Header.PasswordOffset = binary.LittleEndian.Uint16(data[44:46])
	l.Header.PasswordLength = binary.LittleEndian.Uint16(data[46:48])
	l.Header.AppNameOffset = binary.LittleEndian.Uint16(data[48:50])
	l.Header.AppNameLength = binary.LittleEndian.Uint16(data[50:52])
	l.Header.ServerNameOffset = binary.LittleEndian.Uint16(data[52:54])
	l.Header.ServerNameLength = binary.LittleEndian.Uint16(data[54:56])
	l.Header.ExtensionOffset = binary.LittleEndian.Uint16(data[56:58])
	l.Header.ExtensionLength = binary.LittleEndian.Uint16(data[58:60])
	l.Header.CtlIntNameOffset = binary.LittleEndian.Uint16(data[60:62])
	l.Header.CtlIntNameLength = binary.LittleEndian.Uint16(data[62:64])
	l.Header.LanguageOffset = binary.LittleEndian.Uint16(data[64:66])
	l.Header.LanguageLength = binary.LittleEndian.Uint16(data[66:68])
	l.Header.DatabaseOffset = binary.LittleEndian.Uint16(data[68:70])
	l.Header.DatabaseLength = binary.LittleEndian.Uint16(data[70:72])
	copy(l.Header.ClientID[:], data[72:78])
	l.Header.SSPIOffset = binary.LittleEndian.Uint16(data[78:80])
	l.Header.SSPILength = binary.LittleEndian.Uint16(data[80:82])
	l.Header.AtchDBFileOffset = binary.LittleEndian.Uint16(data[82:84])
	l.Header.AtchDBFileLength = binary.LittleEndian.Uint16(data[84:86])
	l.Header.ChangePasswordOffset = binary.LittleEndian.Uint16(data[86:88])
	l.Header.ChangePasswordLength = binary.LittleEndian.Uint16(data[88:90])
	l.Header.SSPILongLength = binary.LittleEndian.Uint32(data[90:94])

	// Parse variable-length fields
	var err error

	l.HostName, err = readUCS2String(data, l.Header.HostNameOffset, l.Header.HostNameLength)
	if err != nil {
		return nil, fmt.Errorf("reading hostname: %w", err)
	}

	l.UserName, err = readUCS2String(data, l.Header.UserNameOffset, l.Header.UserNameLength)
	if err != nil {
		return nil, fmt.Errorf("reading username: %w", err)
	}

	// Password is mangled
	l.Password, err = readMangledPassword(data, l.Header.PasswordOffset, l.Header.PasswordLength)
	if err != nil {
		return nil, fmt.Errorf("reading password: %w", err)
	}

	l.AppName, err = readUCS2String(data, l.Header.AppNameOffset, l.Header.AppNameLength)
	if err != nil {
		return nil, fmt.Errorf("reading appname: %w", err)
	}

	l.ServerName, err = readUCS2String(data, l.Header.ServerNameOffset, l.Header.ServerNameLength)
	if err != nil {
		return nil, fmt.Errorf("reading servername: %w", err)
	}

	l.CtlIntName, err = readUCS2String(data, l.Header.CtlIntNameOffset, l.Header.CtlIntNameLength)
	if err != nil {
		return nil, fmt.Errorf("reading ctlintname: %w", err)
	}

	l.Language, err = readUCS2String(data, l.Header.LanguageOffset, l.Header.LanguageLength)
	if err != nil {
		return nil, fmt.Errorf("reading language: %w", err)
	}

	l.Database, err = readUCS2String(data, l.Header.DatabaseOffset, l.Header.DatabaseLength)
	if err != nil {
		return nil, fmt.Errorf("reading database: %w", err)
	}

	l.AtchDBFile, err = readUCS2String(data, l.Header.AtchDBFileOffset, l.Header.AtchDBFileLength)
	if err != nil {
		return nil, fmt.Errorf("reading atchdbfile: %w", err)
	}

	// Change password is also mangled
	if l.Header.ChangePasswordLength > 0 {
		l.ChangePassword, err = readMangledPassword(data, l.Header.ChangePasswordOffset, l.Header.ChangePasswordLength)
		if err != nil {
			return nil, fmt.Errorf("reading change password: %w", err)
		}
	}

	// SSPI data
	sspiLen := uint32(l.Header.SSPILength)
	if l.Header.SSPILongLength > 0 {
		sspiLen = l.Header.SSPILongLength
	}
	if sspiLen > 0 {
		start := int(l.Header.SSPIOffset)
		end := start + int(sspiLen)
		if end > len(data) {
			return nil, fmt.Errorf("SSPI data out of bounds")
		}
		l.SSPI = make([]byte, sspiLen)
		copy(l.SSPI, data[start:end])
	}

	// Feature extensions
	if l.Header.OptionFlags3&FlagExtension != 0 && l.Header.ExtensionLength > 0 {
		extStart := int(l.Header.ExtensionOffset)
		if extStart+4 > len(data) {
			return nil, fmt.Errorf("extension offset out of bounds")
		}
		// Extension offset points to a DWORD containing the actual offset
		featureExtOffset := binary.LittleEndian.Uint32(data[extStart : extStart+4])
		if int(featureExtOffset) < len(data) {
			// Read until terminator (0xFF)
			featureStart := int(featureExtOffset)
			for i := featureStart; i < len(data); i++ {
				if data[i] == 0xFF {
					l.FeatureExt = data[featureStart : i+1]
					break
				}
			}
		}
	}

	return l, nil
}

// IsIntegratedAuth returns true if integrated (SSPI) authentication is requested.
func (l *Login7) IsIntegratedAuth() bool {
	return l.Header.OptionFlags2&FlagIntSecurity != 0
}

// IsReadOnlyIntent returns true if read-only application intent is specified.
func (l *Login7) IsReadOnlyIntent() bool {
	return l.Header.TypeFlags&FlagReadOnlyIntent != 0
}

// readUCS2String reads a UCS-2 (UTF-16LE) encoded string from data.
// offset and length are in characters (not bytes).
func readUCS2String(data []byte, offset, length uint16) (string, error) {
	if length == 0 {
		return "", nil
	}

	byteOffset := int(offset)
	byteLen := int(length) * 2 // UCS-2 is 2 bytes per character

	if byteOffset+byteLen > len(data) {
		return "", fmt.Errorf("string data out of bounds: offset=%d, len=%d, datalen=%d",
			byteOffset, byteLen, len(data))
	}

	return ucs2ToString(data[byteOffset : byteOffset+byteLen]), nil
}

// readMangledPassword reads and demangles a password from the LOGIN7 packet.
// The password is XOR'd and bit-rotated for obfuscation (not security).
func readMangledPassword(data []byte, offset, length uint16) (string, error) {
	if length == 0 {
		return "", nil
	}

	byteOffset := int(offset)
	byteLen := int(length) * 2 // UCS-2 is 2 bytes per character

	if byteOffset+byteLen > len(data) {
		return "", fmt.Errorf("password data out of bounds")
	}

	// Copy and demangle
	mangled := make([]byte, byteLen)
	copy(mangled, data[byteOffset:byteOffset+byteLen])

	for i := range mangled {
		// Reverse the mangling: XOR with 0xA5, then reverse the nibble swap
		b := mangled[i] ^ 0xA5
		mangled[i] = (b >> 4) | (b << 4)
	}

	return ucs2ToString(mangled), nil
}

// ucs2ToString converts UCS-2 (UTF-16LE) bytes to a Go string.
func ucs2ToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}

	u16 := make([]uint16, len(b)/2)
	for i := 0; i < len(u16); i++ {
		u16[i] = binary.LittleEndian.Uint16(b[i*2:])
	}

	return string(utf16.Decode(u16))
}

// stringToUCS2 converts a Go string to UCS-2 (UTF-16LE) bytes.
func stringToUCS2(s string) []byte {
	u16 := utf16.Encode([]rune(s))
	b := make([]byte, len(u16)*2)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(b[i*2:], v)
	}
	return b
}
