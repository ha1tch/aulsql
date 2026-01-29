package tds

import (
	"encoding/binary"
	"fmt"
	"io"
)

// TDS protocol versions.
const (
	VerTDS70     uint32 = 0x70000000
	VerTDS71     uint32 = 0x71000000
	VerTDS71Rev1 uint32 = 0x71000001
	VerTDS72     uint32 = 0x72090002
	VerTDS73A    uint32 = 0x730A0003
	VerTDS73B    uint32 = 0x730B0003
	VerTDS74     uint32 = 0x74000004
	VerTDS80     uint32 = 0x08000000 // TDS 8.0 (strict encryption)
)

// VersionString returns a human-readable version string.
func VersionString(ver uint32) string {
	switch ver {
	case VerTDS70:
		return "7.0"
	case VerTDS71:
		return "7.1"
	case VerTDS71Rev1:
		return "7.1 Rev 1"
	case VerTDS72:
		return "7.2"
	case VerTDS73A:
		return "7.3A"
	case VerTDS73B:
		return "7.3B"
	case VerTDS74:
		return "7.4"
	case VerTDS80:
		return "8.0"
	default:
		return fmt.Sprintf("unknown (0x%08X)", ver)
	}
}

// Prelogin option tokens.
const (
	PreloginVersion    uint8 = 0x00
	PreloginEncryption uint8 = 0x01
	PreloginInstOpt    uint8 = 0x02
	PreloginThreadID   uint8 = 0x03
	PreloginMARS       uint8 = 0x04
	PreloginTraceID    uint8 = 0x05
	PreloginFedAuth    uint8 = 0x06
	PreloginNonceOpt   uint8 = 0x07
	PreloginTerminator uint8 = 0xFF
)

// Encryption options for prelogin.
const (
	EncryptOff    uint8 = 0x00 // Encryption available but off
	EncryptOn     uint8 = 0x01 // Encryption available and on
	EncryptNotSup uint8 = 0x02 // Encryption not supported
	EncryptReq    uint8 = 0x03 // Encryption required
	EncryptStrict uint8 = 0x04 // Strict encryption (TDS 8.0)
)

// PreloginOption represents a single prelogin option.
type PreloginOption struct {
	Token  uint8
	Offset uint16
	Length uint16
}

// Prelogin represents a TDS prelogin message.
type Prelogin struct {
	Version    []byte // 6 bytes: 4 version + 2 subbuild
	Encryption uint8
	Instance   string
	ThreadID   uint32
	MARS       uint8
	TraceID    []byte // 36 bytes if present
	FedAuth    uint8
	Nonce      []byte // 32 bytes if present
}

// ParsePrelogin parses a prelogin message from raw bytes.
func ParsePrelogin(data []byte) (*Prelogin, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty prelogin data")
	}

	p := &Prelogin{}

	// First pass: read option headers
	options := make(map[uint8]PreloginOption)
	offset := 0
	for {
		if offset >= len(data) {
			return nil, fmt.Errorf("prelogin data truncated reading options")
		}

		token := data[offset]
		if token == PreloginTerminator {
			break
		}

		if offset+5 > len(data) {
			return nil, fmt.Errorf("prelogin option header truncated")
		}

		opt := PreloginOption{
			Token:  token,
			Offset: binary.BigEndian.Uint16(data[offset+1 : offset+3]),
			Length: binary.BigEndian.Uint16(data[offset+3 : offset+5]),
		}
		options[token] = opt
		offset += 5
	}

	// Second pass: read option values
	for token, opt := range options {
		start := int(opt.Offset)
		end := start + int(opt.Length)
		if end > len(data) {
			return nil, fmt.Errorf("prelogin option %d data out of bounds", token)
		}
		value := data[start:end]

		switch token {
		case PreloginVersion:
			if len(value) >= 6 {
				p.Version = make([]byte, 6)
				copy(p.Version, value[:6])
			}
		case PreloginEncryption:
			if len(value) >= 1 {
				p.Encryption = value[0]
			}
		case PreloginInstOpt:
			// Instance is null-terminated
			for i, b := range value {
				if b == 0 {
					p.Instance = string(value[:i])
					break
				}
			}
			if p.Instance == "" && len(value) > 0 {
				p.Instance = string(value)
			}
		case PreloginThreadID:
			if len(value) >= 4 {
				p.ThreadID = binary.BigEndian.Uint32(value)
			}
		case PreloginMARS:
			if len(value) >= 1 {
				p.MARS = value[0]
			}
		case PreloginTraceID:
			if len(value) >= 36 {
				p.TraceID = make([]byte, 36)
				copy(p.TraceID, value[:36])
			}
		case PreloginFedAuth:
			if len(value) >= 1 {
				p.FedAuth = value[0]
			}
		case PreloginNonceOpt:
			if len(value) >= 32 {
				p.Nonce = make([]byte, 32)
				copy(p.Nonce, value[:32])
			}
		}
	}

	return p, nil
}

// ServerVersion represents the server version for prelogin response.
type ServerVersion struct {
	Major    uint8
	Minor    uint8
	Build    uint16
	SubBuild uint16
}

// DefaultServerVersion returns a default server version (SQL Server 2019-like).
func DefaultServerVersion() ServerVersion {
	return ServerVersion{
		Major:    15,
		Minor:    0,
		Build:    2000,
		SubBuild: 0,
	}
}

// Bytes returns the 6-byte version representation.
func (v ServerVersion) Bytes() []byte {
	buf := make([]byte, 6)
	buf[0] = v.Major
	buf[1] = v.Minor
	binary.BigEndian.PutUint16(buf[2:4], v.Build)
	binary.BigEndian.PutUint16(buf[4:6], v.SubBuild)
	return buf
}

// PreloginResponse represents the server's prelogin response.
type PreloginResponse struct {
	Version    ServerVersion
	Encryption uint8
	Instance   string
	ThreadID   uint32
	MARS       uint8
	FedAuth    uint8
}

// Encode encodes the prelogin response to bytes.
func (r *PreloginResponse) Encode() []byte {
	// Calculate sizes
	versionData := r.Version.Bytes()
	instanceData := []byte(r.Instance)
	if len(instanceData) == 0 {
		instanceData = []byte{0} // null terminator
	} else {
		instanceData = append(instanceData, 0) // null terminator
	}

	// Count options and calculate header size
	numOptions := 5 // VERSION, ENCRYPTION, INSTOPT, THREADID, MARS
	if r.FedAuth != 0 {
		numOptions++
	}
	headerSize := numOptions*5 + 1 // 5 bytes per option + terminator

	// Calculate data offsets
	offset := uint16(headerSize)
	offsets := make([]uint16, numOptions)
	lengths := make([]uint16, numOptions)

	idx := 0
	// VERSION
	offsets[idx] = offset
	lengths[idx] = uint16(len(versionData))
	offset += lengths[idx]
	idx++

	// ENCRYPTION
	offsets[idx] = offset
	lengths[idx] = 1
	offset += lengths[idx]
	idx++

	// INSTOPT
	offsets[idx] = offset
	lengths[idx] = uint16(len(instanceData))
	offset += lengths[idx]
	idx++

	// THREADID
	offsets[idx] = offset
	lengths[idx] = 4
	offset += lengths[idx]
	idx++

	// MARS
	offsets[idx] = offset
	lengths[idx] = 1
	offset += lengths[idx]
	idx++

	// FEDAUTH (optional)
	if r.FedAuth != 0 {
		offsets[idx] = offset
		lengths[idx] = 1
		offset += lengths[idx]
		idx++
	}

	// Build the response
	totalSize := int(offset)
	buf := make([]byte, totalSize)
	pos := 0

	// Write option headers
	tokens := []uint8{PreloginVersion, PreloginEncryption, PreloginInstOpt, PreloginThreadID, PreloginMARS}
	if r.FedAuth != 0 {
		tokens = append(tokens, PreloginFedAuth)
	}

	for i, token := range tokens {
		buf[pos] = token
		binary.BigEndian.PutUint16(buf[pos+1:pos+3], offsets[i])
		binary.BigEndian.PutUint16(buf[pos+3:pos+5], lengths[i])
		pos += 5
	}
	buf[pos] = PreloginTerminator
	pos++

	// Write option data
	// VERSION
	copy(buf[pos:], versionData)
	pos += len(versionData)

	// ENCRYPTION
	buf[pos] = r.Encryption
	pos++

	// INSTOPT
	copy(buf[pos:], instanceData)
	pos += len(instanceData)

	// THREADID
	binary.BigEndian.PutUint32(buf[pos:pos+4], r.ThreadID)
	pos += 4

	// MARS
	buf[pos] = r.MARS
	pos++

	// FEDAUTH
	if r.FedAuth != 0 {
		buf[pos] = r.FedAuth
		pos++
	}

	return buf
}

// WritePreloginResponse writes a prelogin response packet.
func WritePreloginResponse(w io.Writer, resp *PreloginResponse, spid uint16) error {
	data := resp.Encode()

	hdr := Header{
		Type:     PacketReply,
		Status:   StatusEOM,
		Length:   uint16(HeaderSize + len(data)),
		SPID:     spid,
		PacketID: 1,
		Window:   0,
	}

	if err := hdr.Write(w); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}
