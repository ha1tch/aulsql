package tds

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unicode/utf16"
)

// System stored procedure IDs used in RPC requests.
const (
	ProcIDCursor        uint16 = 1
	ProcIDCursorOpen    uint16 = 2
	ProcIDCursorPrepare uint16 = 3
	ProcIDCursorExecute uint16 = 4
	ProcIDCursorPrepExec uint16 = 5
	ProcIDCursorUnprepare uint16 = 6
	ProcIDCursorFetch   uint16 = 7
	ProcIDCursorOption  uint16 = 8
	ProcIDCursorClose   uint16 = 9
	ProcIDExecuteSQL    uint16 = 10
	ProcIDPrepare       uint16 = 11
	ProcIDExecute       uint16 = 12
	ProcIDPrepExec      uint16 = 13
	ProcIDPrepExecRPC   uint16 = 14
	ProcIDUnprepare     uint16 = 15
)

// ProcIDName returns the name for a system stored procedure ID.
func ProcIDName(id uint16) string {
	switch id {
	case ProcIDCursor:
		return "sp_cursor"
	case ProcIDCursorOpen:
		return "sp_cursoropen"
	case ProcIDCursorPrepare:
		return "sp_cursorprepare"
	case ProcIDCursorExecute:
		return "sp_cursorexecute"
	case ProcIDCursorPrepExec:
		return "sp_cursorprepexec"
	case ProcIDCursorUnprepare:
		return "sp_cursorunprepare"
	case ProcIDCursorFetch:
		return "sp_cursorfetch"
	case ProcIDCursorOption:
		return "sp_cursoroption"
	case ProcIDCursorClose:
		return "sp_cursorclose"
	case ProcIDExecuteSQL:
		return "sp_executesql"
	case ProcIDPrepare:
		return "sp_prepare"
	case ProcIDExecute:
		return "sp_execute"
	case ProcIDPrepExec:
		return "sp_prepexec"
	case ProcIDPrepExecRPC:
		return "sp_prepexecrpc"
	case ProcIDUnprepare:
		return "sp_unprepare"
	default:
		return fmt.Sprintf("sp_unknown_%d", id)
	}
}

// RPC option flags.
const (
	RPCOptionWithRecomp   uint16 = 0x0001 // WITH RECOMPILE
	RPCOptionNoMetaData   uint16 = 0x0002 // No metadata in result
	RPCOptionReuseCursor  uint16 = 0x0004 // Reuse cursor
)

// Parameter status flags.
const (
	ParamByRefValue uint8 = 0x01 // Output parameter
	ParamDefaultValue uint8 = 0x02 // Use default value
	ParamEncrypted  uint8 = 0x08 // Always Encrypted
)

// RPCRequest represents a parsed RPC request.
type RPCRequest struct {
	ProcID     uint16       // System procedure ID (0 if named)
	ProcName   string       // Procedure name
	Options    uint16       // RPC option flags
	Parameters []RPCParam   // Parameters
}

// RPCParam represents an RPC parameter.
type RPCParam struct {
	Name      string      // Parameter name (may be empty for positional)
	Status    uint8       // Parameter status flags
	Type      TypeInfo    // Type information
	Value     interface{} // Decoded value
	IsNull    bool        // True if value is NULL
	IsOutput  bool        // True if output parameter
}

// TypeInfo describes the type of a parameter or column.
type TypeInfo struct {
	TypeID    SQLType
	Size      uint32  // Max size for variable types
	Precision uint8   // For decimal/numeric
	Scale     uint8   // For decimal/numeric and time types
	Collation []byte  // 5 bytes for string types
}

// ParseRPCRequest parses an RPC_REQUEST packet payload.
// The data should not include the TDS packet header.
func ParseRPCRequest(data []byte, tdsVersion uint32) (*RPCRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("RPC request too short: %d bytes", len(data))
	}

	r := &rpcReader{data: data, pos: 0}
	req := &RPCRequest{}

	// Skip ALL_HEADERS if present (TDS 7.2+)
	if tdsVersion >= VerTDS72 {
		totalLen, err := r.readUint32()
		if err != nil {
			return nil, fmt.Errorf("reading headers length: %w", err)
		}
		if totalLen > 4 {
			// Skip remaining header bytes
			if err := r.skip(int(totalLen) - 4); err != nil {
				return nil, fmt.Errorf("skipping headers: %w", err)
			}
		}
	}

	// Read procedure name length
	nameLen, err := r.readUint16()
	if err != nil {
		return nil, fmt.Errorf("reading name length: %w", err)
	}

	if nameLen == 0xFFFF {
		// Procedure ID
		procID, err := r.readUint16()
		if err != nil {
			return nil, fmt.Errorf("reading proc ID: %w", err)
		}
		req.ProcID = procID
		req.ProcName = ProcIDName(procID)
	} else {
		// Procedure name (UTF-16LE)
		nameBytes, err := r.readBytes(int(nameLen) * 2)
		if err != nil {
			return nil, fmt.Errorf("reading proc name: %w", err)
		}
		req.ProcName = decodeUTF16(nameBytes)
	}

	// Read option flags
	options, err := r.readUint16()
	if err != nil {
		return nil, fmt.Errorf("reading options: %w", err)
	}
	req.Options = options

	// Parse parameters until end of data
	for r.pos < len(r.data) {
		param, err := r.readParameter()
		if err != nil {
			return nil, fmt.Errorf("reading parameter %d: %w", len(req.Parameters), err)
		}
		req.Parameters = append(req.Parameters, param)
	}

	return req, nil
}

// rpcReader helps parse RPC request data.
type rpcReader struct {
	data []byte
	pos  int
}

func (r *rpcReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("unexpected end of data at pos %d", r.pos)
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *rpcReader) readBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, fmt.Errorf("unexpected end of data: need %d bytes at pos %d, have %d", n, r.pos, len(r.data)-r.pos)
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *rpcReader) readUint16() (uint16, error) {
	b, err := r.readBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *rpcReader) readUint32() (uint32, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *rpcReader) readInt32() (int32, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func (r *rpcReader) readUint64() (uint64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *rpcReader) readInt64() (int64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (r *rpcReader) readFloat32() (float32, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
}

func (r *rpcReader) readFloat64() (float64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}

func (r *rpcReader) skip(n int) error {
	if r.pos+n > len(r.data) {
		return fmt.Errorf("cannot skip %d bytes at pos %d", n, r.pos)
	}
	r.pos += n
	return nil
}

// readParameter reads a single RPC parameter.
func (r *rpcReader) readParameter() (RPCParam, error) {
	param := RPCParam{}

	// Parameter name (B_VARCHAR: 1-byte length, UTF-16LE string)
	nameLen, err := r.readByte()
	if err != nil {
		return param, fmt.Errorf("reading param name length: %w", err)
	}
	if nameLen > 0 {
		nameBytes, err := r.readBytes(int(nameLen) * 2)
		if err != nil {
			return param, fmt.Errorf("reading param name: %w", err)
		}
		param.Name = decodeUTF16(nameBytes)
		// Remove leading @ if present
		if len(param.Name) > 0 && param.Name[0] == '@' {
			param.Name = param.Name[1:]
		}
	}

	// Status flags
	status, err := r.readByte()
	if err != nil {
		return param, fmt.Errorf("reading param status: %w", err)
	}
	param.Status = status
	param.IsOutput = (status & ParamByRefValue) != 0

	// TYPE_INFO
	typeInfo, err := r.readTypeInfo()
	if err != nil {
		return param, fmt.Errorf("reading type info: %w", err)
	}
	param.Type = typeInfo

	// Read value based on type
	value, isNull, err := r.readValue(typeInfo)
	if err != nil {
		return param, fmt.Errorf("reading value: %w", err)
	}
	param.Value = value
	param.IsNull = isNull

	return param, nil
}

// readTypeInfo reads TYPE_INFO structure.
func (r *rpcReader) readTypeInfo() (TypeInfo, error) {
	ti := TypeInfo{}

	typeID, err := r.readByte()
	if err != nil {
		return ti, fmt.Errorf("reading type ID: %w", err)
	}
	ti.TypeID = SQLType(typeID)

	switch ti.TypeID {
	// Fixed-length types - no additional info
	case TypeNull:
		ti.Size = 0
	case TypeInt1:
		ti.Size = 1
	case TypeBit:
		ti.Size = 1
	case TypeInt2:
		ti.Size = 2
	case TypeInt4:
		ti.Size = 4
	case TypeInt8:
		ti.Size = 8
	case TypeFloat4:
		ti.Size = 4
	case TypeFloat8:
		ti.Size = 8
	case TypeMoney4:
		ti.Size = 4
	case TypeMoney:
		ti.Size = 8
	case TypeDateTime4:
		ti.Size = 4
	case TypeDateTime:
		ti.Size = 8

	// Variable integer types (1-byte max size)
	case TypeIntN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	case TypeBitN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	case TypeFloatN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	case TypeMoneyN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	case TypeDateTimeN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	// Date/time types
	case TypeDateN:
		// No additional metadata
		ti.Size = 3

	case TypeTimeN, TypeDateTime2N, TypeDateTimeOffsetN:
		scale, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Scale = scale

	// Decimal/Numeric
	case TypeDecimalN, TypeNumericN:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)
		prec, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Precision = prec
		scale, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Scale = scale

	// GUID
	case TypeGUID:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size) // 16 or 0

	// Legacy short strings (1-byte max length)
	case TypeChar, TypeVarChar:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)
		// Collation
		coll, err := r.readBytes(5)
		if err != nil {
			return ti, err
		}
		ti.Collation = coll

	case TypeBinary, TypeVarBinary:
		size, err := r.readByte()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	// Big strings (2-byte max length)
	case TypeBigVarChar, TypeBigChar:
		size, err := r.readUint16()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)
		// Collation
		coll, err := r.readBytes(5)
		if err != nil {
			return ti, err
		}
		ti.Collation = coll

	case TypeBigVarBin, TypeBigBinary:
		size, err := r.readUint16()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)

	// Unicode strings (2-byte max length in bytes)
	case TypeNVarChar, TypeNChar:
		size, err := r.readUint16()
		if err != nil {
			return ti, err
		}
		ti.Size = uint32(size)
		// Collation
		coll, err := r.readBytes(5)
		if err != nil {
			return ti, err
		}
		ti.Collation = coll

	// Text/Image (4-byte max length)
	case TypeText, TypeNText:
		size, err := r.readUint32()
		if err != nil {
			return ti, err
		}
		ti.Size = size
		// Collation
		coll, err := r.readBytes(5)
		if err != nil {
			return ti, err
		}
		ti.Collation = coll
		// Table name parts
		numParts, err := r.readByte()
		if err != nil {
			return ti, err
		}
		for i := uint8(0); i < numParts; i++ {
			partLen, err := r.readUint16()
			if err != nil {
				return ti, err
			}
			if err := r.skip(int(partLen) * 2); err != nil {
				return ti, err
			}
		}

	case TypeImage:
		size, err := r.readUint32()
		if err != nil {
			return ti, err
		}
		ti.Size = size
		// Table name parts
		numParts, err := r.readByte()
		if err != nil {
			return ti, err
		}
		for i := uint8(0); i < numParts; i++ {
			partLen, err := r.readUint16()
			if err != nil {
				return ti, err
			}
			if err := r.skip(int(partLen) * 2); err != nil {
				return ti, err
			}
		}

	case TypeXML:
		// Schema info
		schemaPresent, err := r.readByte()
		if err != nil {
			return ti, err
		}
		if schemaPresent != 0 {
			// Skip schema info
			// dbname length
			dbLen, err := r.readByte()
			if err != nil {
				return ti, err
			}
			if err := r.skip(int(dbLen) * 2); err != nil {
				return ti, err
			}
			// owning schema length
			schemaLen, err := r.readByte()
			if err != nil {
				return ti, err
			}
			if err := r.skip(int(schemaLen) * 2); err != nil {
				return ti, err
			}
			// xml schema collection length
			collLen, err := r.readUint16()
			if err != nil {
				return ti, err
			}
			if err := r.skip(int(collLen) * 2); err != nil {
				return ti, err
			}
		}

	default:
		return ti, fmt.Errorf("unsupported type: 0x%02X", typeID)
	}

	return ti, nil
}

// readValue reads a parameter value based on its type info.
func (r *rpcReader) readValue(ti TypeInfo) (interface{}, bool, error) {
	switch ti.TypeID {
	// Fixed-length types
	case TypeNull:
		return nil, true, nil

	case TypeInt1:
		v, err := r.readByte()
		return int64(v), false, err

	case TypeBit:
		v, err := r.readByte()
		return v != 0, false, err

	case TypeInt2:
		v, err := r.readUint16()
		return int64(int16(v)), false, err

	case TypeInt4:
		v, err := r.readInt32()
		return int64(v), false, err

	case TypeInt8:
		v, err := r.readInt64()
		return v, false, err

	case TypeFloat4:
		v, err := r.readFloat32()
		return float64(v), false, err

	case TypeFloat8:
		v, err := r.readFloat64()
		return v, false, err

	case TypeDateTime4:
		// 2 bytes days since 1900-01-01, 2 bytes minutes since midnight
		days, err := r.readUint16()
		if err != nil {
			return nil, false, err
		}
		mins, err := r.readUint16()
		if err != nil {
			return nil, false, err
		}
		return decodeSmallDateTime(days, mins), false, nil

	case TypeDateTime:
		// 4 bytes days since 1900-01-01, 4 bytes 1/300ths of second
		days, err := r.readInt32()
		if err != nil {
			return nil, false, err
		}
		ticks, err := r.readUint32()
		if err != nil {
			return nil, false, err
		}
		return decodeDateTime(days, ticks), false, nil

	// Variable-length nullable types
	case TypeIntN:
		return r.readIntN(ti.Size)

	case TypeBitN:
		return r.readBitN()

	case TypeFloatN:
		return r.readFloatN(ti.Size)

	case TypeDateTimeN:
		return r.readDateTimeN(ti.Size)

	case TypeDateN:
		return r.readDateN()

	case TypeTimeN:
		return r.readTimeN(ti.Scale)

	case TypeDateTime2N:
		return r.readDateTime2N(ti.Scale)

	case TypeDateTimeOffsetN:
		return r.readDateTimeOffsetN(ti.Scale)

	case TypeDecimalN, TypeNumericN:
		return r.readDecimalN(ti.Precision, ti.Scale)

	case TypeMoneyN:
		return r.readMoneyN(ti.Size)

	case TypeGUID:
		return r.readGUID()

	// String types
	case TypeChar, TypeVarChar:
		return r.readShortVarChar()

	case TypeBigVarChar, TypeBigChar:
		return r.readLongVarChar()

	case TypeNVarChar, TypeNChar:
		return r.readNVarChar()

	case TypeBinary, TypeVarBinary:
		return r.readShortVarBinary()

	case TypeBigVarBin, TypeBigBinary:
		return r.readLongVarBinary()

	case TypeText, TypeNText, TypeImage:
		return r.readTextPointer(ti.TypeID)

	case TypeXML:
		return r.readXML()

	default:
		return nil, false, fmt.Errorf("cannot read value for type 0x%02X", ti.TypeID)
	}
}

// Nullable integer reading
func (r *rpcReader) readIntN(maxSize uint32) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	switch size {
	case 1:
		v, err := r.readByte()
		return int64(v), false, err
	case 2:
		v, err := r.readUint16()
		return int64(int16(v)), false, err
	case 4:
		v, err := r.readInt32()
		return int64(v), false, err
	case 8:
		v, err := r.readInt64()
		return v, false, err
	default:
		return nil, false, fmt.Errorf("invalid IntN size: %d", size)
	}
}

func (r *rpcReader) readBitN() (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	v, err := r.readByte()
	return v != 0, false, err
}

func (r *rpcReader) readFloatN(maxSize uint32) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	switch size {
	case 4:
		v, err := r.readFloat32()
		return float64(v), false, err
	case 8:
		v, err := r.readFloat64()
		return v, false, err
	default:
		return nil, false, fmt.Errorf("invalid FloatN size: %d", size)
	}
}

func (r *rpcReader) readDateTimeN(maxSize uint32) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	switch size {
	case 4:
		days, err := r.readUint16()
		if err != nil {
			return nil, false, err
		}
		mins, err := r.readUint16()
		if err != nil {
			return nil, false, err
		}
		return decodeSmallDateTime(days, mins), false, nil
	case 8:
		days, err := r.readInt32()
		if err != nil {
			return nil, false, err
		}
		ticks, err := r.readUint32()
		if err != nil {
			return nil, false, err
		}
		return decodeDateTime(days, ticks), false, nil
	default:
		return nil, false, fmt.Errorf("invalid DateTimeN size: %d", size)
	}
}

func (r *rpcReader) readDateN() (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	// 3 bytes for days since 0001-01-01
	b, err := r.readBytes(3)
	if err != nil {
		return nil, false, err
	}
	days := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
	return decodeDate(days), false, nil
}

func (r *rpcReader) readTimeN(scale uint8) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return decodeTime(b, scale), false, nil
}

func (r *rpcReader) readDateTime2N(scale uint8) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return decodeDateTime2(b, scale), false, nil
}

func (r *rpcReader) readDateTimeOffsetN(scale uint8) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return decodeDateTimeOffset(b, scale), false, nil
}

func (r *rpcReader) readDecimalN(precision, scale uint8) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return decodeDecimal(b, precision, scale), false, nil
}

func (r *rpcReader) readMoneyN(maxSize uint32) (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	switch size {
	case 4:
		v, err := r.readInt32()
		if err != nil {
			return nil, false, err
		}
		return float64(v) / 10000.0, false, nil
	case 8:
		hi, err := r.readInt32()
		if err != nil {
			return nil, false, err
		}
		lo, err := r.readUint32()
		if err != nil {
			return nil, false, err
		}
		v := int64(hi)<<32 | int64(lo)
		return float64(v) / 10000.0, false, nil
	default:
		return nil, false, fmt.Errorf("invalid MoneyN size: %d", size)
	}
}

func (r *rpcReader) readGUID() (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 {
		return nil, true, nil
	}
	b, err := r.readBytes(16)
	if err != nil {
		return nil, false, err
	}
	return formatGUID(b), false, nil
}

// String reading
func (r *rpcReader) readShortVarChar() (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 || size == 0xFF {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return string(b), false, nil
}

func (r *rpcReader) readLongVarChar() (interface{}, bool, error) {
	size, err := r.readUint16()
	if err != nil {
		return nil, false, err
	}
	if size == 0xFFFF {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return string(b), false, nil
}

func (r *rpcReader) readNVarChar() (interface{}, bool, error) {
	size, err := r.readUint16()
	if err != nil {
		return nil, false, err
	}
	if size == 0xFFFF {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	return decodeUTF16(b), false, nil
}

func (r *rpcReader) readShortVarBinary() (interface{}, bool, error) {
	size, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if size == 0 || size == 0xFF {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	result := make([]byte, len(b))
	copy(result, b)
	return result, false, nil
}

func (r *rpcReader) readLongVarBinary() (interface{}, bool, error) {
	size, err := r.readUint16()
	if err != nil {
		return nil, false, err
	}
	if size == 0xFFFF {
		return nil, true, nil
	}
	b, err := r.readBytes(int(size))
	if err != nil {
		return nil, false, err
	}
	result := make([]byte, len(b))
	copy(result, b)
	return result, false, nil
}

func (r *rpcReader) readTextPointer(typeID SQLType) (interface{}, bool, error) {
	// Text pointer length
	tpLen, err := r.readByte()
	if err != nil {
		return nil, false, err
	}
	if tpLen == 0 {
		return nil, true, nil
	}
	// Skip text pointer and timestamp
	if err := r.skip(int(tpLen) + 8); err != nil {
		return nil, false, err
	}
	// Actual data length
	dataLen, err := r.readUint32()
	if err != nil {
		return nil, false, err
	}
	b, err := r.readBytes(int(dataLen))
	if err != nil {
		return nil, false, err
	}
	if typeID == TypeNText {
		return decodeUTF16(b), false, nil
	}
	if typeID == TypeImage {
		result := make([]byte, len(b))
		copy(result, b)
		return result, false, nil
	}
	return string(b), false, nil
}

func (r *rpcReader) readXML() (interface{}, bool, error) {
	// XML uses PLP (Partially Length-prefixed) format
	totalLen, err := r.readUint64()
	if err != nil {
		return nil, false, err
	}
	if totalLen == 0xFFFFFFFFFFFFFFFF {
		return nil, true, nil
	}

	var result []byte
	if totalLen == 0xFFFFFFFFFFFFFFFE {
		// Unknown length, read chunks
		for {
			chunkLen, err := r.readUint32()
			if err != nil {
				return nil, false, err
			}
			if chunkLen == 0 {
				break
			}
			chunk, err := r.readBytes(int(chunkLen))
			if err != nil {
				return nil, false, err
			}
			result = append(result, chunk...)
		}
	} else {
		// Known length, but still chunked
		for uint64(len(result)) < totalLen {
			chunkLen, err := r.readUint32()
			if err != nil {
				return nil, false, err
			}
			if chunkLen == 0 {
				break
			}
			chunk, err := r.readBytes(int(chunkLen))
			if err != nil {
				return nil, false, err
			}
			result = append(result, chunk...)
		}
	}

	return decodeUTF16(result), false, nil
}

// Helper functions for decoding values

func decodeUTF16(b []byte) string {
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

var baseDate1900 = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
var baseDate0001 = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

func decodeSmallDateTime(days uint16, mins uint16) time.Time {
	return baseDate1900.AddDate(0, 0, int(days)).Add(time.Duration(mins) * time.Minute)
}

func decodeDateTime(days int32, ticks uint32) time.Time {
	// ticks are 1/300th of a second
	ns := int64(ticks) * 1000000000 / 300
	return baseDate1900.AddDate(0, 0, int(days)).Add(time.Duration(ns))
}

func decodeDate(days uint32) time.Time {
	return baseDate0001.AddDate(0, 0, int(days))
}

func decodeTime(b []byte, scale uint8) time.Time {
	// Time is stored as scaled integer
	var ticks uint64
	for i := 0; i < len(b); i++ {
		ticks |= uint64(b[i]) << (uint(i) * 8)
	}
	// Scale determines precision: 10^scale ticks per second
	divisor := uint64(1)
	for i := uint8(0); i < 7-scale; i++ {
		divisor *= 10
	}
	ns := ticks * 100 * divisor
	return time.Date(1, 1, 1, 0, 0, 0, int(ns), time.UTC)
}

func decodeDateTime2(b []byte, scale uint8) time.Time {
	// Last 3 bytes are date
	timeLen := len(b) - 3
	timeBytes := b[:timeLen]
	dateBytes := b[timeLen:]

	days := uint32(dateBytes[0]) | uint32(dateBytes[1])<<8 | uint32(dateBytes[2])<<16
	date := baseDate0001.AddDate(0, 0, int(days))

	var ticks uint64
	for i := 0; i < len(timeBytes); i++ {
		ticks |= uint64(timeBytes[i]) << (uint(i) * 8)
	}
	divisor := uint64(1)
	for i := uint8(0); i < 7-scale; i++ {
		divisor *= 10
	}
	ns := ticks * 100 * divisor

	return date.Add(time.Duration(ns))
}

func decodeDateTimeOffset(b []byte, scale uint8) time.Time {
	// Last 2 bytes are timezone offset, before that 3 bytes date
	offsetBytes := b[len(b)-2:]
	dateTimeBytes := b[:len(b)-2]

	offsetMins := int16(binary.LittleEndian.Uint16(offsetBytes))
	loc := time.FixedZone("", int(offsetMins)*60)

	// Decode datetime2 portion
	timeLen := len(dateTimeBytes) - 3
	timeBytes := dateTimeBytes[:timeLen]
	dateBytes := dateTimeBytes[timeLen:]

	days := uint32(dateBytes[0]) | uint32(dateBytes[1])<<8 | uint32(dateBytes[2])<<16
	date := time.Date(1, 1, 1, 0, 0, 0, 0, loc).AddDate(0, 0, int(days))

	var ticks uint64
	for i := 0; i < len(timeBytes); i++ {
		ticks |= uint64(timeBytes[i]) << (uint(i) * 8)
	}
	divisor := uint64(1)
	for i := uint8(0); i < 7-scale; i++ {
		divisor *= 10
	}
	ns := ticks * 100 * divisor

	return date.Add(time.Duration(ns))
}

func decodeDecimal(b []byte, precision, scale uint8) string {
	if len(b) == 0 {
		return "0"
	}
	sign := b[0]
	data := b[1:]

	// Convert to big integer
	var val uint64
	for i := 0; i < len(data) && i < 8; i++ {
		val |= uint64(data[i]) << (uint(i) * 8)
	}

	// Format with scale
	str := fmt.Sprintf("%d", val)
	if int(scale) >= len(str) {
		str = "0." + fmt.Sprintf("%0*d", scale, val)
	} else if scale > 0 {
		pos := len(str) - int(scale)
		str = str[:pos] + "." + str[pos:]
	}

	if sign == 0 {
		str = "-" + str
	}
	return str
}

func formatGUID(b []byte) string {
	// SQL Server GUID is stored in a specific byte order
	return fmt.Sprintf("%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15])
}
