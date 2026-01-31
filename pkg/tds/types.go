package tds

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// SQL Server data type constants.
type SQLType uint8

const (
	TypeNull          SQLType = 0x1F // 31
	TypeInt1          SQLType = 0x30 // 48  - tinyint
	TypeBit           SQLType = 0x32 // 50
	TypeInt2          SQLType = 0x34 // 52  - smallint
	TypeInt4          SQLType = 0x38 // 56  - int
	TypeDateTime4     SQLType = 0x3A // 58  - smalldatetime
	TypeFloat4        SQLType = 0x3B // 59  - real
	TypeMoney         SQLType = 0x3C // 60
	TypeDateTime      SQLType = 0x3D // 61
	TypeFloat8        SQLType = 0x3E // 62  - float
	TypeMoney4        SQLType = 0x7A // 122 - smallmoney
	TypeInt8          SQLType = 0x7F // 127 - bigint

	// Variable length types
	TypeGUID          SQLType = 0x24 // 36
	TypeIntN          SQLType = 0x26 // 38
	TypeDecimal       SQLType = 0x37 // 55  - (legacy)
	TypeNumeric       SQLType = 0x3F // 63  - (legacy)
	TypeBitN          SQLType = 0x68 // 104
	TypeDecimalN      SQLType = 0x6A // 106
	TypeNumericN      SQLType = 0x6C // 108
	TypeFloatN        SQLType = 0x6D // 109
	TypeMoneyN        SQLType = 0x6E // 110
	TypeDateTimeN     SQLType = 0x6F // 111
	TypeDateN         SQLType = 0x28 // 40
	TypeTimeN         SQLType = 0x29 // 41
	TypeDateTime2N    SQLType = 0x2A // 42
	TypeDateTimeOffsetN SQLType = 0x2B // 43

	// String types
	TypeChar          SQLType = 0x2F // 47
	TypeVarChar       SQLType = 0x27 // 39
	TypeBinary        SQLType = 0x2D // 45
	TypeVarBinary     SQLType = 0x25 // 37

	// Large types (use 2-byte length)
	TypeBigVarBin     SQLType = 0xA5 // 165
	TypeBigVarChar    SQLType = 0xA7 // 167
	TypeBigBinary     SQLType = 0xAD // 173
	TypeBigChar       SQLType = 0xAF // 175
	TypeNVarChar      SQLType = 0xE7 // 231
	TypeNChar         SQLType = 0xEF // 239
	TypeXML           SQLType = 0xF1 // 241
	TypeUDT           SQLType = 0xF0 // 240

	// Max types (varchar(max), etc.)
	TypeText          SQLType = 0x23 // 35
	TypeImage         SQLType = 0x22 // 34
	TypeNText         SQLType = 0x63 // 99
	TypeSSVariant     SQLType = 0x62 // 98
)

func (t SQLType) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeInt1:
		return "TINYINT"
	case TypeBit:
		return "BIT"
	case TypeInt2:
		return "SMALLINT"
	case TypeInt4:
		return "INT"
	case TypeInt8:
		return "BIGINT"
	case TypeFloat4:
		return "REAL"
	case TypeFloat8:
		return "FLOAT"
	case TypeDateTime:
		return "DATETIME"
	case TypeDateTime4:
		return "SMALLDATETIME"
	case TypeMoney:
		return "MONEY"
	case TypeMoney4:
		return "SMALLMONEY"
	case TypeGUID:
		return "UNIQUEIDENTIFIER"
	case TypeIntN:
		return "INTN"
	case TypeBitN:
		return "BITN"
	case TypeFloatN:
		return "FLOATN"
	case TypeMoneyN:
		return "MONEYN"
	case TypeDateTimeN:
		return "DATETIMEN"
	case TypeDateN:
		return "DATE"
	case TypeTimeN:
		return "TIME"
	case TypeDateTime2N:
		return "DATETIME2"
	case TypeDateTimeOffsetN:
		return "DATETIMEOFFSET"
	case TypeDecimalN, TypeNumericN:
		return "DECIMAL"
	case TypeChar:
		return "CHAR"
	case TypeVarChar:
		return "VARCHAR"
	case TypeBinary:
		return "BINARY"
	case TypeVarBinary:
		return "VARBINARY"
	case TypeBigVarBin:
		return "VARBINARY"
	case TypeBigVarChar:
		return "VARCHAR"
	case TypeBigBinary:
		return "BINARY"
	case TypeBigChar:
		return "CHAR"
	case TypeNVarChar:
		return "NVARCHAR"
	case TypeNChar:
		return "NCHAR"
	case TypeText:
		return "TEXT"
	case TypeNText:
		return "NTEXT"
	case TypeImage:
		return "IMAGE"
	case TypeXML:
		return "XML"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(t))
	}
}

// Column represents a column in a result set.
type Column struct {
	Name       string
	Type       SQLType
	Length     uint32 // Max length for variable types
	Precision  uint8  // For decimal/numeric
	Scale      uint8  // For decimal/numeric
	Collation  []byte // 5 bytes for collation info
	Nullable   bool
	UserType   uint32
	Flags      uint16
}

// ColumnFlags for COLMETADATA.
const (
	ColFlagNullable     uint16 = 0x0001
	ColFlagCaseSen      uint16 = 0x0002
	ColFlagUpdateable   uint16 = 0x0008
	ColFlagIdentity     uint16 = 0x0010
	ColFlagComputed     uint16 = 0x0020
	ColFlagFixedLenCLR  uint16 = 0x0100
	ColFlagSparseColumn uint16 = 0x0400
	ColFlagEncrypted    uint16 = 0x0800
	ColFlagHidden       uint16 = 0x2000
	ColFlagKey          uint16 = 0x4000
	ColFlagNullableUnknown uint16 = 0x8000
)

// ResultSet represents a complete result set with columns and rows.
type ResultSet struct {
	Columns []Column
	Rows    [][]interface{}
}

// ResultSetWriter helps write result sets to the token stream.
type ResultSetWriter struct {
	tw        *TokenWriter
	columns   []Column
	useNBCRow bool // Enable NBCRow encoding for rows with many NULLs
}

// NewResultSetWriter creates a result set writer with the given columns.
func NewResultSetWriter(tw *TokenWriter, columns []Column) *ResultSetWriter {
	return &ResultSetWriter{
		tw:      tw,
		columns: columns,
	}
}

// WriteColMetadata writes the COLMETADATA token for the result set.
func (r *ResultSetWriter) WriteColMetadata() {
	buf := &r.tw.buf

	// Token type
	buf.WriteByte(byte(TokenColMetadata))

	// Column count
	binary.Write(buf, binary.LittleEndian, uint16(len(r.columns)))

	// Write each column
	for _, col := range r.columns {
		// UserType (4 bytes for TDS 7.2+)
		binary.Write(buf, binary.LittleEndian, col.UserType)

		// Flags
		flags := col.Flags
		if col.Nullable {
			flags |= ColFlagNullable
		}
		binary.Write(buf, binary.LittleEndian, flags)

		// TYPE_INFO varies by type
		r.writeTypeInfo(col)

		// Column name (B_VARCHAR)
		nameBytes := stringToUCS2(col.Name)
		buf.WriteByte(byte(len(col.Name)))
		buf.Write(nameBytes)
	}
}

// writeTypeInfo writes the TYPE_INFO portion for a column.
func (r *ResultSetWriter) writeTypeInfo(col Column) {
	buf := &r.tw.buf

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
		// Legacy types with 1-byte length
		buf.WriteByte(byte(col.Length))
		if col.Type == TypeChar || col.Type == TypeVarChar {
			// Collation
			if len(col.Collation) >= 5 {
				buf.Write(col.Collation[:5])
			} else {
				buf.Write([]byte{0, 0, 0, 0, 0}) // Default collation
			}
		}

	case TypeBigVarChar, TypeBigChar, TypeBigVarBin, TypeBigBinary:
		// 2-byte length prefix
		binary.Write(buf, binary.LittleEndian, uint16(col.Length))
		if col.Type == TypeBigVarChar || col.Type == TypeBigChar {
			// Collation
			if len(col.Collation) >= 5 {
				buf.Write(col.Collation[:5])
			} else {
				buf.Write([]byte{0, 0, 0, 0, 0})
			}
		}

	case TypeNVarChar, TypeNChar:
		// 2-byte length prefix (in bytes, not characters)
		binary.Write(buf, binary.LittleEndian, uint16(col.Length))
		// Collation
		if len(col.Collation) >= 5 {
			buf.Write(col.Collation[:5])
		} else {
			buf.Write([]byte{0, 0, 0, 0, 0})
		}

	case TypeText, TypeNText, TypeImage:
		// LOB types
		binary.Write(buf, binary.LittleEndian, uint32(col.Length))
		if col.Type != TypeImage {
			// Collation for text types
			if len(col.Collation) >= 5 {
				buf.Write(col.Collation[:5])
			} else {
				buf.Write([]byte{0, 0, 0, 0, 0})
			}
		}
		// Table name (empty for our purposes)
		buf.WriteByte(0) // Number of parts
	}
}

// WriteRow writes a ROW token with the given values.
func (r *ResultSetWriter) WriteRow(values []interface{}) error {
	if len(values) != len(r.columns) {
		return fmt.Errorf("value count %d doesn't match column count %d", len(values), len(r.columns))
	}

	buf := &r.tw.buf
	buf.WriteByte(byte(TokenRow))

	for i, val := range values {
		if err := r.writeValue(val, r.columns[i]); err != nil {
			return fmt.Errorf("writing column %d (%s): %w", i, r.columns[i].Name, err)
		}
	}

	return nil
}

// writeValue writes a single value according to its column type.
func (r *ResultSetWriter) writeValue(val interface{}, col Column) error {
	buf := &r.tw.buf

	// Handle NULL
	if val == nil {
		return r.writeNull(col)
	}

	switch col.Type {
	case TypeInt1:
		v, ok := toInt64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", val)
		}
		buf.WriteByte(byte(v))

	case TypeInt2:
		v, ok := toInt64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", val)
		}
		binary.Write(buf, binary.LittleEndian, int16(v))

	case TypeInt4:
		v, ok := toInt64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", val)
		}
		binary.Write(buf, binary.LittleEndian, int32(v))

	case TypeInt8:
		v, ok := toInt64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", val)
		}
		binary.Write(buf, binary.LittleEndian, v)

	case TypeIntN:
		v, ok := toInt64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", val)
		}
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

	case TypeBit:
		v, ok := toBool(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to bool", val)
		}
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

	case TypeBitN:
		v, ok := toBool(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to bool", val)
		}
		buf.WriteByte(1) // length
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

	case TypeFloat4:
		v, ok := toFloat64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to float", val)
		}
		binary.Write(buf, binary.LittleEndian, float32(v))

	case TypeFloat8:
		v, ok := toFloat64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to float", val)
		}
		binary.Write(buf, binary.LittleEndian, v)

	case TypeFloatN:
		v, ok := toFloat64(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to float", val)
		}
		buf.WriteByte(byte(col.Length))
		if col.Length == 4 {
			binary.Write(buf, binary.LittleEndian, float32(v))
		} else {
			binary.Write(buf, binary.LittleEndian, v)
		}

	case TypeNVarChar, TypeNChar:
		s := toString(val)
		data := stringToUCS2(s)
		if len(data) > int(col.Length) {
			data = data[:col.Length]
		}
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	case TypeBigVarChar, TypeBigChar:
		s := toString(val)
		data := []byte(s)
		if len(data) > int(col.Length) {
			data = data[:col.Length]
		}
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	case TypeBigVarBin, TypeBigBinary:
		data, ok := toBytes(val)
		if !ok {
			return fmt.Errorf("cannot convert %T to bytes", val)
		}
		if len(data) > int(col.Length) {
			data = data[:col.Length]
		}
		binary.Write(buf, binary.LittleEndian, uint16(len(data)))
		buf.Write(data)

	default:
		return fmt.Errorf("unsupported type: %s", col.Type)
	}

	return nil
}

// writeNull writes a NULL value for the given column type.
func (r *ResultSetWriter) writeNull(col Column) error {
	buf := &r.tw.buf

	switch col.Type {
	case TypeIntN, TypeBitN, TypeFloatN, TypeMoneyN, TypeDateTimeN, TypeGUID:
		buf.WriteByte(0) // 0 length = NULL

	case TypeNVarChar, TypeNChar, TypeBigVarChar, TypeBigChar,
		TypeBigVarBin, TypeBigBinary:
		binary.Write(buf, binary.LittleEndian, uint16(0xFFFF)) // -1 = NULL

	case TypeDecimalN, TypeNumericN:
		buf.WriteByte(0) // 0 length = NULL

	case TypeDateN:
		buf.WriteByte(0)

	case TypeTimeN, TypeDateTime2N, TypeDateTimeOffsetN:
		buf.WriteByte(0)

	default:
		// For fixed-length types that don't support NULL directly,
		// write zeros (shouldn't happen if column is marked nullable)
		return fmt.Errorf("type %s doesn't support NULL directly", col.Type)
	}

	return nil
}

// WriteDoneInProc writes a DONEINPROC token for result set completion.
func (r *ResultSetWriter) WriteDoneInProc(rowCount uint64) {
	r.tw.WriteDoneInProc(DoneCount|DoneMore, 0xC1, rowCount) // 0xC1 = SELECT
}

// Helper conversion functions.

func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	default:
		return 0, false
	}
}

func toBool(v interface{}) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case int:
		return x != 0, true
	case int64:
		return x != 0, true
	default:
		return false, false
	}
}

func toString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case decimal.Decimal:
		return x.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toBytes(v interface{}) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	default:
		return nil, false
	}
}

// SQL collation for Latin1_General_CI_AS (common default).
var DefaultCollation = []byte{0x09, 0x04, 0xD0, 0x00, 0x34}

// Time conversion helpers.
var baseDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

func encodeDatetime(t time.Time) (days int32, timeTicks int32) {
	days = int32(t.Sub(baseDate).Hours() / 24)
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	timeTicks = int32(t.Sub(midnight).Milliseconds() * 3 / 10) // 1/300th second ticks
	return
}

// WriteDateTime writes a DATETIME value.
func WriteDateTime(w io.Writer, t time.Time) {
	days, ticks := encodeDatetime(t)
	binary.Write(w, binary.LittleEndian, days)
	binary.Write(w, binary.LittleEndian, ticks)
}

// IEEE 754 helpers for special values.
func isNaN(f float64) bool {
	return math.IsNaN(f)
}

func isInf(f float64) bool {
	return math.IsInf(f, 0)
}
