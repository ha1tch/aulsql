package tds

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestProcIDName(t *testing.T) {
	tests := []struct {
		id   uint16
		name string
	}{
		{ProcIDExecuteSQL, "sp_executesql"},
		{ProcIDPrepare, "sp_prepare"},
		{ProcIDExecute, "sp_execute"},
		{ProcIDUnprepare, "sp_unprepare"},
		{ProcIDCursor, "sp_cursor"},
		{ProcIDCursorOpen, "sp_cursoropen"},
		{ProcIDCursorFetch, "sp_cursorfetch"},
		{ProcIDCursorClose, "sp_cursorclose"},
		{999, "sp_unknown_999"},
	}

	for _, tt := range tests {
		got := ProcIDName(tt.id)
		if got != tt.name {
			t.Errorf("ProcIDName(%d) = %q, want %q", tt.id, got, tt.name)
		}
	}
}

func TestParseRPCRequest_SpExecuteSQL(t *testing.T) {
	// Build a minimal sp_executesql RPC request
	var buf bytes.Buffer

	// ALL_HEADERS (minimal - just total length)
	binary.Write(&buf, binary.LittleEndian, uint32(4)) // Total length = 4 (just this field)

	// Procedure ID (0xFFFF = use ID, then ID value)
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))

	// Option flags
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// Parameter 1: @stmt (NVARCHAR)
	// Name: empty (positional)
	buf.WriteByte(0) // name length = 0
	// Status
	buf.WriteByte(0) // not output
	// Type: NVARCHAR
	buf.WriteByte(byte(TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000)) // max size
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})       // collation
	// Value: "SELECT 1"
	sqlText := encodeUTF16LE("SELECT 1")
	binary.Write(&buf, binary.LittleEndian, uint16(len(sqlText)))
	buf.Write(sqlText)

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if req.ProcID != ProcIDExecuteSQL {
		t.Errorf("ProcID = %d, want %d", req.ProcID, ProcIDExecuteSQL)
	}
	if req.ProcName != "sp_executesql" {
		t.Errorf("ProcName = %q, want %q", req.ProcName, "sp_executesql")
	}
	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	param := req.Parameters[0]
	if param.IsNull {
		t.Error("Parameter should not be null")
	}
	if param.IsOutput {
		t.Error("Parameter should not be output")
	}
	val, ok := param.Value.(string)
	if !ok {
		t.Fatalf("Parameter value is %T, want string", param.Value)
	}
	if val != "SELECT 1" {
		t.Errorf("Parameter value = %q, want %q", val, "SELECT 1")
	}
}

func TestParseRPCRequest_NamedProcedure(t *testing.T) {
	var buf bytes.Buffer

	// ALL_HEADERS
	binary.Write(&buf, binary.LittleEndian, uint32(4))

	// Procedure name: "dbo.MyProc"
	procName := "dbo.MyProc"
	procNameUTF16 := encodeUTF16LE(procName)
	binary.Write(&buf, binary.LittleEndian, uint16(len(procName))) // length in chars
	buf.Write(procNameUTF16)

	// Option flags
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// Parameter: @id INT = 42
	// Name: "@id"
	paramName := "@id"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	// Status
	buf.WriteByte(0)
	// Type: INTN with size 4
	buf.WriteByte(byte(TypeIntN))
	buf.WriteByte(4) // size
	// Value
	buf.WriteByte(4) // actual size
	binary.Write(&buf, binary.LittleEndian, int32(42))

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if req.ProcID != 0 {
		t.Errorf("ProcID = %d, want 0 (named proc)", req.ProcID)
	}
	if req.ProcName != "dbo.MyProc" {
		t.Errorf("ProcName = %q, want %q", req.ProcName, "dbo.MyProc")
	}
	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	param := req.Parameters[0]
	if param.Name != "id" { // @ should be stripped
		t.Errorf("Parameter name = %q, want %q", param.Name, "id")
	}
	val, ok := param.Value.(int64)
	if !ok {
		t.Fatalf("Parameter value is %T, want int64", param.Value)
	}
	if val != 42 {
		t.Errorf("Parameter value = %d, want 42", val)
	}
}

func TestParseRPCRequest_OutputParameter(t *testing.T) {
	var buf bytes.Buffer

	// ALL_HEADERS
	binary.Write(&buf, binary.LittleEndian, uint32(4))

	// Procedure name
	procName := "GetNextID"
	procNameUTF16 := encodeUTF16LE(procName)
	binary.Write(&buf, binary.LittleEndian, uint16(len(procName)))
	buf.Write(procNameUTF16)

	// Option flags
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// Output parameter: @nextID INT OUTPUT
	paramName := "@nextID"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	// Status: OUTPUT
	buf.WriteByte(ParamByRefValue)
	// Type: INTN with size 4
	buf.WriteByte(byte(TypeIntN))
	buf.WriteByte(4)
	// Value: NULL (output params typically start null)
	buf.WriteByte(0)

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	param := req.Parameters[0]
	if !param.IsOutput {
		t.Error("Parameter should be marked as output")
	}
	if !param.IsNull {
		t.Error("Parameter should be null")
	}
}

func TestParseRPCRequest_MultipleParameters(t *testing.T) {
	var buf bytes.Buffer

	// ALL_HEADERS
	binary.Write(&buf, binary.LittleEndian, uint32(4))

	// sp_executesql with params
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))

	// Option flags
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// Param 1: @stmt NVARCHAR
	buf.WriteByte(0) // no name
	buf.WriteByte(0) // status
	buf.WriteByte(byte(TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	sql := encodeUTF16LE("SELECT * FROM users WHERE id = @id AND name = @name")
	binary.Write(&buf, binary.LittleEndian, uint16(len(sql)))
	buf.Write(sql)

	// Param 2: @params NVARCHAR (parameter definitions)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	params := encodeUTF16LE("@id INT, @name NVARCHAR(100)")
	binary.Write(&buf, binary.LittleEndian, uint16(len(params)))
	buf.Write(params)

	// Param 3: @id INT = 123
	idName := "@id"
	idNameUTF16 := encodeUTF16LE(idName)
	buf.WriteByte(byte(len(idName)))
	buf.Write(idNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeIntN))
	buf.WriteByte(4)
	buf.WriteByte(4)
	binary.Write(&buf, binary.LittleEndian, int32(123))

	// Param 4: @name NVARCHAR = 'Alice'
	nameName := "@name"
	nameNameUTF16 := encodeUTF16LE(nameName)
	buf.WriteByte(byte(len(nameName)))
	buf.Write(nameNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(200))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	nameVal := encodeUTF16LE("Alice")
	binary.Write(&buf, binary.LittleEndian, uint16(len(nameVal)))
	buf.Write(nameVal)

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 4 {
		t.Fatalf("len(Parameters) = %d, want 4", len(req.Parameters))
	}

	// Check @id
	idParam := req.Parameters[2]
	if idParam.Name != "id" {
		t.Errorf("Param 3 name = %q, want %q", idParam.Name, "id")
	}
	if v, ok := idParam.Value.(int64); !ok || v != 123 {
		t.Errorf("Param 3 value = %v, want 123", idParam.Value)
	}

	// Check @name
	nameParam := req.Parameters[3]
	if nameParam.Name != "name" {
		t.Errorf("Param 4 name = %q, want %q", nameParam.Name, "name")
	}
	if v, ok := nameParam.Value.(string); !ok || v != "Alice" {
		t.Errorf("Param 4 value = %q, want %q", nameParam.Value, "Alice")
	}
}

func TestParseRPCRequest_NullParameter(t *testing.T) {
	var buf bytes.Buffer

	// ALL_HEADERS
	binary.Write(&buf, binary.LittleEndian, uint32(4))

	// Named proc
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @stmt = NULL
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeNVarChar))
	binary.Write(&buf, binary.LittleEndian, uint16(8000))
	buf.Write([]byte{0x09, 0x04, 0xD0, 0x00, 0x34})
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF)) // NULL marker

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}
	if !req.Parameters[0].IsNull {
		t.Error("Parameter should be null")
	}
}

func TestParseRPCRequest_BitParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @flag BIT = 1
	paramName := "@flag"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeBitN))
	buf.WriteByte(1) // max size
	buf.WriteByte(1) // actual size
	buf.WriteByte(1) // value = true

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}
	if v, ok := req.Parameters[0].Value.(bool); !ok || !v {
		t.Errorf("Parameter value = %v, want true", req.Parameters[0].Value)
	}
}

func TestParseRPCRequest_FloatParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @price FLOAT = 3.14159
	paramName := "@price"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeFloatN))
	buf.WriteByte(8) // max size (FLOAT = 8 bytes)
	buf.WriteByte(8) // actual size
	binary.Write(&buf, binary.LittleEndian, float64(3.14159))

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}
	v, ok := req.Parameters[0].Value.(float64)
	if !ok {
		t.Fatalf("Parameter value is %T, want float64", req.Parameters[0].Value)
	}
	if v != 3.14159 {
		t.Errorf("Parameter value = %f, want 3.14159", v)
	}
}

func TestParseRPCRequest_VarBinaryParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @data VARBINARY = 0xDEADBEEF
	paramName := "@data"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeBigVarBin))
	binary.Write(&buf, binary.LittleEndian, uint16(8000)) // max size
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	binary.Write(&buf, binary.LittleEndian, uint16(len(data)))
	buf.Write(data)

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}
	v, ok := req.Parameters[0].Value.([]byte)
	if !ok {
		t.Fatalf("Parameter value is %T, want []byte", req.Parameters[0].Value)
	}
	if !bytes.Equal(v, data) {
		t.Errorf("Parameter value = %x, want %x", v, data)
	}
}

// Helper to encode string as UTF-16LE
func encodeUTF16LE(s string) []byte {
	b := make([]byte, len(s)*2)
	for i, r := range s {
		b[i*2] = byte(r)
		b[i*2+1] = byte(r >> 8)
	}
	return b
}

func TestParseRPCRequest_DateTimeParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @dt DATETIME = '2024-06-15 14:30:00'
	paramName := "@dt"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeDateTimeN))
	buf.WriteByte(8) // max size
	buf.WriteByte(8) // actual size
	// Days since 1900-01-01: 2024-06-15 is 45456 days from 1900-01-01
	// Time: 14:30:00 = (14*3600 + 30*60) * 300 = 15660000 ticks
	binary.Write(&buf, binary.LittleEndian, int32(45456))
	binary.Write(&buf, binary.LittleEndian, uint32(15660000))

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	param := req.Parameters[0]
	if param.IsNull {
		t.Error("Parameter should not be null")
	}

	v, ok := param.Value.(time.Time)
	if !ok {
		t.Fatalf("Parameter value is %T, want time.Time", param.Value)
	}

	if v.Year() != 2024 || v.Month() != 6 || v.Day() != 15 {
		t.Errorf("Date = %v, want 2024-06-15", v.Format("2006-01-02"))
	}
	if v.Hour() != 14 || v.Minute() != 30 {
		t.Errorf("Time = %v, want 14:30", v.Format("15:04"))
	}
}

func TestParseRPCRequest_GUIDParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	// @guid UNIQUEIDENTIFIER
	paramName := "@guid"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeGUID))
	buf.WriteByte(16) // max size
	buf.WriteByte(16) // actual size
	// GUID bytes (SQL Server byte order)
	guid := []byte{
		0x78, 0x56, 0x34, 0x12, // 12345678 reversed
		0x34, 0x12,             // 1234 reversed
		0x78, 0x56,             // 5678 reversed
		0x9A, 0xBC,             // 9ABC
		0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, // DEF012345678
	}
	buf.Write(guid)

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	param := req.Parameters[0]
	v, ok := param.Value.(string)
	if !ok {
		t.Fatalf("Parameter value is %T, want string", param.Value)
	}

	expected := "12345678-1234-5678-9ABC-DEF012345678"
	if v != expected {
		t.Errorf("GUID = %q, want %q", v, expected)
	}
}

func TestParseRPCRequest_BigIntParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	paramName := "@big"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeIntN))
	buf.WriteByte(8) // max size
	buf.WriteByte(8) // actual size
	binary.Write(&buf, binary.LittleEndian, int64(9223372036854775807))

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	v, ok := req.Parameters[0].Value.(int64)
	if !ok {
		t.Fatalf("Parameter value is %T, want int64", req.Parameters[0].Value)
	}
	if v != 9223372036854775807 {
		t.Errorf("Value = %d, want 9223372036854775807", v)
	}
}

func TestParseRPCRequest_SmallIntParameter(t *testing.T) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(4))
	binary.Write(&buf, binary.LittleEndian, uint16(0xFFFF))
	binary.Write(&buf, binary.LittleEndian, uint16(ProcIDExecuteSQL))
	binary.Write(&buf, binary.LittleEndian, uint16(0))

	paramName := "@small"
	paramNameUTF16 := encodeUTF16LE(paramName)
	buf.WriteByte(byte(len(paramName)))
	buf.Write(paramNameUTF16)
	buf.WriteByte(0)
	buf.WriteByte(byte(TypeIntN))
	buf.WriteByte(2) // max size (SMALLINT)
	buf.WriteByte(2) // actual size
	binary.Write(&buf, binary.LittleEndian, int16(-12345))

	req, err := ParseRPCRequest(buf.Bytes(), VerTDS74)
	if err != nil {
		t.Fatalf("ParseRPCRequest failed: %v", err)
	}

	if len(req.Parameters) != 1 {
		t.Fatalf("len(Parameters) = %d, want 1", len(req.Parameters))
	}

	v, ok := req.Parameters[0].Value.(int64)
	if !ok {
		t.Fatalf("Parameter value is %T, want int64", req.Parameters[0].Value)
	}
	if v != -12345 {
		t.Errorf("Value = %d, want -12345", v)
	}
}
