package tsqlruntime

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// JSON Functions for T-SQL runtime
// Supports: JSON_VALUE, JSON_QUERY, JSON_MODIFY, ISJSON, OPENJSON (table)

// jsonParsePath parses a JSON path like '$.customer.name' or '$[0].id'
// Returns the path segments
func jsonParsePath(path string) []string {
	if path == "" || path == "$" {
		return nil
	}

	// Remove leading $
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	var segments []string
	var current strings.Builder
	inBracket := false

	for _, ch := range path {
		switch ch {
		case '.':
			if !inBracket && current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			} else if inBracket {
				current.WriteRune(ch)
			}
		case '[':
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}
			inBracket = true
		case ']':
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}
			inBracket = false
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		segments = append(segments, current.String())
	}

	return segments
}

// jsonNavigate navigates to a path in a parsed JSON structure
func jsonNavigate(data interface{}, path []string) (interface{}, bool) {
	current := data

	for _, segment := range path {
		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[segment]
			if !ok {
				return nil, false
			}
			current = val
		case []interface{}:
			// Array index
			idx, err := strconv.Atoi(segment)
			if err != nil || idx < 0 || idx >= len(v) {
				return nil, false
			}
			current = v[idx]
		default:
			return nil, false
		}
	}

	return current, true
}

// jsonSetValue sets a value at a path in a parsed JSON structure
func jsonSetValue(data interface{}, path []string, newValue interface{}) (interface{}, bool) {
	if len(path) == 0 {
		return newValue, true
	}

	switch v := data.(type) {
	case map[string]interface{}:
		if len(path) == 1 {
			v[path[0]] = newValue
			return v, true
		}
		child, ok := v[path[0]]
		if !ok {
			// Create intermediate object
			child = make(map[string]interface{})
			v[path[0]] = child
		}
		result, ok := jsonSetValue(child, path[1:], newValue)
		if ok {
			v[path[0]] = result
		}
		return v, ok

	case []interface{}:
		idx, err := strconv.Atoi(path[0])
		if err != nil || idx < 0 {
			return data, false
		}
		// Extend array if needed
		for len(v) <= idx {
			v = append(v, nil)
		}
		if len(path) == 1 {
			v[idx] = newValue
			return v, true
		}
		child := v[idx]
		if child == nil {
			child = make(map[string]interface{})
		}
		result, ok := jsonSetValue(child, path[1:], newValue)
		if ok {
			v[idx] = result
		}
		return v, ok

	default:
		return data, false
	}
}

// JSONValue implements JSON_VALUE - extracts a scalar value from JSON
func JSONValue(jsonStr string, path string) (Value, error) {
	if jsonStr == "" {
		return Null(TypeNVarChar), nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return Null(TypeNVarChar), nil // Invalid JSON returns NULL
	}

	segments := jsonParsePath(path)
	result, ok := jsonNavigate(data, segments)
	if !ok {
		return Null(TypeNVarChar), nil
	}

	// JSON_VALUE only returns scalar values
	switch v := result.(type) {
	case string:
		return NewNVarChar(v, -1), nil
	case float64:
		// Check if it's an integer
		if v == float64(int64(v)) {
			return NewNVarChar(strconv.FormatInt(int64(v), 10), -1), nil
		}
		return NewNVarChar(strconv.FormatFloat(v, 'f', -1, 64), -1), nil
	case bool:
		if v {
			return NewNVarChar("true", -1), nil
		}
		return NewNVarChar("false", -1), nil
	case nil:
		return Null(TypeNVarChar), nil
	default:
		// Objects and arrays return NULL in JSON_VALUE
		return Null(TypeNVarChar), nil
	}
}

// JSONQuery implements JSON_QUERY - extracts an object or array from JSON
func JSONQuery(jsonStr string, path string) (Value, error) {
	if jsonStr == "" {
		return Null(TypeNVarChar), nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return Null(TypeNVarChar), nil
	}

	segments := jsonParsePath(path)
	result, ok := jsonNavigate(data, segments)
	if !ok {
		return Null(TypeNVarChar), nil
	}

	// JSON_QUERY only returns objects or arrays
	switch v := result.(type) {
	case map[string]interface{}, []interface{}:
		bytes, err := json.Marshal(v)
		if err != nil {
			return Null(TypeNVarChar), nil
		}
		return NewNVarChar(string(bytes), -1), nil
	default:
		// Scalars return NULL in JSON_QUERY
		return Null(TypeNVarChar), nil
	}
}

// JSONModify implements JSON_MODIFY - modifies a value in JSON
func JSONModify(jsonStr string, path string, newValue interface{}) (Value, error) {
	if jsonStr == "" {
		jsonStr = "{}"
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return Null(TypeNVarChar), nil
	}

	segments := jsonParsePath(path)

	// Handle append mode (path ends with [append])
	appendMode := false
	if len(segments) > 0 && strings.ToLower(segments[len(segments)-1]) == "append" {
		appendMode = true
		segments = segments[:len(segments)-1]
	}

	if appendMode {
		// Navigate to the array and append
		target, ok := jsonNavigate(data, segments)
		if !ok {
			return Null(TypeNVarChar), nil
		}
		arr, ok := target.([]interface{})
		if !ok {
			return Null(TypeNVarChar), nil
		}
		arr = append(arr, newValue)
		data, _ = jsonSetValue(data, segments, arr)
	} else {
		data, _ = jsonSetValue(data, segments, newValue)
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return Null(TypeNVarChar), nil
	}

	return NewNVarChar(string(bytes), -1), nil
}

// IsJSON implements ISJSON - checks if a string is valid JSON
func IsJSON(str string) (Value, error) {
	if str == "" {
		return NewInt(0), nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(str), &data); err != nil {
		return NewInt(0), nil
	}

	return NewInt(1), nil
}

// OpenJSONResult represents a row from OPENJSON
type OpenJSONResult struct {
	Key   string
	Value string
	Type  int // 0=null, 1=string, 2=number, 3=bool, 4=array, 5=object
}

// OpenJSON implements OPENJSON - parses JSON into rows
func OpenJSON(jsonStr string, path string) ([]OpenJSONResult, error) {
	if jsonStr == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Navigate to path if specified
	if path != "" && path != "$" {
		segments := jsonParsePath(path)
		var ok bool
		data, ok = jsonNavigate(data, segments)
		if !ok {
			return nil, nil
		}
	}

	var results []OpenJSONResult

	switch v := data.(type) {
	case map[string]interface{}:
		for key, val := range v {
			results = append(results, openJSONValue(key, val))
		}
	case []interface{}:
		for i, val := range v {
			results = append(results, openJSONValue(strconv.Itoa(i), val))
		}
	default:
		results = append(results, openJSONValue("0", data))
	}

	return results, nil
}

func openJSONValue(key string, val interface{}) OpenJSONResult {
	result := OpenJSONResult{Key: key}

	switch v := val.(type) {
	case nil:
		result.Value = ""
		result.Type = 0
	case string:
		result.Value = v
		result.Type = 1
	case float64:
		if v == float64(int64(v)) {
			result.Value = strconv.FormatInt(int64(v), 10)
		} else {
			result.Value = strconv.FormatFloat(v, 'f', -1, 64)
		}
		result.Type = 2
	case bool:
		if v {
			result.Value = "true"
		} else {
			result.Value = "false"
		}
		result.Type = 3
	case []interface{}:
		bytes, _ := json.Marshal(v)
		result.Value = string(bytes)
		result.Type = 4
	case map[string]interface{}:
		bytes, _ := json.Marshal(v)
		result.Value = string(bytes)
		result.Type = 5
	}

	return result
}

// OpenJSONWithSchema parses JSON with a schema (WITH clause)
func OpenJSONWithSchema(jsonStr string, path string, schema []OpenJSONColumn) ([]map[string]Value, error) {
	if jsonStr == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Navigate to path if specified
	if path != "" && path != "$" {
		segments := jsonParsePath(path)
		var ok bool
		data, ok = jsonNavigate(data, segments)
		if !ok {
			return nil, nil
		}
	}

	var results []map[string]Value

	// Handle array or single object
	var items []interface{}
	switch v := data.(type) {
	case []interface{}:
		items = v
	case map[string]interface{}:
		items = []interface{}{v}
	default:
		return nil, nil
	}

	for _, item := range items {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		row := make(map[string]Value)
		for _, col := range schema {
			jsonPath := col.JSONPath
			if jsonPath == "" {
				jsonPath = "$." + col.Name
			}

			segments := jsonParsePath(jsonPath)
			val, found := jsonNavigate(obj, segments)

			if !found || val == nil {
				row[col.Name] = Null(col.Type)
				continue
			}

			row[col.Name] = convertJSONValue(val, col.Type)
		}
		results = append(results, row)
	}

	return results, nil
}

// OpenJSONColumn represents a column in OPENJSON WITH clause
type OpenJSONColumn struct {
	Name     string
	Type     DataType
	JSONPath string // Optional, defaults to $.Name
}

func convertJSONValue(val interface{}, targetType DataType) Value {
	switch v := val.(type) {
	case string:
		strVal := NewVarChar(v, -1)
		result, err := Cast(strVal, targetType, 0, 0, -1)
		if err != nil {
			return Null(targetType)
		}
		return result
	case float64:
		if targetType == TypeInt || targetType == TypeBigInt || targetType == TypeSmallInt {
			return NewInt(int64(v))
		}
		if targetType == TypeDecimal || targetType == TypeNumeric {
			return NewDecimal(decimal.NewFromFloat(v), 18, 4)
		}
		return NewFloat(v)
	case bool:
		if v {
			return NewBit(true)
		}
		return NewBit(false)
	case nil:
		return Null(targetType)
	default:
		// Object or array - convert to string
		bytes, _ := json.Marshal(v)
		return NewVarChar(string(bytes), -1)
	}
}

// ForJSON converts a result set to JSON
type ForJSONMode int

const (
	ForJSONAuto ForJSONMode = iota
	ForJSONPath
	ForJSONRaw
)

// ForJSONOptions holds options for FOR JSON clause
type ForJSONOptions struct {
	Mode            ForJSONMode
	RootName        string // ROOT('name')
	IncludeNullValues bool
	WithoutArrayWrapper bool
}

// ForJSON converts rows to JSON format
func ForJSON(columns []string, rows [][]Value, options ForJSONOptions) (string, error) {
	var result []map[string]interface{}

	for _, row := range rows {
		obj := make(map[string]interface{})
		for i, col := range columns {
			if i >= len(row) {
				continue
			}
			val := row[i]

			if val.IsNull && !options.IncludeNullValues {
				continue
			}

			if val.IsNull {
				obj[col] = nil
			} else {
				obj[col] = val.ToInterface()
			}
		}
		result = append(result, obj)
	}

	var output interface{}
	if options.WithoutArrayWrapper && len(result) == 1 {
		output = result[0]
	} else {
		output = result
	}

	if options.RootName != "" {
		output = map[string]interface{}{options.RootName: output}
	}

	bytes, err := json.Marshal(output)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

// Updated function registry entries
func fnJSONValueFull(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull || args[1].IsNull {
		return Null(TypeNVarChar), nil
	}
	return JSONValue(args[0].AsString(), args[1].AsString())
}

func fnJSONQueryFull(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull || args[1].IsNull {
		return Null(TypeNVarChar), nil
	}
	return JSONQuery(args[0].AsString(), args[1].AsString())
}

func fnJSONModifyFull(args []Value) (Value, error) {
	if len(args) < 3 || args[0].IsNull || args[1].IsNull {
		return Null(TypeNVarChar), nil
	}

	var newValue interface{}
	if args[2].IsNull {
		newValue = nil
	} else {
		newValue = args[2].ToInterface()
	}

	return JSONModify(args[0].AsString(), args[1].AsString(), newValue)
}

func fnIsJSONFull(args []Value) (Value, error) {
	if len(args) < 1 || args[0].IsNull {
		return Null(TypeInt), nil
	}
	return IsJSON(args[0].AsString())
}
