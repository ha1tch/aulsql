package tsqlruntime

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"strings"
)

// RegisterStage3Functions registers additional functions for Stage 3
// Note: Many functions already exist in functions.go, so only truly new ones are added here
func RegisterStage3Functions(registry *FunctionRegistry) {
	// Hash functions
	registry.Register("HASHBYTES", fnHashBytes)
	registry.Register("CHECKSUM", fnChecksum)
	registry.Register("BINARY_CHECKSUM", fnBinaryChecksum)

	// JSON functions (full implementations in json.go)
	registry.Register("ISJSON", fnIsJSONFull)
	registry.Register("JSON_VALUE", fnJSONValueFull)
	registry.Register("JSON_QUERY", fnJSONQueryFull)
	registry.Register("JSON_MODIFY", fnJSONModifyFull)

	// XML functions (implementations in xml.go)
	registry.Register("XML_VALUE", fnXMLValue)
	registry.Register("XML_QUERY", fnXMLQuery)
	registry.Register("XML_EXIST", fnXMLExist)

	// Logical functions
	registry.Register("GREATEST", fnGreatest)
	registry.Register("LEAST", fnLeast)

	// System functions
	registry.Register("HOST_NAME", fnHostName)
	registry.Register("APP_NAME", fnAppName)
	registry.Register("SUSER_NAME", fnSuserName)
	registry.Register("SUSER_SNAME", fnSuserSname)
	registry.Register("USER_NAME", fnUserName)
	registry.Register("SYSTEM_USER", fnSystemUser)
	registry.Register("SESSION_USER", fnSessionUser)
	registry.Register("CURRENT_USER", fnCurrentUser)
	registry.Register("ORIGINAL_LOGIN", fnOriginalLogin)

	// Conversion functions
	registry.Register("TRY_CAST", fnTryCast)
	registry.Register("TRY_CONVERT", fnTryConvert)
	registry.Register("TRY_PARSE", fnTryParse)
	registry.Register("PARSE", fnParse)

	// Metadata functions not already registered
	registry.Register("COL_NAME", fnColName)
	registry.Register("COL_LENGTH", fnColLength)
	registry.Register("COLUMNPROPERTY", fnColumnProperty)
	registry.Register("TYPE_ID", fnTypeId)
	registry.Register("TYPE_NAME", fnTypeName)
}

// Hash functions

func fnHashBytes(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull || args[1].IsNull {
		return Null(TypeVarBinary), nil
	}

	algorithm := strings.ToUpper(args[0].AsString())
	data := []byte(args[1].AsString())

	var hash []byte
	switch algorithm {
	case "MD5":
		h := md5.Sum(data)
		hash = h[:]
	case "SHA", "SHA1":
		h := sha1.Sum(data)
		hash = h[:]
	case "SHA2_256", "SHA256":
		h := sha256.Sum256(data)
		hash = h[:]
	case "SHA2_512", "SHA512":
		h := sha512.Sum512(data)
		hash = h[:]
	default:
		return Null(TypeVarBinary), fmt.Errorf("unknown hash algorithm: %s", algorithm)
	}

	return NewVarBinary(hash, len(hash)), nil
}

func fnChecksum(args []Value) (Value, error) {
	if len(args) < 1 {
		return NewInt(0), nil
	}

	var checksum int64
	for _, arg := range args {
		if arg.IsNull {
			continue
		}
		s := arg.AsString()
		for _, c := range s {
			checksum = (checksum*31 + int64(c)) & 0x7FFFFFFF
		}
	}

	return NewInt(checksum), nil
}

func fnBinaryChecksum(args []Value) (Value, error) {
	return fnChecksum(args)
}

// Logical functions

func fnGreatest(args []Value) (Value, error) {
	if len(args) == 0 {
		return Null(TypeUnknown), nil
	}

	var greatest Value
	for _, arg := range args {
		if arg.IsNull {
			continue
		}
		if greatest.Type == TypeUnknown || greatest.IsNull || arg.Compare(greatest) > 0 {
			greatest = arg
		}
	}

	return greatest, nil
}

func fnLeast(args []Value) (Value, error) {
	if len(args) == 0 {
		return Null(TypeUnknown), nil
	}

	var least Value
	for _, arg := range args {
		if arg.IsNull {
			continue
		}
		if least.Type == TypeUnknown || least.IsNull || arg.Compare(least) < 0 {
			least = arg
		}
	}

	return least, nil
}

// System functions (defined in functions.go)
// fnHostName, fnAppName, fnUserName, fnSystemUser, fnSessionUser,
// fnCurrentUser, fnOriginalLogin are now in functions.go with more complete implementations.

func fnSuserName(args []Value) (Value, error) {
	return NewNVarChar("sa", 128), nil
}

func fnSuserSname(args []Value) (Value, error) {
	return NewNVarChar("sa", 128), nil
}

// Conversion functions

func fnTryCast(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull {
		return Null(TypeUnknown), nil
	}

	// Parse target type from second arg
	targetTypeStr := args[1].AsString()
	targetType, prec, scale, maxLen := ParseDataType(targetTypeStr)

	result, err := Cast(args[0], targetType, prec, scale, maxLen)
	if err != nil {
		return Null(targetType), nil // TRY_CAST returns NULL on failure
	}
	return result, nil
}

func fnTryConvert(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull {
		return Null(TypeUnknown), nil
	}

	targetTypeStr := args[0].AsString()
	targetType, prec, scale, maxLen := ParseDataType(targetTypeStr)

	style := 0
	if len(args) >= 3 {
		style = int(args[2].AsInt())
	}

	result, err := Convert(args[1], targetType, prec, scale, maxLen, style)
	if err != nil {
		return Null(targetType), nil
	}
	return result, nil
}

func fnTryParse(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull {
		return Null(TypeUnknown), nil
	}

	targetTypeStr := args[1].AsString()
	targetType, prec, scale, maxLen := ParseDataType(targetTypeStr)

	result, err := Cast(args[0], targetType, prec, scale, maxLen)
	if err != nil {
		return Null(targetType), nil
	}
	return result, nil
}

func fnParse(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull {
		return Null(TypeUnknown), nil
	}

	targetTypeStr := args[1].AsString()
	targetType, prec, scale, maxLen := ParseDataType(targetTypeStr)

	return Cast(args[0], targetType, prec, scale, maxLen)
}

// Metadata functions

func fnColName(args []Value) (Value, error) {
	if len(args) < 2 {
		return Null(TypeNVarChar), nil
	}
	return NewNVarChar(fmt.Sprintf("column_%d", args[1].AsInt()), 128), nil
}

func fnColLength(args []Value) (Value, error) {
	if len(args) < 2 {
		return Null(TypeInt), nil
	}
	return NewInt(4), nil
}

// fnColumnProperty is now in functions.go with more complete implementation

func fnTypeId(args []Value) (Value, error) {
	if len(args) < 1 || args[0].IsNull {
		return Null(TypeInt), nil
	}
	return NewInt(1), nil
}

func fnTypeName(args []Value) (Value, error) {
	if len(args) < 1 || args[0].IsNull {
		return Null(TypeNVarChar), nil
	}
	return NewNVarChar("int", 128), nil
}
