package tsqlruntime

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// XML Functions for T-SQL runtime
// Supports: .value(), .query(), .exist(), .nodes(), FOR XML

// XMLNode represents a parsed XML node
type XMLNode struct {
	Name       string
	Value      string
	Attributes map[string]string
	Children   []*XMLNode
	Parent     *XMLNode
}

// ParseXML parses an XML string into a tree structure
func ParseXML(xmlStr string) (*XMLNode, error) {
	if xmlStr == "" {
		return nil, nil
	}

	// Wrap in root if needed
	xmlStr = strings.TrimSpace(xmlStr)
	if !strings.HasPrefix(xmlStr, "<?xml") && !strings.HasPrefix(xmlStr, "<") {
		return nil, fmt.Errorf("invalid XML")
	}

	decoder := xml.NewDecoder(strings.NewReader(xmlStr))

	var root *XMLNode
	var current *XMLNode
	var stack []*XMLNode

	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			node := &XMLNode{
				Name:       t.Name.Local,
				Attributes: make(map[string]string),
				Children:   make([]*XMLNode, 0),
			}
			for _, attr := range t.Attr {
				node.Attributes[attr.Name.Local] = attr.Value
			}

			if current != nil {
				node.Parent = current
				current.Children = append(current.Children, node)
			}
			if root == nil {
				root = node
			}

			stack = append(stack, current)
			current = node

		case xml.EndElement:
			if len(stack) > 0 {
				current = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}

		case xml.CharData:
			if current != nil {
				text := strings.TrimSpace(string(t))
				if text != "" {
					current.Value = text
				}
			}
		}
	}

	return root, nil
}

// XPath query support (simplified subset)
// Supports: /root/child, /root/child/@attr, /root/child[1], /root/child[last()]

// xpathPart represents a part of an XPath expression
type xpathPart struct {
	Name      string
	Attribute string // @name
	Index     int    // [1], [2], etc. (1-based, 0 means all)
	IsLast    bool   // [last()]
}

// parseXPath parses a simplified XPath expression
func parseXPath(xpath string) []xpathPart {
	xpath = strings.TrimSpace(xpath)
	if strings.HasPrefix(xpath, "(") && strings.HasSuffix(xpath, ")") {
		// Remove outer parentheses like (/root/item)[1]
		idx := strings.LastIndex(xpath, ")")
		inner := xpath[1:idx]
		suffix := xpath[idx+1:]

		parts := parseXPath(inner)
		if len(parts) > 0 && strings.HasPrefix(suffix, "[") {
			// Parse index suffix
			idxMatch := regexp.MustCompile(`\[(\d+|last\(\))\]`).FindStringSubmatch(suffix)
			if len(idxMatch) > 1 {
				if idxMatch[1] == "last()" {
					parts[len(parts)-1].IsLast = true
				} else if n, err := strconv.Atoi(idxMatch[1]); err == nil {
					parts[len(parts)-1].Index = n
				}
			}
		}
		return parts
	}

	var parts []xpathPart
	segments := strings.Split(strings.TrimPrefix(xpath, "/"), "/")

	for _, seg := range segments {
		if seg == "" {
			continue
		}

		part := xpathPart{}

		// Check for attribute
		if strings.HasPrefix(seg, "@") {
			part.Attribute = strings.TrimPrefix(seg, "@")
			parts = append(parts, part)
			continue
		}

		// Check for predicate [n] or [last()]
		if idx := strings.Index(seg, "["); idx != -1 {
			part.Name = seg[:idx]
			predicate := seg[idx:]

			if strings.Contains(predicate, "last()") {
				part.IsLast = true
			} else {
				idxMatch := regexp.MustCompile(`\[(\d+)\]`).FindStringSubmatch(predicate)
				if len(idxMatch) > 1 {
					n, _ := strconv.Atoi(idxMatch[1])
					part.Index = n
				}
			}
		} else {
			part.Name = seg
		}

		parts = append(parts, part)
	}

	return parts
}

// evaluateXPath evaluates an XPath expression against an XML node
func evaluateXPath(node *XMLNode, parts []xpathPart) []*XMLNode {
	if node == nil || len(parts) == 0 {
		return []*XMLNode{node}
	}

	current := []*XMLNode{node}

	for i, part := range parts {
		var next []*XMLNode

		for _, n := range current {
			if n == nil {
				continue
			}

			// Handle attribute
			if part.Attribute != "" {
				if val, ok := n.Attributes[part.Attribute]; ok {
					// Return as a synthetic node with the value
					next = append(next, &XMLNode{Value: val})
				}
				continue
			}

			// First part might match the node itself (for paths like /root/item where node IS root)
			if i == 0 && n.Name == part.Name {
				next = append(next, n)
				continue
			}

			// Handle element - look in children
			for _, child := range n.Children {
				if child.Name == part.Name || part.Name == "*" {
					next = append(next, child)
				}
			}
		}

		// Apply index/last filter
		if part.Index > 0 && len(next) >= part.Index {
			next = []*XMLNode{next[part.Index-1]}
		} else if part.IsLast && len(next) > 0 {
			next = []*XMLNode{next[len(next)-1]}
		}

		current = next
	}

	return current
}

// XMLValue implements .value() - extracts a scalar value from XML
func XMLValue(xmlStr string, xpath string, targetType DataType) (Value, error) {
	root, err := ParseXML(xmlStr)
	if err != nil || root == nil {
		return Null(targetType), nil
	}

	parts := parseXPath(xpath)
	results := evaluateXPath(root, parts)

	if len(results) == 0 || results[0] == nil {
		return Null(targetType), nil
	}

	value := results[0].Value
	if value == "" && len(results[0].Attributes) > 0 {
		// Might be an attribute result
		for _, v := range results[0].Attributes {
			value = v
			break
		}
	}

	// Convert to target type
	strVal := NewVarChar(value, -1)
	return Cast(strVal, targetType, 0, 0, -1)
}

// XMLQuery implements .query() - extracts XML fragment
func XMLQuery(xmlStr string, xpath string) (Value, error) {
	root, err := ParseXML(xmlStr)
	if err != nil || root == nil {
		return Null(TypeXML), nil
	}

	parts := parseXPath(xpath)
	results := evaluateXPath(root, parts)

	if len(results) == 0 {
		return Null(TypeXML), nil
	}

	// Serialize results back to XML
	var builder strings.Builder
	for _, node := range results {
		serializeXML(node, &builder)
	}

	return NewXML(builder.String()), nil
}

// XMLExist implements .exist() - checks if XPath matches
func XMLExist(xmlStr string, xpath string) (Value, error) {
	root, err := ParseXML(xmlStr)
	if err != nil || root == nil {
		return NewInt(0), nil
	}

	parts := parseXPath(xpath)
	results := evaluateXPath(root, parts)

	if len(results) > 0 && results[0] != nil {
		return NewInt(1), nil
	}
	return NewInt(0), nil
}

// XMLNodesResult represents a row from .nodes()
type XMLNodesResult struct {
	Node *XMLNode
}

// XMLNodes implements .nodes() - shreds XML into rows
func XMLNodes(xmlStr string, xpath string) ([]XMLNodesResult, error) {
	root, err := ParseXML(xmlStr)
	if err != nil || root == nil {
		return nil, nil
	}

	parts := parseXPath(xpath)
	results := evaluateXPath(root, parts)

	var output []XMLNodesResult
	for _, node := range results {
		if node != nil {
			output = append(output, XMLNodesResult{Node: node})
		}
	}

	return output, nil
}

// serializeXML converts an XMLNode back to XML string
func serializeXML(node *XMLNode, builder *strings.Builder) {
	if node == nil {
		return
	}

	builder.WriteString("<")
	builder.WriteString(node.Name)

	for name, value := range node.Attributes {
		builder.WriteString(" ")
		builder.WriteString(name)
		builder.WriteString("=\"")
		builder.WriteString(xmlEscape(value))
		builder.WriteString("\"")
	}

	if len(node.Children) == 0 && node.Value == "" {
		builder.WriteString("/>")
		return
	}

	builder.WriteString(">")

	if node.Value != "" {
		builder.WriteString(xmlEscape(node.Value))
	}

	for _, child := range node.Children {
		serializeXML(child, builder)
	}

	builder.WriteString("</")
	builder.WriteString(node.Name)
	builder.WriteString(">")
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

// FOR XML support

// ForXMLMode represents the FOR XML mode
type ForXMLMode int

const (
	ForXMLRaw ForXMLMode = iota
	ForXMLAuto
	ForXMLPath
	ForXMLExplicit
)

// ForXMLOptions holds options for FOR XML clause
type ForXMLOptions struct {
	Mode        ForXMLMode
	ElementName string // For RAW('name') or PATH('name')
	RootName    string // ROOT('name')
	Elements    bool   // ELEMENTS option
	XSINil      bool   // XSINIL option
}

// ForXML converts rows to XML format
func ForXML(columns []string, rows [][]Value, options ForXMLOptions) (string, error) {
	var builder strings.Builder

	if options.RootName != "" {
		builder.WriteString("<")
		builder.WriteString(options.RootName)
		builder.WriteString(">")
	}

	elementName := options.ElementName
	if elementName == "" {
		switch options.Mode {
		case ForXMLRaw:
			elementName = "row"
		case ForXMLPath:
			elementName = "row"
		}
	}

	for _, row := range rows {
		builder.WriteString("<")
		builder.WriteString(elementName)

		if options.Elements {
			builder.WriteString(">")
			for i, col := range columns {
				if i >= len(row) {
					continue
				}
				val := row[i]
				if val.IsNull {
					if options.XSINil {
						builder.WriteString("<")
						builder.WriteString(col)
						builder.WriteString(" xsi:nil=\"true\"/>")
					}
					continue
				}
				builder.WriteString("<")
				builder.WriteString(col)
				builder.WriteString(">")
				builder.WriteString(xmlEscape(val.AsString()))
				builder.WriteString("</")
				builder.WriteString(col)
				builder.WriteString(">")
			}
			builder.WriteString("</")
			builder.WriteString(elementName)
			builder.WriteString(">")
		} else {
			// Attributes mode
			for i, col := range columns {
				if i >= len(row) {
					continue
				}
				val := row[i]
				if val.IsNull {
					continue
				}
				builder.WriteString(" ")
				builder.WriteString(col)
				builder.WriteString("=\"")
				builder.WriteString(xmlEscape(val.AsString()))
				builder.WriteString("\"")
			}
			builder.WriteString("/>")
		}
	}

	if options.RootName != "" {
		builder.WriteString("</")
		builder.WriteString(options.RootName)
		builder.WriteString(">")
	}

	return builder.String(), nil
}

// NewXML creates a new XML value
func NewXML(s string) Value {
	return Value{Type: TypeXML, stringVal: s}
}

// OpenXML support

// OpenXMLResult represents the result of OPENXML
type OpenXMLResult struct {
	Rows []map[string]Value
}

// OpenXML implements OPENXML for parsing XML into a table
func OpenXML(xmlStr string, xpath string, flags int, schema []OpenXMLColumn) (*OpenXMLResult, error) {
	root, err := ParseXML(xmlStr)
	if err != nil || root == nil {
		return nil, err
	}

	parts := parseXPath(xpath)
	nodes := evaluateXPath(root, parts)

	result := &OpenXMLResult{
		Rows: make([]map[string]Value, 0),
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}

		row := make(map[string]Value)
		for _, col := range schema {
			var val string
			found := false

			// Check if it's an attribute (xpath starts with @)
			colPath := col.XPath
			if colPath == "" {
				colPath = col.Name
			}

			if strings.HasPrefix(colPath, "@") {
				attrName := strings.TrimPrefix(colPath, "@")
				if v, ok := node.Attributes[attrName]; ok {
					val = v
					found = true
				}
			} else {
				// Look for child element
				colParts := parseXPath(colPath)
				children := evaluateXPath(node, colParts)
				if len(children) > 0 && children[0] != nil {
					val = children[0].Value
					found = true
				}
			}

			if found {
				strVal := NewVarChar(val, -1)
				converted, err := Cast(strVal, col.Type, 0, 0, col.MaxLen)
				if err != nil {
					row[col.Name] = Null(col.Type)
				} else {
					row[col.Name] = converted
				}
			} else {
				row[col.Name] = Null(col.Type)
			}
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// OpenXMLColumn represents a column definition for OPENXML
type OpenXMLColumn struct {
	Name   string
	Type   DataType
	MaxLen int
	XPath  string // Optional XPath for the column
}

// Function registry entries for XML

func fnXMLValue(args []Value) (Value, error) {
	if len(args) < 3 || args[0].IsNull || args[1].IsNull {
		return Null(TypeNVarChar), nil
	}

	xmlStr := args[0].AsString()
	xpath := args[1].AsString()
	typeStr := args[2].AsString()

	targetType, _, _, _ := ParseDataType(typeStr)
	return XMLValue(xmlStr, xpath, targetType)
}

func fnXMLQuery(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull || args[1].IsNull {
		return Null(TypeXML), nil
	}
	return XMLQuery(args[0].AsString(), args[1].AsString())
}

func fnXMLExist(args []Value) (Value, error) {
	if len(args) < 2 || args[0].IsNull || args[1].IsNull {
		return NewInt(0), nil
	}
	return XMLExist(args[0].AsString(), args[1].AsString())
}
