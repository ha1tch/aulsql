package tsqlruntime

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// TempTable represents an in-memory temporary table (#table or ##table)
type TempTable struct {
	Name       string
	Columns    []TempTableColumn
	Rows       [][]Value
	PrimaryKey []string
	Indexes    map[string]*TempTableIndex
	mu         sync.RWMutex
}

// TempTableColumn represents a column in a temp table
type TempTableColumn struct {
	Name         string
	Type         DataType
	Precision    int
	Scale        int
	MaxLen       int
	Nullable     bool
	DefaultValue Value
	Identity     bool
	IdentitySeed int64
	IdentityIncr int64
}

// TempTableIndex represents an index on a temp table
type TempTableIndex struct {
	Name      string
	Columns   []string
	Unique    bool
	Clustered bool
}

// TableVariable represents a table variable (@table)
type TableVariable struct {
	*TempTable
}

// TempTableManager manages temporary tables for a session
type TempTableManager struct {
	localTables  map[string]*TempTable  // #tables - session scoped
	globalTables map[string]*TempTable  // ##tables - global (simplified)
	tableVars    map[string]*TableVariable
	mu           sync.RWMutex
}

// NewTempTableManager creates a new temp table manager
func NewTempTableManager() *TempTableManager {
	return &TempTableManager{
		localTables:  make(map[string]*TempTable),
		globalTables: make(map[string]*TempTable),
		tableVars:    make(map[string]*TableVariable),
	}
}

// CreateTempTable creates a new temporary table
func (m *TempTableManager) CreateTempTable(name string, columns []TempTableColumn) (*TempTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Normalize name
	name = strings.ToLower(name)
	isGlobal := strings.HasPrefix(name, "##")

	// Check if already exists
	if isGlobal {
		if _, exists := m.globalTables[name]; exists {
			return nil, fmt.Errorf("temp table %s already exists", name)
		}
	} else {
		if _, exists := m.localTables[name]; exists {
			return nil, fmt.Errorf("temp table %s already exists", name)
		}
	}

	table := &TempTable{
		Name:    name,
		Columns: columns,
		Rows:    make([][]Value, 0),
		Indexes: make(map[string]*TempTableIndex),
	}

	if isGlobal {
		m.globalTables[name] = table
	} else {
		m.localTables[name] = table
	}

	return table, nil
}

// GetTempTable retrieves a temp table by name
func (m *TempTableManager) GetTempTable(name string) (*TempTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = strings.ToLower(name)

	// Check local tables first
	if table, ok := m.localTables[name]; ok {
		return table, true
	}

	// Then global tables
	if table, ok := m.globalTables[name]; ok {
		return table, true
	}

	return nil, false
}

// DropTempTable drops a temporary table
func (m *TempTableManager) DropTempTable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = strings.ToLower(name)

	if strings.HasPrefix(name, "##") {
		if _, exists := m.globalTables[name]; !exists {
			return fmt.Errorf("temp table %s does not exist", name)
		}
		delete(m.globalTables, name)
	} else {
		if _, exists := m.localTables[name]; !exists {
			return fmt.Errorf("temp table %s does not exist", name)
		}
		delete(m.localTables, name)
	}

	return nil
}

// TempTableExists checks if a temp table exists
func (m *TempTableManager) TempTableExists(name string) bool {
	_, exists := m.GetTempTable(name)
	return exists
}

// CreateTableVariable creates a table variable
func (m *TempTableManager) CreateTableVariable(name string, columns []TempTableColumn) (*TableVariable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = strings.ToLower(strings.TrimPrefix(name, "@"))

	if _, exists := m.tableVars[name]; exists {
		return nil, fmt.Errorf("table variable @%s already exists", name)
	}

	tv := &TableVariable{
		TempTable: &TempTable{
			Name:    "@" + name,
			Columns: columns,
			Rows:    make([][]Value, 0),
			Indexes: make(map[string]*TempTableIndex),
		},
	}

	m.tableVars[name] = tv
	return tv, nil
}

// GetTableVariable retrieves a table variable
func (m *TempTableManager) GetTableVariable(name string) (*TableVariable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = strings.ToLower(strings.TrimPrefix(name, "@"))
	tv, ok := m.tableVars[name]
	return tv, ok
}

// ClearSession clears all session-scoped temp tables and table variables
func (m *TempTableManager) ClearSession() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.localTables = make(map[string]*TempTable)
	m.tableVars = make(map[string]*TableVariable)
}

// TempTable methods

// GetColumnIndex returns the index of a column by name
func (t *TempTable) GetColumnIndex(name string) int {
	name = strings.ToLower(name)
	for i, col := range t.Columns {
		if strings.ToLower(col.Name) == name {
			return i
		}
	}
	return -1
}

// GetColumn returns column info by name
func (t *TempTable) GetColumn(name string) (*TempTableColumn, bool) {
	idx := t.GetColumnIndex(name)
	if idx < 0 {
		return nil, false
	}
	return &t.Columns[idx], true
}

// Insert inserts a row into the temp table
func (t *TempTable) Insert(values map[string]Value) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	row := make([]Value, len(t.Columns))
	var identityValue int64

	for i, col := range t.Columns {
		if col.Identity {
			// Generate identity value
			if len(t.Rows) == 0 {
				identityValue = col.IdentitySeed
			} else {
				// Find max identity value
				maxVal := col.IdentitySeed - col.IdentityIncr
				for _, r := range t.Rows {
					if r[i].AsInt() > maxVal {
						maxVal = r[i].AsInt()
					}
				}
				identityValue = maxVal + col.IdentityIncr
			}
			row[i] = NewBigInt(identityValue)
		} else if val, ok := values[strings.ToLower(col.Name)]; ok {
			row[i] = val
		} else if !col.DefaultValue.IsNull || col.Nullable {
			row[i] = col.DefaultValue
		} else {
			return 0, fmt.Errorf("column %s requires a value", col.Name)
		}
	}

	t.Rows = append(t.Rows, row)
	return identityValue, nil
}

// InsertRow inserts a row with values in column order
func (t *TempTable) InsertRow(values []Value) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(values) != len(t.Columns) {
		return 0, fmt.Errorf("expected %d values, got %d", len(t.Columns), len(values))
	}

	var identityValue int64
	row := make([]Value, len(t.Columns))

	for i, col := range t.Columns {
		if col.Identity {
			// Generate identity value
			if len(t.Rows) == 0 {
				identityValue = col.IdentitySeed
			} else {
				maxVal := col.IdentitySeed - col.IdentityIncr
				for _, r := range t.Rows {
					if r[i].AsInt() > maxVal {
						maxVal = r[i].AsInt()
					}
				}
				identityValue = maxVal + col.IdentityIncr
			}
			row[i] = NewBigInt(identityValue)
		} else {
			row[i] = values[i]
		}
	}

	t.Rows = append(t.Rows, row)
	return identityValue, nil
}

// Select returns rows matching the predicate
func (t *TempTable) Select(predicate func(row []Value) bool) [][]Value {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var results [][]Value
	for _, row := range t.Rows {
		if predicate == nil || predicate(row) {
			// Clone row
			clone := make([]Value, len(row))
			copy(clone, row)
			results = append(results, clone)
		}
	}
	return results
}

// SelectAll returns all rows
func (t *TempTable) SelectAll() [][]Value {
	return t.Select(nil)
}

// SelectColumns returns specified columns
func (t *TempTable) SelectColumns(columnNames []string, predicate func(row []Value) bool) ([]string, [][]Value) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Get column indices
	indices := make([]int, len(columnNames))
	for i, name := range columnNames {
		indices[i] = t.GetColumnIndex(name)
		if indices[i] < 0 {
			return nil, nil
		}
	}

	var results [][]Value
	for _, row := range t.Rows {
		if predicate == nil || predicate(row) {
			result := make([]Value, len(indices))
			for i, idx := range indices {
				result[i] = row[idx]
			}
			results = append(results, result)
		}
	}

	return columnNames, results
}

// Update updates rows matching the predicate
func (t *TempTable) Update(updates map[string]Value, predicate func(row []Value) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get column indices for updates
	updateIndices := make(map[int]Value)
	for name, val := range updates {
		idx := t.GetColumnIndex(name)
		if idx >= 0 {
			updateIndices[idx] = val
		}
	}

	count := 0
	for i, row := range t.Rows {
		if predicate == nil || predicate(row) {
			for idx, val := range updateIndices {
				t.Rows[i][idx] = val
			}
			count++
		}
	}
	return count
}

// Delete removes rows matching the predicate
func (t *TempTable) Delete(predicate func(row []Value) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if predicate == nil {
		count := len(t.Rows)
		t.Rows = t.Rows[:0]
		return count
	}

	count := 0
	newRows := make([][]Value, 0, len(t.Rows))
	for _, row := range t.Rows {
		if !predicate(row) {
			newRows = append(newRows, row)
		} else {
			count++
		}
	}
	t.Rows = newRows
	return count
}

// Truncate removes all rows
func (t *TempTable) Truncate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows = t.Rows[:0]
}

// RowCount returns the number of rows
func (t *TempTable) RowCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Rows)
}

// ToResultSet converts the temp table to a ResultSet
func (t *TempTable) ToResultSet() ResultSet {
	t.mu.RLock()
	defer t.mu.RUnlock()

	columns := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = col.Name
	}

	rows := make([][]Value, len(t.Rows))
	for i, row := range t.Rows {
		rows[i] = make([]Value, len(row))
		copy(rows[i], row)
	}

	return ResultSet{
		Columns: columns,
		Rows:    rows,
	}
}

// OrderBy sorts the temp table in place
func (t *TempTable) OrderBy(columnName string, ascending bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx := t.GetColumnIndex(columnName)
	if idx < 0 {
		return fmt.Errorf("column %s not found", columnName)
	}

	sort.Slice(t.Rows, func(i, j int) bool {
		cmp := t.Rows[i][idx].Compare(t.Rows[j][idx])
		if ascending {
			return cmp < 0
		}
		return cmp > 0
	})

	return nil
}

// CreateIndex creates an index on the temp table
func (t *TempTable) CreateIndex(name string, columns []string, unique bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Verify columns exist
	for _, col := range columns {
		if t.GetColumnIndex(col) < 0 {
			return fmt.Errorf("column %s not found", col)
		}
	}

	t.Indexes[name] = &TempTableIndex{
		Name:    name,
		Columns: columns,
		Unique:  unique,
	}

	return nil
}

// ParseColumnDefinitions parses column definitions from CREATE TABLE
func ParseColumnDefinitions(defs []ColumnDef) []TempTableColumn {
	columns := make([]TempTableColumn, len(defs))
	for i, def := range defs {
		dt, prec, scale, maxLen := ParseDataType(def.TypeName)
		columns[i] = TempTableColumn{
			Name:         def.Name,
			Type:         dt,
			Precision:    prec,
			Scale:        scale,
			MaxLen:       maxLen,
			Nullable:     def.Nullable,
			Identity:     def.Identity,
			IdentitySeed: def.IdentitySeed,
			IdentityIncr: def.IdentityIncr,
		}
		if def.DefaultExpr != "" {
			// Simple default parsing - could be enhanced
			columns[i].DefaultValue = NewVarChar(def.DefaultExpr, -1)
		} else {
			columns[i].DefaultValue = Null(dt)
		}
	}
	return columns
}

// ColumnDef is a simple column definition for parsing
type ColumnDef struct {
	Name         string
	TypeName     string
	Nullable     bool
	Identity     bool
	IdentitySeed int64
	IdentityIncr int64
	DefaultExpr  string
	PrimaryKey   bool
}
