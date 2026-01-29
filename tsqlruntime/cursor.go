package tsqlruntime

import (
	"fmt"
	"strings"
	"sync"
)

// CursorType represents the type of cursor
type CursorType int

const (
	CursorForwardOnly CursorType = iota
	CursorStatic
	CursorKeyset
	CursorDynamic
	CursorFastForward
)

// CursorScrollType represents scroll capability
type CursorScrollType int

const (
	CursorScrollNone CursorScrollType = iota
	CursorScrollForward
	CursorScrollBackward
	CursorScrollAbsolute
	CursorScrollRelative
	CursorScrollFirst
	CursorScrollLast
)

// CursorLockType represents the lock type
type CursorLockType int

const (
	CursorReadOnly CursorLockType = iota
	CursorScrollLocks
	CursorOptimistic
)

// Cursor represents a T-SQL cursor
type Cursor struct {
	Name        string
	Query       string
	Columns     []string
	Rows        [][]Value
	CurrentRow  int // -1 = before first, len(rows) = after last
	IsOpen      bool
	IsAllocated bool
	CursorType  CursorType
	ScrollType  CursorScrollType
	LockType    CursorLockType
	IsGlobal    bool
	mu          sync.RWMutex
}

// CursorManager manages cursors for a session
type CursorManager struct {
	localCursors  map[string]*Cursor
	globalCursors map[string]*Cursor
	mu            sync.RWMutex
}

// NewCursorManager creates a new cursor manager
func NewCursorManager() *CursorManager {
	return &CursorManager{
		localCursors:  make(map[string]*Cursor),
		globalCursors: make(map[string]*Cursor),
	}
}

// DeclareCursor declares a new cursor
func (m *CursorManager) DeclareCursor(name string, query string, isGlobal bool, cursorType CursorType, scrollType CursorScrollType, lockType CursorLockType) (*Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = normalizeCursorName(name)

	// Check if already exists
	if isGlobal {
		if _, exists := m.globalCursors[name]; exists {
			return nil, fmt.Errorf("cursor %s already exists", name)
		}
	} else {
		if _, exists := m.localCursors[name]; exists {
			return nil, fmt.Errorf("cursor %s already exists", name)
		}
	}

	cursor := &Cursor{
		Name:        name,
		Query:       query,
		CurrentRow:  -1,
		IsOpen:      false,
		IsAllocated: true,
		CursorType:  cursorType,
		ScrollType:  scrollType,
		LockType:    lockType,
		IsGlobal:    isGlobal,
	}

	if isGlobal {
		m.globalCursors[name] = cursor
	} else {
		m.localCursors[name] = cursor
	}

	return cursor, nil
}

// GetCursor retrieves a cursor by name
func (m *CursorManager) GetCursor(name string) (*Cursor, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = normalizeCursorName(name)

	// Check local first
	if cursor, ok := m.localCursors[name]; ok {
		return cursor, true
	}

	// Then global
	if cursor, ok := m.globalCursors[name]; ok {
		return cursor, true
	}

	return nil, false
}

// DeallocateCursor deallocates a cursor
func (m *CursorManager) DeallocateCursor(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = normalizeCursorName(name)

	// Try local first
	if cursor, ok := m.localCursors[name]; ok {
		if cursor.IsOpen {
			return fmt.Errorf("cursor %s is still open", name)
		}
		delete(m.localCursors, name)
		return nil
	}

	// Then global
	if cursor, ok := m.globalCursors[name]; ok {
		if cursor.IsOpen {
			return fmt.Errorf("cursor %s is still open", name)
		}
		delete(m.globalCursors, name)
		return nil
	}

	return fmt.Errorf("cursor %s does not exist", name)
}

// ClearSession clears all local cursors
func (m *CursorManager) ClearSession() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close and deallocate all local cursors
	for _, cursor := range m.localCursors {
		cursor.IsOpen = false
		cursor.IsAllocated = false
	}
	m.localCursors = make(map[string]*Cursor)
}

// Cursor methods

// Open opens the cursor with result data
func (c *Cursor) Open(columns []string, rows [][]Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsAllocated {
		return fmt.Errorf("cursor %s is not allocated", c.Name)
	}

	if c.IsOpen {
		return fmt.Errorf("cursor %s is already open", c.Name)
	}

	c.Columns = columns
	c.Rows = rows
	c.CurrentRow = -1 // Before first row
	c.IsOpen = true

	return nil
}

// Close closes the cursor
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen {
		return fmt.Errorf("cursor %s is not open", c.Name)
	}

	c.IsOpen = false
	c.Rows = nil
	c.Columns = nil
	c.CurrentRow = -1

	return nil
}

// FetchNext fetches the next row
// Returns the row values and fetch status (0 = success, -1 = no more rows, -2 = row missing)
func (c *Cursor) FetchNext() ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen {
		return nil, -1
	}

	c.CurrentRow++

	if c.CurrentRow >= len(c.Rows) {
		c.CurrentRow = len(c.Rows) // Position after last
		return nil, -1             // No more rows
	}

	return c.cloneRow(c.CurrentRow), 0
}

// FetchPrior fetches the previous row (for scrollable cursors)
func (c *Cursor) FetchPrior() ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen || c.ScrollType == CursorScrollNone {
		return nil, -1
	}

	c.CurrentRow--

	if c.CurrentRow < 0 {
		c.CurrentRow = -1 // Position before first
		return nil, -1
	}

	return c.cloneRow(c.CurrentRow), 0
}

// FetchFirst fetches the first row (for scrollable cursors)
func (c *Cursor) FetchFirst() ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen || c.ScrollType == CursorScrollNone {
		return nil, -1
	}

	if len(c.Rows) == 0 {
		return nil, -1
	}

	c.CurrentRow = 0
	return c.cloneRow(0), 0
}

// FetchLast fetches the last row (for scrollable cursors)
func (c *Cursor) FetchLast() ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen || c.ScrollType == CursorScrollNone {
		return nil, -1
	}

	if len(c.Rows) == 0 {
		return nil, -1
	}

	c.CurrentRow = len(c.Rows) - 1
	return c.cloneRow(c.CurrentRow), 0
}

// FetchAbsolute fetches the row at the absolute position (for scrollable cursors)
func (c *Cursor) FetchAbsolute(position int) ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen || c.ScrollType == CursorScrollNone {
		return nil, -1
	}

	// SQL Server uses 1-based positioning for FETCH ABSOLUTE
	// Negative values count from the end
	var targetRow int
	if position > 0 {
		targetRow = position - 1 // Convert to 0-based
	} else if position < 0 {
		targetRow = len(c.Rows) + position // -1 = last row
	} else {
		// Position 0 is before the first row
		c.CurrentRow = -1
		return nil, -1
	}

	if targetRow < 0 || targetRow >= len(c.Rows) {
		return nil, -1
	}

	c.CurrentRow = targetRow
	return c.cloneRow(c.CurrentRow), 0
}

// FetchRelative fetches relative to current position (for scrollable cursors)
func (c *Cursor) FetchRelative(offset int) ([]Value, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.IsOpen || c.ScrollType == CursorScrollNone {
		return nil, -1
	}

	targetRow := c.CurrentRow + offset

	if targetRow < 0 {
		c.CurrentRow = -1
		return nil, -1
	}

	if targetRow >= len(c.Rows) {
		c.CurrentRow = len(c.Rows)
		return nil, -1
	}

	c.CurrentRow = targetRow
	return c.cloneRow(c.CurrentRow), 0
}

// GetCurrentRow returns the current row values
func (c *Cursor) GetCurrentRow() ([]Value, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.IsOpen || c.CurrentRow < 0 || c.CurrentRow >= len(c.Rows) {
		return nil, false
	}

	return c.cloneRow(c.CurrentRow), true
}

// RowCount returns the number of rows in the cursor
func (c *Cursor) RowCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Rows == nil {
		return 0
	}
	return len(c.Rows)
}

// GetColumnIndex returns the index of a column by name
func (c *Cursor) GetColumnIndex(name string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i, col := range c.Columns {
		if col == name {
			return i
		}
	}
	return -1
}

// cloneRow creates a copy of a row (must be called with lock held)
func (c *Cursor) cloneRow(idx int) []Value {
	if idx < 0 || idx >= len(c.Rows) {
		return nil
	}
	row := make([]Value, len(c.Rows[idx]))
	copy(row, c.Rows[idx])
	return row
}

// normalizeCursorName normalizes a cursor name
func normalizeCursorName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// CursorStatus returns information about a cursor (for CURSOR_STATUS function)
func (c *Cursor) Status() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.IsAllocated {
		return -3 // Cursor does not exist
	}
	if !c.IsOpen {
		return -1 // Cursor is closed
	}
	if len(c.Rows) == 0 {
		return 0 // Cursor is open but empty
	}
	return 1 // Cursor is open with rows
}
