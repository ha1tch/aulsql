package tds

import (
	"context"
	"sync"
	"time"
)

// CursorFetchType identifies the fetch direction.
type CursorFetchType int32

const (
	FetchFirst    CursorFetchType = 1
	FetchNext     CursorFetchType = 2
	FetchPrev     CursorFetchType = 4
	FetchLast     CursorFetchType = 8
	FetchAbsolute CursorFetchType = 16
	FetchRelative CursorFetchType = 32
)

func (f CursorFetchType) String() string {
	switch f {
	case FetchFirst:
		return "FIRST"
	case FetchNext:
		return "NEXT"
	case FetchPrev:
		return "PRIOR"
	case FetchLast:
		return "LAST"
	case FetchAbsolute:
		return "ABSOLUTE"
	case FetchRelative:
		return "RELATIVE"
	default:
		return "UNKNOWN"
	}
}

// CursorScrollOpt defines cursor scrollability options.
type CursorScrollOpt int32

const (
	ScrollOptForwardOnly CursorScrollOpt = 1
	ScrollOptKeyset      CursorScrollOpt = 2
	ScrollOptDynamic     CursorScrollOpt = 4
	ScrollOptStatic      CursorScrollOpt = 8
	ScrollOptFastForward CursorScrollOpt = 16
)

func (s CursorScrollOpt) String() string {
	switch s {
	case ScrollOptForwardOnly:
		return "FORWARD_ONLY"
	case ScrollOptKeyset:
		return "KEYSET"
	case ScrollOptDynamic:
		return "DYNAMIC"
	case ScrollOptStatic:
		return "STATIC"
	case ScrollOptFastForward:
		return "FAST_FORWARD"
	default:
		return "UNKNOWN"
	}
}

// CursorConcurrencyOpt defines cursor concurrency options.
type CursorConcurrencyOpt int32

const (
	CCOptReadOnly          CursorConcurrencyOpt = 1
	CCOptScrollLocks       CursorConcurrencyOpt = 2
	CCOptOptimistic        CursorConcurrencyOpt = 4
	CCOptOptimisticValues  CursorConcurrencyOpt = 8
)

func (c CursorConcurrencyOpt) String() string {
	switch c {
	case CCOptReadOnly:
		return "READ_ONLY"
	case CCOptScrollLocks:
		return "SCROLL_LOCKS"
	case CCOptOptimistic:
		return "OPTIMISTIC"
	case CCOptOptimisticValues:
		return "OPTIMISTIC_VALUES"
	default:
		return "UNKNOWN"
	}
}

// Cursor represents an open server-side cursor.
type Cursor struct {
	Handle    int32
	SQL       string
	Columns   []Column
	ScrollOpt CursorScrollOpt
	CCOpt     CursorConcurrencyOpt
	Position  int64     // Current row position (1-based, 0 = before first)
	RowCount  int32     // Total rows (-1 if unknown)
	CreatedAt time.Time
	LastFetch time.Time
}

// CursorManager handles server-side cursor operations.
type CursorManager interface {
	// Open creates a new cursor and returns its handle and row count.
	Open(ctx context.Context, stmt string, scrollOpt CursorScrollOpt, ccOpt CursorConcurrencyOpt) (handle int32, rowCount int32, columns []Column, err error)

	// Fetch retrieves rows from a cursor.
	Fetch(ctx context.Context, handle int32, fetchType CursorFetchType, rowNum int64, nRows int32) (*CursorFetchResult, error)

	// Close closes and releases a cursor.
	Close(ctx context.Context, handle int32) error

	// SetOption sets cursor options.
	SetOption(ctx context.Context, handle int32, option, value int32) error

	// GetCursor returns the cursor for a handle, if it exists.
	GetCursor(handle int32) (*Cursor, bool)
}

// CursorFetchResult holds the result of a cursor fetch operation.
type CursorFetchResult struct {
	Rows         [][]interface{}
	RowCount     int32 // Number of rows returned
	FetchStatus  int32 // 0 = success, -1 = failed, -2 = row missing
}

// CursorCache is an in-memory cache of cursors.
type CursorCache struct {
	mu         sync.RWMutex
	cursors    map[int32]*Cursor
	handlePool *HandlePool
	executor   CursorExecutor
}

// CursorExecutor is called to actually execute cursor operations.
// This is implemented by the application layer (aul).
type CursorExecutor interface {
	// OpenCursor opens a cursor for the given SQL and returns column metadata.
	OpenCursor(ctx context.Context, cursor *Cursor) (rowCount int32, columns []Column, err error)

	// FetchCursor fetches rows from an open cursor.
	FetchCursor(ctx context.Context, cursor *Cursor, fetchType CursorFetchType, rowNum int64, nRows int32) (*CursorFetchResult, error)

	// CloseCursor releases resources for a cursor.
	CloseCursor(ctx context.Context, cursor *Cursor) error
}

// NewCursorCache creates a new cursor cache with the given executor.
func NewCursorCache(executor CursorExecutor) *CursorCache {
	return &CursorCache{
		cursors:    make(map[int32]*Cursor),
		handlePool: NewHandlePool(),
		executor:   executor,
	}
}

// Open implements CursorManager.
func (c *CursorCache) Open(ctx context.Context, stmt string, scrollOpt CursorScrollOpt, ccOpt CursorConcurrencyOpt) (int32, int32, []Column, error) {
	handle := c.handlePool.Acquire()

	cursor := &Cursor{
		Handle:    handle,
		SQL:       stmt,
		ScrollOpt: scrollOpt,
		CCOpt:     ccOpt,
		Position:  0, // Before first row
		RowCount:  -1,
		CreatedAt: time.Now(),
	}

	// Open via executor if available
	var columns []Column
	var rowCount int32 = -1
	if c.executor != nil {
		var err error
		rowCount, columns, err = c.executor.OpenCursor(ctx, cursor)
		if err != nil {
			c.handlePool.Release(handle)
			return 0, 0, nil, err
		}
		cursor.Columns = columns
		cursor.RowCount = rowCount
	}

	c.mu.Lock()
	c.cursors[handle] = cursor
	c.mu.Unlock()

	return handle, rowCount, columns, nil
}

// Fetch implements CursorManager.
func (c *CursorCache) Fetch(ctx context.Context, handle int32, fetchType CursorFetchType, rowNum int64, nRows int32) (*CursorFetchResult, error) {
	c.mu.RLock()
	cursor, ok := c.cursors[handle]
	c.mu.RUnlock()

	if !ok {
		return nil, &CursorError{Handle: handle, Message: "invalid cursor handle"}
	}

	// Validate fetch type against scroll options
	if cursor.ScrollOpt == ScrollOptForwardOnly && fetchType != FetchNext {
		return nil, &CursorError{Handle: handle, Message: "cursor is forward-only"}
	}

	// Update last fetch time
	c.mu.Lock()
	cursor.LastFetch = time.Now()
	c.mu.Unlock()

	// Execute fetch
	if c.executor == nil {
		return nil, ErrNotImplemented("cursor fetch")
	}

	return c.executor.FetchCursor(ctx, cursor, fetchType, rowNum, nRows)
}

// Close implements CursorManager.
func (c *CursorCache) Close(ctx context.Context, handle int32) error {
	c.mu.Lock()
	cursor, ok := c.cursors[handle]
	if !ok {
		c.mu.Unlock()
		return &CursorError{Handle: handle, Message: "invalid cursor handle"}
	}
	delete(c.cursors, handle)
	c.mu.Unlock()

	c.handlePool.Release(handle)

	// Close via executor if available
	if c.executor != nil {
		return c.executor.CloseCursor(ctx, cursor)
	}

	return nil
}

// SetOption implements CursorManager.
func (c *CursorCache) SetOption(ctx context.Context, handle int32, option, value int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cursor, ok := c.cursors[handle]
	if !ok {
		return &CursorError{Handle: handle, Message: "invalid cursor handle"}
	}

	// Handle common options
	// This is a placeholder - real implementation would handle specific options
	_ = cursor
	_ = option
	_ = value

	return nil
}

// GetCursor implements CursorManager.
func (c *CursorCache) GetCursor(handle int32) (*Cursor, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cursor, ok := c.cursors[handle]
	return cursor, ok
}

// CursorError indicates an error with a cursor.
type CursorError struct {
	Handle  int32
	Message string
}

func (e *CursorError) Error() string {
	return e.Message
}

// NullCursorManager is a stub that rejects all cursor operations.
type NullCursorManager struct{}

func (NullCursorManager) Open(ctx context.Context, stmt string, scrollOpt CursorScrollOpt, ccOpt CursorConcurrencyOpt) (int32, int32, []Column, error) {
	return 0, 0, nil, ErrNotImplemented("cursors")
}

func (NullCursorManager) Fetch(ctx context.Context, handle int32, fetchType CursorFetchType, rowNum int64, nRows int32) (*CursorFetchResult, error) {
	return nil, ErrNotImplemented("cursors")
}

func (NullCursorManager) Close(ctx context.Context, handle int32) error {
	return ErrNotImplemented("cursors")
}

func (NullCursorManager) SetOption(ctx context.Context, handle int32, option, value int32) error {
	return ErrNotImplemented("cursors")
}

func (NullCursorManager) GetCursor(handle int32) (*Cursor, bool) {
	return nil, false
}
