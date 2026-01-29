package tds

// NBCRow (Null Bitmap Compressed Row) encoding.
//
// NBCRow is an optimization for result sets with many nullable columns.
// Instead of encoding NULL as a type-specific marker in each column,
// a bitmap at the start of the row indicates which columns are NULL.
//
// Format:
//   TokenNBCRow (0xD2)
//   NullBitmap: ceil(numColumns/8) bytes
//     - Bit N = 1 means column N is NULL
//   ColumnData: Only non-NULL columns, in order

// BuildNullBitmap creates the null bitmap for a row.
// The bitmap has one bit per column; bit set = NULL.
func BuildNullBitmap(values []interface{}, numColumns int) []byte {
	bitmapLen := (numColumns + 7) / 8
	bitmap := make([]byte, bitmapLen)

	for i := 0; i < numColumns && i < len(values); i++ {
		if values[i] == nil {
			// Set bit i to indicate NULL
			byteIndex := i / 8
			bitIndex := uint(i % 8)
			bitmap[byteIndex] |= 1 << bitIndex
		}
	}

	return bitmap
}

// IsNullInBitmap checks if a column is marked as NULL in the bitmap.
func IsNullInBitmap(bitmap []byte, columnIndex int) bool {
	byteIndex := columnIndex / 8
	bitIndex := uint(columnIndex % 8)

	if byteIndex >= len(bitmap) {
		return false
	}

	return (bitmap[byteIndex] & (1 << bitIndex)) != 0
}

// CountNulls returns the number of NULL values in the bitmap.
func CountNulls(bitmap []byte, numColumns int) int {
	count := 0
	for i := 0; i < numColumns; i++ {
		if IsNullInBitmap(bitmap, i) {
			count++
		}
	}
	return count
}

// ShouldUseNBCRow determines if NBCRow encoding would be beneficial.
// Heuristics:
//   - TDS version must be >= 7.3 (SQL Server 2008+)
//   - More than 4 nullable columns
//   - At least 20% of values are NULL
func ShouldUseNBCRow(tdsVersion uint32, columns []Column, values []interface{}) bool {
	// NBCRow requires TDS 7.3+
	if tdsVersion < VerTDS73A {
		return false
	}

	// Count nullable columns
	nullableCount := 0
	for _, col := range columns {
		if col.Nullable {
			nullableCount++
		}
	}

	// Need at least 5 nullable columns to benefit
	if nullableCount < 5 {
		return false
	}

	// Count actual NULLs in this row
	nullCount := 0
	for _, v := range values {
		if v == nil {
			nullCount++
		}
	}

	// Use NBCRow if more than 20% of values are NULL
	threshold := len(values) / 5
	return nullCount >= threshold
}

// WriteNBCRow writes a row using null bitmap compression to the TokenWriter.
func (w *ResultSetWriter) WriteNBCRow(values []interface{}) error {
	numColumns := len(w.columns)
	if len(values) != numColumns {
		return &RowError{Message: "value count mismatch"}
	}

	// Build null bitmap
	bitmap := BuildNullBitmap(values, numColumns)

	// Write token and bitmap
	w.tw.buf.WriteByte(byte(TokenNBCRow))
	w.tw.buf.Write(bitmap)

	// Write only non-NULL values using the existing writeValue method
	for i, col := range w.columns {
		if values[i] == nil {
			continue // Skip NULLs - they're encoded in the bitmap
		}

		if err := w.writeValue(values[i], col); err != nil {
			return err
		}
	}

	return nil
}

// EnableNBCRow enables or disables NBCRow encoding for subsequent rows.
// When enabled and appropriate, WriteRow will automatically use NBCRow format.
func (w *ResultSetWriter) EnableNBCRow(enabled bool) {
	w.useNBCRow = enabled
}

// WriteRowAuto writes a row using either ROW or NBCROW format,
// automatically choosing based on the data.
func (w *ResultSetWriter) WriteRowAuto(values []interface{}, tdsVersion uint32) error {
	if w.useNBCRow && ShouldUseNBCRow(tdsVersion, w.columns, values) {
		return w.WriteNBCRow(values)
	}
	return w.WriteRow(values)
}

// RowError indicates an error writing a row.
type RowError struct {
	Message string
}

func (e *RowError) Error() string {
	return e.Message
}
