package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

type tableDef struct {
	Name  string
	Types []uint32
	Cols  []string
	// the first Pkeys columns are the primary key
	Pkeys int
	// auto-assigned B-tree key Prefix for the table
	Prefix uint32
}

func (tdef tableDef) Serialize(b *bytes.Buffer) error {
	return json.NewEncoder(b).Encode(tdef)
}

func (tdef tableDef) Validate() error {
	if tdef.Name == "" {
		return fmt.Errorf("table name is empty")
	}
	if len(tdef.Cols) != len(tdef.Types) {
		return fmt.Errorf("column count mismatch")
	}
	if tdef.Pkeys < 1 || tdef.Pkeys > len(tdef.Cols) {
		return fmt.Errorf("invalid primary key")
	}
	return nil
}

type record struct {
	Vals  []value
	tdef  *tableDef
	valid bool
}

// newRecord create a new record. If tdef is nil, the record is empty
// If tdef is not nil, the record is initialized with the columns and values from the table definition
func newRecord(tdef *tableDef) *record {
	if tdef == nil {
		return &record{}
	}
	r := &record{
		Vals: make([]value, len(tdef.Cols)),
		tdef: tdef,
	}
	return r
}

func encodeKey(out *bytes.Buffer, prefix uint32, values []value) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], prefix)
	if _, err := out.Write(buf[:]); err != nil {
		return err
	}
	if err := serializeValues(out, values); err != nil {
		return nil
	}
	return nil
}
func (r record) serializePK(w io.Writer) error {
	if err := r.validate(); err != nil {
		return err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], r.tdef.Prefix)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if err := serializeValues(w, r.Vals[:r.tdef.Pkeys]); err != nil {
		return nil
	}
	return nil
}

func (r record) serializeValues(w io.Writer) error {
	if err := r.validate(); err != nil {
		return err
	}
	return serializeValues(w, r.Vals[r.tdef.Pkeys:])
}

func (r *record) deserializeValues(reader io.Reader) error {
	vals := r.Vals[r.tdef.Pkeys:]
	if err := deserializeValues(reader, &vals); err != nil {
		return err
	}
	if len(vals) != len(r.tdef.Cols)-r.tdef.Pkeys {
		r.valid = false
		return fmt.Errorf("expected %d values, received %d", len(r.tdef.Cols)-r.tdef.Pkeys, len(vals))
	}
	return nil
}

func (r *record) setTdef(tdef *tableDef) {
	r.tdef = tdef
	r.valid = false
}

// valid check if the record is valid for the table definition.
// It checks the following:
// - the number of columns and values match the table definition
// - the column names match the table definition (in order)
// - the value types match the table definition (in order)
// - the primary key columns are present
func (r record) validate() error {
	if r.valid {
		return nil
	}
	if r.tdef == nil {
		return fmt.Errorf("table definition is nil")
	}
	if len(r.Vals) != len(r.tdef.Cols) {
		return fmt.Errorf("value count mismatch")
	}

	for i := 0; i < len(r.tdef.Cols); i++ {
		if !r.Vals[i].isNull() && r.Vals[i].Type != r.tdef.Types[i] {
			return fmt.Errorf("expected type %d at %d, received %d", r.tdef.Types[i], i, r.Vals[i].Type)
		}
	}
	r.valid = true
	return nil
}

//	func (r *record) reorder() bool {
//		if r.tdef == nil {
//			return false
//		}
//		if len(r.cols) != len(r.tdef.cols) {
//			return false
//		}
//		if len(r.vals) != len(r.tdef.cols) {
//			return false
//		}
//
//		// Create new arrays to hold reordered data
//		newCols := make([]string, len(r.tdef.cols))
//		newVals := make([]value, len(r.tdef.cols))
//
//		// Maps to check if all columns from table definition exist in the record
//		colExists := make(map[string]bool)
//		for _, col := range r.cols {
//			colExists[col] = true
//		}
//
//		// Check if all columns from table definition exist in the record
//		for _, col := range r.tdef.cols {
//			if !colExists[col] {
//				return false
//			}
//		}
//
//		// Reorder the columns and values according to table definition
//		for i, col := range r.tdef.cols {
//			idx := slices.Index(r.cols, col)
//			if idx == -1 {
//				return false // This should never happen because we checked column existence above
//			}
//			newCols[i] = col
//			newVals[i] = r.vals[idx]
//		}
//
//		// Replace the original columns and values with the reordered ones
//		r.cols = newCols
//		r.vals = newVals
//
//		return true
//	}
func (r *record) AddVal(col string, val value) *record {
	if r.validate() != nil {
		return r
	}
	idx := slices.Index(r.tdef.Cols, col)
	if idx == -1 {
		return r
	}
	val.Set = true
	r.Vals[idx] = val
	r.valid = false
	return r
}

func (rec *record) AddBlob(key string, val []byte) *record {
	return rec.AddVal(key, newBlob(val))
}

func (rec *record) AddInt64(key string, val int64) *record {
	return rec.AddVal(key, newInt64(val))
}

func (rec *record) Get(col string) *value {
	idx := slices.Index(rec.tdef.Cols, col)
	if idx == -1 {
		return nil
	}
	return &rec.Vals[idx]
}

const (
	errorType = 0
	typeBlob  = 1
	typeInt64 = 2
)

type value struct {
	Type uint32
	I64  int64
	Blob []byte
	Set  bool
}

func (v value) isNull() bool {
	return !v.Set
}

func newInt64(i int64) value {
	return value{Type: typeInt64, I64: i, Set: true}
}

func newBlob(b []byte) value {
	return value{Type: typeBlob, Blob: b, Set: true}
}

func newNullValue(typ uint32) value {
	return value{Type: typ, Set: false}
}

func serializeValues(w io.Writer, values []value) error {
	return json.NewEncoder(w).Encode(values)
}

func deserializeValues(r io.Reader, values *[]value) error {
	return json.NewDecoder(r).Decode(values)
}
