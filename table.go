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
	Types []Type
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

type AnonymousRecord map[string]value

// IntoRecord converts the anonymous record into a table record.
// Values that do not match the table definition are ignored.
func (ar AnonymousRecord) IntoTableRecord(tdef *tableDef) *tableRecord {
	r := newTableRecord(tdef)

	for _, col := range tdef.Cols {
		v, ok := ar[col]
		if !ok {
			continue
		}
		r.SetVal(col, v)
	}
	return r
}

type tableRecord struct {
	Vals  []value
	tdef  *tableDef
	valid bool
}

// newTableRecord create a new record. If tdef is nil, the record is empty
// If tdef is not nil, the record is initialized with the columns and values from the table definition
func newTableRecord(tdef *tableDef) *tableRecord {
	assert(tdef != nil, "table definition is nil")
	r := &tableRecord{
		Vals: make([]value, len(tdef.Cols)),
		tdef: tdef,
	}
	for i, typ := range tdef.Types {
		r.Vals[i] = newNullValue(typ)
	}
	return r
}

func (r tableRecord) serializePK(w io.Writer) error {
	if err := r.ValidatePK(); err != nil {
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

func (r *tableRecord) deserializePK(reader io.Reader) error {
	var buf [4]byte
	if _, err := reader.Read(buf[:]); err != nil {
		return err
	}
	vals := r.Vals[:r.tdef.Pkeys]
	if err := deserializeValues(reader, vals); err != nil {
		return err
	}
	return nil
}

func (r tableRecord) serializeValues(w io.Writer) error {
	if err := r.validate(); err != nil {
		return err
	}
	return serializeValues(w, r.Vals[r.tdef.Pkeys:])
}

func (r *tableRecord) deserializeValues(reader io.Reader) error {
	vals := r.Vals[r.tdef.Pkeys:]
	if err := deserializeValues(reader, vals); err != nil {
		return err
	}
	if len(vals) != len(r.tdef.Cols)-r.tdef.Pkeys {
		r.valid = false
		return fmt.Errorf("expected %d values, received %d", len(r.tdef.Cols)-r.tdef.Pkeys, len(vals))
	}
	return nil
}

// ValidatePK checks if primary key columns are not null
// It fails if the record is not valid.
func (r *tableRecord) ValidatePK() error {
	if err := r.validate(); err != nil {
		return err
	}
	for i := 0; i < r.tdef.Pkeys; i++ {
		if r.Vals[i].isNull() {
			return fmt.Errorf("primary key column %d is null", i)
		}
	}
	return nil

}

// valid check if the record is valid for the table definition.
// It checks the following:
//   - the number of columns and values match the table definition
//   - the column names match the table definition (in order)
//   - the value types match the table definition (in order)
//   - the primary key columns are not null
func (r tableRecord) validate() error {
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
			return fmt.Errorf("expected %s for %s got %d", r.tdef.Types[i], r.tdef.Cols[i], r.Vals[i].Type)
		}
	}

	r.valid = true
	return nil
}

func (r tableRecord) isValid() bool {
	return r.validate() == nil
}

func (r *tableRecord) SetVal(col string, val value) *tableRecord {
	if !r.isValid() {
		return r
	}

	idx := slices.Index(r.tdef.Cols, col)
	if idx == -1 {
		return r
	}
	if val.Type != r.tdef.Types[idx] {
		return r
	}
	r.Vals[idx] = val
	return r
}

func (rec *tableRecord) SetBlob(key string, val []byte) *tableRecord {
	return rec.SetVal(key, newBlob(val))
}

func (rec *tableRecord) SetInt64(key string, val int64) *tableRecord {
	return rec.SetVal(key, newInt64(val))
}

func (rec *tableRecord) Get(col string) *value {
	idx := slices.Index(rec.tdef.Cols, col)
	if idx == -1 {
		return nil
	}
	return &rec.Vals[idx]
}

type Type uint32

func (t Type) String() string {
	switch t {
	case typeBlob:
		return "blob"
	case typeInt64:
		return "int"
	default:
		return "unknown type"
	}
}

const (
	errorType Type = 0
	typeBlob  Type = 1
	typeInt64 Type = 2
)

type value struct {
	Type Type
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

func newNullValue(typ Type) value {
	return value{Type: typ, Set: false}
}

// serializeValues serializes each value in the slice to the writer.
// The following encoding is used for:
// - int64: Fixed Bias Encoding. Null values are encoed as 8 bytes of \x00.
// - blob: null-terminated byte array. Null values are encoed as \x00.
func serializeValues(w io.Writer, values []value) error {
	for _, v := range values {
		switch v.Type {
		case typeInt64:
			var u uint64
			if !v.isNull() {
				u = uint64(v.I64) + 1<<63
			}
			if err := binary.Write(w, binary.LittleEndian, u); err != nil {
				return fmt.Errorf("encoding %v: %w", v, err)
			}
		case typeBlob:
			if !v.isNull() {
				if _, err := w.Write(escapeNull(v.Blob)); err != nil {
					return fmt.Errorf("encoding %v: %w", v, err)
				}
			}
			if _, err := w.Write([]byte{0}); err != nil {
				return fmt.Errorf("encoding %v: %w", v, err)
			}
		default:
			panic("unknown type")
		}
	}

	return nil
}

func deserializeValues(r io.Reader, values []value) error {
	for i, value := range values {
		var isNull bool
		switch value.Type {
		case typeInt64:
			v := uint64(0)
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return fmt.Errorf("deserializing %dth value: %w", i, err)
			}
			isNull = v == 0
			if !isNull {
				values[i].I64 = int64(v - 1<<63)
			}
		case typeBlob:
			blob, err := readNullTerminatedBlob(r)
			if err != nil {
				return fmt.Errorf("deserializing %dth value: %w", i, err)
			}
			if len(blob) == 0 {
				isNull = true
			}
			if !isNull {
				values[i].Blob = unescapeNull(blob)
			}
		default:
			panic("unknown type")
		}
		values[i].Set = !isNull
	}
	return nil
}

func readNullTerminatedBlob(r io.Reader) ([]byte, error) {
	b := make([]byte, 1)
	var blob []byte
	for {
		if _, err := r.Read(b); err != nil {
			return nil, err
		}
		if b[0] == 0 {
			break
		}
		blob = append(blob, b[0])
	}
	return blob, nil
}

// escapeNull escapes \x00 with \x01\x01 and \x01 with \x01\x02
// The returned slice is only copied on write.
func escapeNull(b []byte) []byte {
	zeros := bytes.Count(b, []byte{0})
	ones := bytes.Count(b, []byte{1})
	if zeros == 0 && ones == 0 {
		return b
	}

	escaped := make([]byte, 0, len(b)+zeros+ones)
	for _, c := range b {
		if c <= 1 {
			escaped = append(escaped, 0x01, c+1)
		} else {
			escaped = append(escaped, c)
		}
	}
	return escaped
}

// unescapeNull unescapes \x01\x01 to \x00 and \x01\x02 to \x01 in place.tabl
func unescapeNull(escaped []byte) []byte {
	escapedIdx := 0
	unescapedIdx := 0
	for escapedIdx < len(escaped) {
		if escaped[escapedIdx] == 0x01 {
			if escaped[escapedIdx+1] == 0x01 {
				escaped[unescapedIdx] = 0
			} else if escaped[escapedIdx+1] == 0x02 {
				escaped[unescapedIdx] = 1
			} else {
				panic(fmt.Sprintf("invalid escape sequence: %x", escaped[escapedIdx:escapedIdx+2]))
			}
			escapedIdx += 2
			unescapedIdx++
		} else {
			escaped[unescapedIdx] = escaped[escapedIdx]
			escapedIdx++
			unescapedIdx++
		}
	}
	return escaped[:unescapedIdx]
}
