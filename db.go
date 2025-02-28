package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

const (
	errorType = 0
	typeBlob  = 1
	typeInt64 = 2

	tableInitPrefix = 3
)

type value struct {
	typ  uint32
	i64  int64
	blob []byte
}

type record struct {
	cols  []string
	vals  []value
	tdef  *tableDef
	valid bool
}

// newRecord create a new record. If tdef is nil, the record is empty
// If tdef is not nil, the record is initialized with the columns and values from the table definition
func newRecord(tdef *tableDef) *record {
	if tdef == nil {
		return &record{}
	}
	return newRecordOf(*tdef)
}

func encodeKey(out *bytes.Buffer, prefix uint32, values []value) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], prefix)
	if _, err := out.Write(buf[:]); err != nil {
		return err
	}
	if err := encodeValues(out, values); err != nil {
		return nil
	}
	return nil
}
func (r record) serializePK(w io.Writer) error {
	if !r.valid {
		return fmt.Errorf("record is not valid")
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], r.tdef.prefix)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if err := encodeValues(w, r.vals[:r.tdef.pkeys]); err != nil {
		return nil
	}
	return nil
}

func (r record) serializeNonPK(w io.Writer) error {
	if !r.valid {
		return fmt.Errorf("record is not valid")
	}
	return encodeValues(w, r.vals[r.tdef.pkeys:])
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
	if len(r.cols) != len(r.tdef.cols) {
		return fmt.Errorf("column count mismatch")
	}
	if len(r.vals) != len(r.tdef.cols) {
		return fmt.Errorf("value count mismatch")
	}
	for i := 0; i < len(r.tdef.cols); i++ {
		if r.cols[i] != r.tdef.cols[i] {
			return fmt.Errorf("expected column %s at index %d, got %s", r.tdef.cols[i], i, r.cols[i])
		}
		if r.vals[i].typ != r.tdef.types[i] {
			return fmt.Errorf("expected type %d at index %d, got %d", r.tdef.types[i], i, r.vals[i].typ)
		}
	}
	return nil
}

// newRecordOf creates a new record with the columns and values from the table definition
func newRecordOf(tdef tableDef) *record {
	r := &record{
		cols: make([]string, len(tdef.cols)),
		vals: make([]value, len(tdef.cols)),
	}
	copy(r.cols, tdef.cols)
	return r
}

func (r *record) reorder() bool {
	if r.tdef == nil {
		return false
	}
	if len(r.cols) != len(r.tdef.cols) {
		return false
	}
	if len(r.vals) != len(r.tdef.cols) {
		return false
	}

	// Create new arrays to hold reordered data
	newCols := make([]string, len(r.tdef.cols))
	newVals := make([]value, len(r.tdef.cols))

	// Maps to check if all columns from table definition exist in the record
	colExists := make(map[string]bool)
	for _, col := range r.cols {
		colExists[col] = true
	}

	// Check if all columns from table definition exist in the record
	for _, col := range r.tdef.cols {
		if !colExists[col] {
			return false
		}
	}

	// Reorder the columns and values according to table definition
	for i, col := range r.tdef.cols {
		idx := slices.Index(r.cols, col)
		if idx == -1 {
			return false // This should never happen because we checked column existence above
		}
		newCols[i] = col
		newVals[i] = r.vals[idx]
	}

	// Replace the original columns and values with the reordered ones
	r.cols = newCols
	r.vals = newVals

	return true
}

func (r *record) AddVal(col string, val value) *record {
	idx := slices.Index(r.cols, col)
	if idx != -1 {
		r.vals[idx] = val
	} else {
		r.cols = append(r.cols, col)
		r.vals = append(r.vals, val)
	}
	r.valid = false
	return r
}

func (rec *record) AddBlob(key string, val []byte) *record {
	return rec.AddVal(key, value{typ: typeBlob, blob: val})
}

func (rec *record) AddInt64(key string, val int64) *record {
	return rec.AddVal(key, value{typ: typeInt64, i64: val})
}

func (rec *record) Get(key string) *value {
	idx := slices.Index(rec.cols, key)
	if idx == -1 {
		return nil
	}
	return &rec.vals[idx]
}

type tableDef struct {
	name  string
	types []uint32
	cols  []string
	// the first pkeys columns are the primary key
	pkeys int
	// auto-assigned B-tree key prefix for the table
	prefix uint32
}

func (tdef tableDef) Serialize(b *bytes.Buffer) error {
	return json.NewEncoder(b).Encode(tdef)
}

func (tdef tableDef) Validate() error {
	if tdef.name == "" {
		return fmt.Errorf("table name is empty")
	}
	if len(tdef.cols) != len(tdef.types) {
		return fmt.Errorf("column count mismatch")
	}
	if tdef.pkeys < 1 || tdef.pkeys > len(tdef.cols) {
		return fmt.Errorf("invalid primary key")
	}
	return nil
}

var metaDataTable = tableDef{
	prefix: 1,
	name:   "@meta",
	types:  []uint32{typeBlob, typeBlob},
	cols:   []string{"key", "value"},
	pkeys:  1,
}

var tableDefsTable = tableDef{
	prefix: 2,
	name:   "@table",
	types:  []uint32{typeBlob, typeBlob},
	cols:   []string{"name", "def"},
	pkeys:  1,
}

type DB struct {
	path   string
	kv     *KV
	tables map[string]*tableDef
}

func (db *DB) CreateTable(tdef *tableDef) error {
	if err := tdef.Validate(); err != nil {
		return fmt.Errorf("invalid table def: %w", err)
	}

	// check if the table exist
	tdefRecord := newRecord(&tableDefsTable).AddBlob("name", []byte(tdef.name))
	if ok, err := db.get(&tableDefsTable, tdefRecord); err != nil {
		return fmt.Errorf("retreiving table definition: %w", err)
	} else if ok {
		return fmt.Errorf("table already exists")
	}

	metaRecord := newRecord(&metaDataTable).AddBlob("key", []byte("next_prefix"))
	ok, err := db.get(&metaDataTable, metaRecord)
	if err != nil {
		return fmt.Errorf("retreiving next_prefix: %w", err)
	}
	if !ok {
		tdef.prefix = tableInitPrefix
		metaRecord.AddBlob("value", make([]byte, 4))
	} else {
		tdef.prefix = binary.LittleEndian.Uint32(metaRecord.Get("value").blob)
	}

	// increment the next_prefix
	binary.LittleEndian.PutUint32(metaRecord.Get("value").blob, tdef.prefix+1)
	if _, err := db._insert(*metaRecord, Upsert); err != nil {
		return fmt.Errorf("updating next_prefix: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := tdef.Serialize(buf); err != nil {
		return fmt.Errorf("serializing table definition: %w", err)
	}
	tdefRecord.AddBlob("def", buf.Bytes())
	if _, err := db._insert(*tdefRecord, Insert); err != nil {
		return fmt.Errorf("inserting table definition: %w", err)
	}
	return nil
}

func (db *DB) Get(table string, rec *record) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	rec.setTdef(tdef)
	rec.reorder()
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db.get(tdef, rec)
}

func (db *DB) getTableDef(table string) (*tableDef, error) {
	tdef, ok := db.tables[table]
	if ok {
		return tdef, nil
	}
	rec := new(record).AddBlob("name", []byte(table))
	ok, err := db.get(&tableDefsTable, rec)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	tdef = new(tableDef)
	err = json.Unmarshal(rec.Get("def").blob, tdef)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling: %w", err)
	}
	db.tables[table] = tdef
	return tdef, nil

}

func (db *DB) get(tdef *tableDef, rec *record) (bool, error) {
	if err := rec.validate(); err != nil {
		return false, err
	}

	key := new(bytes.Buffer)
	if err := rec.serializePK(key); err != nil {
		return false, fmt.Errorf("serializing primary key: %w", err)
	}
	val, ok := db.kv.Get(key.Bytes())
	if !ok {
		return false, nil
	}

	valBuf := bytes.NewBuffer(val)
	if err := decodeValues(valBuf, rec.vals[rec.tdef.pkeys:]); err != nil {
		return false, fmt.Errorf("decoding values: %w", err)
	}

	return true, nil
}

func (db *DB) delete(tdef *tableDef, rec record) (bool, error) {
	if err := rec.validate(); err != nil {
		return false, err
	}
	key := new(bytes.Buffer)
	if err := rec.serializePK(key); err != nil {
		return false, fmt.Errorf("serializing primary key: %w", err)
	}
	return db.kv.Del(key.Bytes())
}

func (db *DB) Delete(table string, rec record) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	rec.setTdef(tdef)
	rec.reorder()
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db.delete(tdef, rec)
}

func (db *DB) insert(table string, rec record, mode InsertMode) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	rec.setTdef(tdef)
	rec.reorder()
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db._insert(rec, mode)
}

func (db *DB) _insert(rec record, mode InsertMode) (bool, error) {
	if err := rec.validate(); err != nil {
		return false, err
	}
	key := new(bytes.Buffer)
	if err := rec.serializePK(key); err != nil {
		return false, fmt.Errorf("serializing primary key: %w", err)
	}
	val := new(bytes.Buffer)
	if err := rec.serializeNonPK(val); err != nil {
		return false, fmt.Errorf("serializing non-primary key: %w", err)
	}
	return db.kv.Update(key.Bytes(), val.Bytes(), mode)
}

func (db *DB) Insert(table string, rec record) (bool, error) {
	return db.insert(table, rec, Insert)
}

func (db *DB) Upsert(table string, rec record) (bool, error) {
	return db.insert(table, rec, Upsert)
}

func (db *DB) Update(table string, rec record) (bool, error) {
	return db.insert(table, rec, Update)
}

func decodeValues(in *bytes.Buffer, values []value) error {
	return json.NewDecoder(in).Decode(&values)
}

func encodeValues(out io.Writer, values []value) error {
	return json.NewEncoder(out).Encode(values)
}
