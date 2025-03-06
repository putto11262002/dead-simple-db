package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const tableInitPrefix = 3

var metaDataTable = tableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []Type{typeBlob, typeBlob},
	Cols:   []string{"key", "value"},
	Pkeys:  1,
}

var tableDefsTable = tableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []Type{typeBlob, typeBlob},
	Cols:   []string{"name", "def"},
	Pkeys:  1,
}

type DB struct {
	path   string
	kv     *KV
	tables map[string]*tableDef
}

func NewDB(path string) (*DB, error) {
	kv, err := NewKV(path)
	if err != nil {
		return nil, fmt.Errorf("init kv: %w", err)
	}
	db := &DB{
		path:   path,
		kv:     kv,
		tables: make(map[string]*tableDef),
	}
	return db, nil
}

func (db *DB) Close() error {
	return db.kv.Close()
}

func (db *DB) Insert(table string, rec AnonymousRecord) (bool, error) {
	return db.insert(table, rec, Insert)
}

func (db *DB) Upsert(table string, rec AnonymousRecord) (bool, error) {
	return db.insert(table, rec, Upsert)
}

func (db *DB) Delete(table string, ar AnonymousRecord) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	tr := ar.IntoTableRecord(tdef)
	return db.deleteRecord(*tr)
}

func (db *DB) Get(table string, ar AnonymousRecord) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	tr := ar.IntoTableRecord(tdef)
	return db.getRecord(*tr)
}

func (db *DB) Scan(table string, from tableRecord, fromCmp Cmp, t tableRecord, toCmp Cmp) (*Scanner, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return nil, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	return db.scan(from, fromCmp, t, toCmp)
}

func (db *DB) CreateTable(tdef *tableDef) error {
	if err := tdef.Validate(); err != nil {
		return fmt.Errorf("invalid table def: %w", err)
	}

	// check if the table exist
	tdefRecord := newTableRecord(&tableDefsTable).SetBlob("name", []byte(tdef.Name))
	if ok, err := db.getRecord(*tdefRecord); err != nil {
		return fmt.Errorf("retreiving table definition: %w", err)
	} else if ok {
		return fmt.Errorf("table already exists")
	}

	metaRecord := newTableRecord(&metaDataTable).SetBlob("key", []byte("next_prefix"))
	ok, err := db.getRecord(*metaRecord)
	if err != nil {
		return fmt.Errorf("retreiving next_prefix: %w", err)
	}
	if !ok {
		tdef.Prefix = tableInitPrefix
		metaRecord.SetBlob("value", make([]byte, 4))
	} else {
		tdef.Prefix = binary.LittleEndian.Uint32(metaRecord.Get("value").Blob)
	}

	// increment the next_prefix
	binary.LittleEndian.PutUint32(metaRecord.Get("value").Blob, tdef.Prefix+1)
	if _, err := db.insertRecord(*metaRecord, Upsert); err != nil {
		return fmt.Errorf("updating next_prefix: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := tdef.Serialize(buf); err != nil {
		return fmt.Errorf("serializing table definition: %w", err)
	}
	tdefRecord.SetBlob("def", buf.Bytes())
	if _, err := db.insertRecord(*tdefRecord, Insert); err != nil {
		return fmt.Errorf("inserting table definition: %w", err)
	}
	db.tables[tdef.Name] = tdef
	return nil
}

func (db *DB) insert(table string, ar AnonymousRecord, mode InsertMode) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	tr := ar.IntoTableRecord(tdef)
	return db.insertRecord(*tr, mode)
}

func (db *DB) getTableDef(table string) (*tableDef, error) {
	tdef, ok := db.tables[table]
	if ok {
		return tdef, nil
	}
	rec := newTableRecord(&tableDefsTable).SetBlob("name", []byte(table))
	ok, err := db.getRecord(*rec)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	tdef = new(tableDef)
	err = json.Unmarshal(rec.Get("def").Blob, tdef)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling: %w", err)
	}
	db.tables[table] = tdef
	return tdef, nil
}

func (db *DB) getRecord(rec tableRecord) (bool, error) {
	if err := rec.ValidatePK(); err != nil {
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
	if err := rec.deserializeValues(valBuf); err != nil {
		return false, fmt.Errorf("decoding values: %w", err)
	}

	return true, nil
}

func (db *DB) deleteRecord(rec tableRecord) (bool, error) {
	if err := rec.ValidatePK(); err != nil {
		return false, err
	}
	key := new(bytes.Buffer)
	if err := rec.serializePK(key); err != nil {
		return false, fmt.Errorf("serializing primary key: %w", err)
	}
	return db.kv.Del(key.Bytes())
}

func (db *DB) insertRecord(rec tableRecord, mode InsertMode) (bool, error) {
	if err := rec.validate(); err != nil {
		return false, err
	}
	key := new(bytes.Buffer)
	if err := rec.serializePK(key); err != nil {
		return false, fmt.Errorf("serializing primary key: %w", err)
	}
	val := new(bytes.Buffer)
	if err := rec.serializeValues(val); err != nil {
		return false, fmt.Errorf("serializing non-primary key: %w", err)
	}
	return db.kv.Update(key.Bytes(), val.Bytes(), mode)
}

func (db *DB) scan(from tableRecord, fromCmp Cmp, t tableRecord, toCmp Cmp) (*Scanner, error) {
	if !(fromCmp > 0 && toCmp < 0) {
		return nil, fmt.Errorf("invalid range")
	}

	if err := from.ValidatePK(); err != nil {
		return nil, fmt.Errorf("from : %w", err)
	}
	if err := t.ValidatePK(); err != nil {
		return nil, fmt.Errorf("to : %w", err)
	}

	fromKey := new(bytes.Buffer)
	if err := from.serializePK(fromKey); err != nil {
		return nil, fmt.Errorf("serializing from key: %w", err)
	}
	toKey := new(bytes.Buffer)
	if err := t.serializePK(toKey); err != nil {
		return nil, fmt.Errorf("serializing to key: %w", err)
	}
	iter := db.kv.tree.Seek(fromKey.Bytes(), fromCmp)

	scanner := &Scanner{
		tdef:  t.tdef,
		toKey: toKey.Bytes(),
		toCmp: toCmp,
		iter:  iter,
	}
	return scanner, nil
}

type Scanner struct {
	tdef *tableDef
	iter *BtreeIter
	// encoded Key1eIter
	toKey []byte
	toCmp Cmp
}

// Valid returns true if the scanner is within specified range
func (sc *Scanner) Valid() bool {
	if sc.iter == nil {
		return false
	}
	if !sc.iter.isIterable() {
		return false
	}
	key, _, _ := sc.iter.Cur()
	return cmpOK(key, sc.toCmp, sc.toKey)
}

// Next moves the scanner to the next record
func (sc *Scanner) Next() {
	assert(sc.Valid(), "scanner is invalid")
	sc.iter.next()
}

// Cur returns the current record
func (sc *Scanner) Cur() (*tableRecord, bool, error) {
	if !sc.Valid() {
		return nil, false, nil
	}
	key, val, _ := sc.iter.Cur()
	rec := newTableRecord(sc.tdef)

	if err := rec.deserializePK(bytes.NewReader(key)); err != nil {
		return nil, false, fmt.Errorf("decoding primary key: %w", err)
	}
	if err := rec.deserializeValues(bytes.NewReader(val)); err != nil {
		return nil, false, fmt.Errorf("decoding values: %w", err)
	}
	return rec, true, nil
}
