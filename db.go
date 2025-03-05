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
	Types:  []uint32{typeBlob, typeBlob},
	Cols:   []string{"key", "value"},
	Pkeys:  1,
}

var tableDefsTable = tableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{typeBlob, typeBlob},
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

func (db *DB) CreateTable(tdef *tableDef) error {
	if err := tdef.Validate(); err != nil {
		return fmt.Errorf("invalid table def: %w", err)
	}

	// check if the table exist
	tdefRecord := newRecord(&tableDefsTable).AddBlob("name", []byte(tdef.Name))
	if ok, err := db.get(tdefRecord); err != nil {
		return fmt.Errorf("retreiving table definition: %w", err)
	} else if ok {
		return fmt.Errorf("table already exists")
	}

	metaRecord := newRecord(&metaDataTable).AddBlob("key", []byte("next_prefix"))
	ok, err := db.get(metaRecord)
	if err != nil {
		return fmt.Errorf("retreiving next_prefix: %w", err)
	}
	if !ok {
		tdef.Prefix = tableInitPrefix
		metaRecord.AddBlob("value", make([]byte, 4))
	} else {
		tdef.Prefix = binary.LittleEndian.Uint32(metaRecord.Get("value").Blob)
	}

	// increment the next_prefix
	binary.LittleEndian.PutUint32(metaRecord.Get("value").Blob, tdef.Prefix+1)
	if _, err := db.insert(*metaRecord, Upsert); err != nil {
		return fmt.Errorf("updating next_prefix: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := tdef.Serialize(buf); err != nil {
		return fmt.Errorf("serializing table definition: %w", err)
	}
	tdefRecord.AddBlob("def", buf.Bytes())
	if _, err := db.insert(*tdefRecord, Insert); err != nil {
		return fmt.Errorf("inserting table definition: %w", err)
	}
	db.tables[tdef.Name] = tdef
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
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db.get(rec)
}

func (db *DB) getTableDef(table string) (*tableDef, error) {
	tdef, ok := db.tables[table]
	if ok {
		return tdef, nil
	}
	rec := newRecord(&tableDefsTable).AddBlob("name", []byte(table))
	ok, err := db.get(rec)
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

func (db *DB) get(rec *record) (bool, error) {
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
	if err := rec.deserializeValues(valBuf); err != nil {
		return false, fmt.Errorf("decoding values: %w", err)
	}

	return true, nil
}

func (db *DB) delete(rec record) (bool, error) {
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
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db.delete(rec)
}

func (db *DB) insertWithMode(table string, rec record, mode InsertMode) (bool, error) {
	tdef, err := db.getTableDef(table)
	if err != nil {
		return false, fmt.Errorf("getting table definition: %w", err)
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found")
	}
	rec.setTdef(tdef)
	if err := rec.validate(); err != nil {
		return false, err
	}
	return db.insert(rec, mode)
}

func (db *DB) insert(rec record, mode InsertMode) (bool, error) {
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

func (db *DB) Insert(table string, rec record) (bool, error) {
	return db.insertWithMode(table, rec, Insert)
}

func (db *DB) Upsert(table string, rec record) (bool, error) {
	return db.insertWithMode(table, rec, Upsert)
}
