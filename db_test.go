package deadsimpledb

import (
	"bytes"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_record(t *testing.T) {
	t.Run("serialize/deserialize", func(t *testing.T) {
		r := []value{
			newBlob([]byte("hello")),
			newNullValue(typeBlob),
			newInt64(123),
			newNullValue(typeInt64),
		}

		serialized := new(bytes.Buffer)
		err := serializeValues(serialized, r)
		require.NoError(t, err, "failed to serialze values")
		var deserialized []value
		err = deserializeValues(serialized, &deserialized)
		require.NoError(t, err, "failed to deserialze values")
		require.Equal(t, r, deserialized, "values not match")
	})
}

func TestDB(t *testing.T) {
	setupDB := func() *DB {
		dbPath := path.Join(t.TempDir(), fmt.Sprintf("%d", time.Now().Unix()))
		db, err := NewDB(dbPath)
		require.NoError(t, err, "failed to init db")
		return db
	}

	t.Run("Table", func(t *testing.T) {
		tdef := &tableDef{
			Name:  "test_table",
			Types: []uint32{typeInt64, typeBlob, typeInt64},
			Cols:  []string{"key", "field1", "flied2"},
			Pkeys: 1,
		}
		db := setupDB()
		// Store table def in db
		err := db.CreateTable(tdef)
		require.NoError(t, err, "failed to create table")

		// clear table def cache
		clear(db.tables)

		// Load table def from db
		_tdef, err := db.getTableDef(tdef.Name)
		require.NoError(t, err, "failed to get table def")
		require.Equal(t, tdef, _tdef, "table def not match")

		// Load non-exist table def
		nilTdef, err := db.getTableDef("not_exist_table")
		require.Error(t, err, "failed to get table def")
		require.Nil(t, nilTdef, "table def should be nil")
	})

}
