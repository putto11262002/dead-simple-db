package deadsimpledb

import (
	"fmt"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
			Types: []Type{typeInt64, typeBlob, typeInt64},
			Cols:  []string{"key", "field1", "flied2"},
			Pkeys: 1,
		}
		db := setupDB()
		defer db.Close()
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
		require.NoError(t, err)
		require.Nil(t, nilTdef, "table def should be nil")
	})

	testTdef := &tableDef{
		Name:  "test_table",
		Types: []Type{typeInt64, typeBlob, typeInt64},
		Cols:  []string{"key", "field1", "flied2"},
		Pkeys: 1,
	}

	// records := []*tableRecord{
	// 	newTableRecord(testTdef).
	// 		SetInt64("key", 1).
	// 		SetBlob("field1", []byte("hello")).
	// 		SetInt64("flied2", 2),
	// 	newTableRecord(testTdef).
	// 		SetInt64("key", 2).
	// 		SetVal("field1", newNullValue(typeBlob)).
	// 		SetVal("flied2", newNullValue(typeInt64)),
	// }

	tr := newTableRecord(testTdef).
		SetInt64("key", 1).
		SetBlob("field1", []byte("hello")).
		SetInt64("flied2", 2)

	t.Run("insertRecord", func(t *testing.T) {
		t.Run("insert_mode", func(t *testing.T) {
			db := setupDB()
			defer db.Close()
			require.NoError(t, tr.validate(), "record is invalid")

			// insert record
			ok, err := db.insertRecord(*tr, Insert)
			require.NoError(t, err)
			require.True(t, ok)

			// assert record is inserted
			_tr := newTableRecord(testTdef).SetInt64("key", 1)
			require.NoError(t, _tr.validate(), "record is invalid")
			ok, err = db.getRecord(*_tr)
			require.NoError(t, err, "failed to get record")
			require.True(t, ok, "record should be found")
			require.Equal(t, tr, _tr, "record not match")

			// insert duplicate record
			ok, err = db.insertRecord(*tr, Insert)
			require.NoError(t, err, "failed to insert record")
			require.False(t, ok, "record should not be inserted")
		})
		t.Run("upsert_mode", func(t *testing.T) {
			db := setupDB()
			defer db.Close()
			tr := newTableRecord(testTdef).
				SetInt64("key", 1).
				SetBlob("field1", []byte("hello")).
				SetInt64("flied2", 2)
			require.NoError(t, tr.validate(), "record is invalid")

			// insert record
			ok, err := db.insertRecord(*tr, Upsert)
			require.NoError(t, err)
			require.True(t, ok)

			// assert record is inserted
			_tr := newTableRecord(testTdef).SetInt64("key", 1)
			require.NoError(t, _tr.validate(), "record is invalid")
			ok, err = db.getRecord(*_tr)
			require.NoError(t, err, "failed to get record")
			require.True(t, ok, "record should be found")
			require.Equal(t, tr, _tr, "record not match")

			// should update record if it exists
			tr.SetBlob("field1", []byte("hello_updated"))
			ok, err = db.insertRecord(*tr, Upsert)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.getRecord(*_tr)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tr, _tr)
		})
	})

	t.Run("getRecord", func(t *testing.T) {
		t.Run("record_exist", func(t *testing.T) {
			db := setupDB()
			defer db.Close()

			ok, err := db.insertRecord(*tr, Insert)
			require.NoError(t, err)
			require.True(t, ok)

			_tr := newTableRecord(testTdef).SetInt64("key", 1)
			ok, err = db.getRecord(*_tr)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tr, _tr)
		})

		t.Run("record_not_exist", func(t *testing.T) {
			db := setupDB()
			defer db.Close()

			_tr := newTableRecord(testTdef).SetInt64("key", 69)
			ok, err := db.getRecord(*_tr)
			require.NoError(t, err)
			require.False(t, ok)
		})
	})

	t.Run("deleteRecord", func(t *testing.T) {
		db := setupDB()
		defer db.Close()

		ok, err := db.insertRecord(*tr, Insert)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = db.deleteRecord(*tr)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = db.getRecord(*tr)
		require.NoError(t, err)
		require.False(t, ok)

		randomTr := newTableRecord(testTdef).SetInt64("key", 69)
		ok, err = db.deleteRecord(*randomTr)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("scan", func(t *testing.T) {
		setUpRecordWithKey := func(key int64) tableRecord {
			return *newTableRecord(testTdef).
				SetInt64("key", key).
				SetVal("field1", newNullValue(typeBlob)).
				SetVal("field2", newNullValue(typeInt64))
		}

		recordKeys := []int64{1, 3, 6, 12}

		testCases := []struct {
			from, to                 int64
			fromCmp, toCmp           Cmp
			expectedFrom, expectedTo int64
			invalid                  bool
		}{
			{
				from:         1,
				fromCmp:      CmpGE,
				to:           12,
				toCmp:        CmpLE,
				expectedFrom: 1,
				expectedTo:   12,
			},
			{
				from:         1,
				fromCmp:      CmpGT,
				to:           12,
				toCmp:        CmpLT,
				expectedFrom: 3,
				expectedTo:   6,
			},
			{
				from:         0,
				fromCmp:      CmpGE,
				to:           42,
				toCmp:        CmpLE,
				expectedFrom: 1,
				expectedTo:   12,
			},
			{
				from:    100,
				fromCmp: CmpGE,
				to:      200,
				toCmp:   CmpLE,
				invalid: true,
			},
		}

		db := setupDB()
		defer db.Close()
		records := make([]tableRecord, 0, len(recordKeys))
		for _, k := range recordKeys {
			r := setUpRecordWithKey(k)
			records = append(records, r)
			ok, err := db.insertRecord(r, Insert)
			require.NoError(t, err)
			require.True(t, ok)
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("testcase_%d", i+1), func(t *testing.T) {
				scanner, err := db.scan(setUpRecordWithKey(tc.from), tc.fromCmp, setUpRecordWithKey(tc.to), tc.toCmp)
				require.NoError(t, err)
				if tc.invalid {
					ok := scanner.Valid()
					require.False(t, ok)
				} else {
					fromIdx := slices.Index(recordKeys, tc.expectedFrom)
					toIdx := slices.Index(recordKeys, tc.expectedTo)
					for i := fromIdx; i <= toIdx; i++ {
						r, ok, err := scanner.Cur()
						require.NoError(t, err)
						require.Truef(t, ok, "expected %v but got invalid", records[i])
						require.Equal(t, records[i], *r)
						scanner.Next()
					}
				}
			})
		}

	})
}
