package kv

import (
	"encoding/csv"
	"os"
	"testing"
)

func testData(b *testing.B) map[string][]byte {
	f, err := os.Open("data.csv")
	if err != nil {
		b.Fatal(err)
	}

	r := csv.NewReader(f)

	data := make(map[string][]byte)

	for {
		record, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			b.Fatal(err)
		}
		data[record[0]] = []byte(record[1])

	}

	return data
}

func BenchmarkDB_Set(b *testing.B) {
	db := DB{}
	if err := db.Open("test.db"); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	kvs := testData(b)

	b.Logf("Inserting %d records", len(kvs))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k, v := range kvs {
			db.Set([]byte(k), v)
		}
	}
}

var result []byte

func BenchmarkDB_Get(b *testing.B) {
	db := DB{}
	if err := db.Open("test.db"); err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	kvs := testData(b)

	for k, v := range kvs {
		db.Set([]byte(k), v)
	}

	b.ResetTimer()

	var r []byte
	var err error

	for i := 0; i < b.N; i++ {
		for k := range kvs {
			r, err = db.Get([]byte(k))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	result = r
}
