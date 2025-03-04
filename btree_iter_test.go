package deadsimpledb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

type KVPair struct {
	key []byte
	val []byte
}

func (kvp KVPair) Less(o btree.Item) bool {
	return bytes.Compare(kvp.key, o.(KVPair).key) < 0
}

func TestBtreeIter(t *testing.T) {

	btreeSize := 10

	kvs := make([]KVPair, btreeSize)
	for i := 0; i < btreeSize; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := makeData(fmt.Sprintf("val-%d", i), BtreeMaxValueSize/4)
		kv := KVPair{key, val}
		kvs[i] = kv
	}
	pager := newMemoryPager()
	btree := newBtree(0, pager)
	for _, kv := range kvs {
		btree.Insert(kv.key, kv.val)
	}

	t.Run("next", func(t *testing.T) {
		btreeIter := btree.SeekLE(kvs[0].key)
		require.NotNil(t, btreeIter)
		require.True(t, btreeIter.isValid(), "btreeIter should be valid")

		for i, kv := range kvs {
			key, val, ok := btreeIter.Cur()
			require.Truef(t, ok, "kv %d: ok should be true", i)
			require.Equalf(t, kv.key, key, "kv %d: key should match", i)
			require.Equalf(t, kv.val, val, "kv %d: val should match", i)

			ok = btreeIter.next()
			if i == btreeSize-1 {
				require.Falsef(t, ok, "kv %d: next should return false", i)
				require.Falsef(t, btreeIter.isValid(), "btreeIter should be invalid")
			} else {
				require.Truef(t, ok, "kv %d: next should return true", i)
			}
		}
	})

	t.Run("prev", func(t *testing.T) {
		btreeIter := btree.SeekLE(kvs[btreeSize-1].key)
		require.NotNil(t, btreeIter)
		require.True(t, btreeIter.isValid(), "btreeIter should be valid")

		for i := btreeSize - 1; i >= 0; i-- {
			kv := kvs[i]
			key, val, ok := btreeIter.Cur()
			require.Truef(t, ok, "kv %d: ok should be true", i)
			require.Equalf(t, kv.key, key, "kv %d: key should match", i)
			require.Equalf(t, kv.val, val, "kv %d: val should match", i)

			ok = btreeIter.prev()
			if i == 0 {
				require.Falsef(t, ok, "kv %d: prev should return false", i)
				require.Falsef(t, btreeIter.isValid(), "btreeIter should be invalid")
			} else {
				require.Truef(t, ok, "kv %d: prev should return true", i)
			}
		}
	})
}
