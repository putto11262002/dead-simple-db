package deadsimpledb

import (
	"encoding/binary"
	"fmt"
	"testing"

	testAssert "github.com/stretchr/testify/assert"
)

func Test_getNodeType(t *testing.T) {
	node := BtreeNode{make([]byte, 2, 2)}
	binary.LittleEndian.PutUint16(node.data, BTREE_INTERNAL_NODE)
	testAssert.Equal(t, BTREE_INTERNAL_NODE, node.getNodeType())
}

func Test_getNKeys(t *testing.T) {
	node := BtreeNode{make([]byte, 4, 4)}
	binary.LittleEndian.PutUint16(node.data[2:], 42)
	testAssert.Equal(t, uint16(42), node.getNkeys())
}

func Test_setHeader(t *testing.T) {
	node := BtreeNode{make([]byte, 4, 4)}
	node.setHeader(BTREE_INTERNAL_NODE, 42)
	testAssert.Equal(t, BTREE_INTERNAL_NODE, node.getNodeType())
	testAssert.Equal(t, uint16(42), node.getNkeys())
}

func Test_getPointer(t *testing.T) {
	node := BtreeNode{make([]byte, 28, 28)}
	node.setHeader(BTREE_INTERNAL_NODE, 3)
	binary.LittleEndian.PutUint64(node.data[BTREE_NODE_HEADER_SIZE:], 1)
	binary.LittleEndian.PutUint64(
		node.data[BTREE_NODE_HEADER_SIZE+BTREE_POINTER_SIZE:], 2)
	binary.LittleEndian.PutUint64(
		node.data[BTREE_NODE_HEADER_SIZE+2*BTREE_POINTER_SIZE:], 3)
	t.Run("index out of upper bounds", func(t *testing.T) {
		defer func() {
			r := recover()
			testAssert.NotNil(t, r)
		}()
		node.getPointer(3)
	})

	t.Run("get valid pointers", func(t *testing.T) {
		testAssert.Equal(t, uint64(1), node.getPointer(0))
		testAssert.Equal(t, uint64(2), node.getPointer(1))
		testAssert.Equal(t, uint64(3), node.getPointer(2))
	})
}

func Test_setPointer(t *testing.T) {
	node := BtreeNode{make([]byte, 20, 20)}
	node.setHeader(BTREE_INTERNAL_NODE, 2)
	t.Run("index out of bounds", func(t *testing.T) {
		defer func() {
			r := recover()
			testAssert.NotNil(t, r)
		}()
		node.setPointer(2, 1)
	})

	t.Run("set valid pointers", func(t *testing.T) {
		node.setPointer(0, 1)
		node.setPointer(1, 2)
		testAssert.Equal(t, uint64(1), node.getPointer(0))
		testAssert.Equal(t, uint64(2), node.getPointer(1))

	})
}

func Test_getOffset(t *testing.T) {
	node := BtreeNode{make([]byte, PageSize, PageSize)}
	node.setHeader(BTREE_LEAF_NODE, 2)
	binary.LittleEndian.PutUint16(
		node.data[BTREE_NODE_HEADER_SIZE+2*BTREE_POINTER_SIZE:], 1)
	binary.LittleEndian.PutUint16(
		node.data[BTREE_NODE_HEADER_SIZE+2*BTREE_POINTER_SIZE+BTREE_OFFSET_SIZE:], 2)

	testCases := []struct {
		i        uint16
		panic    bool
		expected uint16
	}{
		{
			i:        0,
			expected: 0,
		},
		{
			i:        1,
			expected: 1,
		},
		{
			i:        2,
			expected: 2,
		},
		{
			i:     3,
			panic: true,
		},
	}

	for _, ts := range testCases {
		t.Run(fmt.Sprintf("%+v", ts), func(t *testing.T) {
			defer func() {
				r := recover()
				if ts.panic {
					testAssert.NotNil(t, r)
				} else {
					testAssert.Nil(t, r)
				}
			}()
			offset := node.getOffset(ts.i)
			if !ts.panic {
				testAssert.Equal(t, ts.expected, offset)
			}
		})
	}

}

func Test_setOffset(t *testing.T) {
	node := BtreeNode{make([]byte, PageSize, PageSize)}
	node.setHeader(BTREE_LEAF_NODE, 2)

	testCases := []struct {
		i        uint16
		offset   uint16
		expected uint16
		panic    bool
	}{
		{
			i:        0,
			offset:   10,
			expected: 0,
		},
		{
			i:        1,
			offset:   1,
			expected: 1,
		},
		{
			i:        2,
			offset:   2,
			expected: 2,
		},
		{
			i:     3,
			panic: true,
		},
	}

	for _, ts := range testCases {
		t.Run(fmt.Sprintf("%+v", ts), func(t *testing.T) {
			defer func() {
				r := recover()
				if ts.panic {
					testAssert.NotNil(t, r)
				} else {
					testAssert.Nil(t, r)
				}
			}()
			node.setOffset(ts.i, ts.offset)
			if !ts.panic {
				testAssert.Equal(t, ts.expected, node.getOffset(ts.i))

			}
		})
	}
}

func Test_getKvPos(t *testing.T) {
	node := BtreeNode{make([]byte, PageSize, PageSize)}
	node.setHeader(BTREE_LEAF_NODE, 3)

	// Simulate offsets for key-value pairs
	node.setOffset(0, 0)
	node.setOffset(1, 10)
	node.setOffset(2, 25)
	node.setOffset(3, 40)

	expectedPositions := []uint16{
		BTREE_NODE_HEADER_SIZE + 3*BTREE_POINTER_SIZE + 3*BTREE_OFFSET_SIZE + 0,  // First KV at 0
		BTREE_NODE_HEADER_SIZE + 3*BTREE_POINTER_SIZE + 3*BTREE_OFFSET_SIZE + 10, // Second KV at 10
		BTREE_NODE_HEADER_SIZE + 3*BTREE_POINTER_SIZE + 3*BTREE_OFFSET_SIZE + 25, // Third KV at 25
		BTREE_NODE_HEADER_SIZE + 3*BTREE_POINTER_SIZE + 3*BTREE_OFFSET_SIZE + 40, // End
	}

	for i, expectedPos := range expectedPositions {
		testAssert.Equal(t, expectedPos, node.getKvPos(uint16(i)), "getKvPos failed for index %d", i)
	}

	// Out-of-bounds test: should panic
	t.Run("out-of-bounds index", func(t *testing.T) {
		defer func() {
			r := recover()
			testAssert.NotNil(t, r, "Expected panic for out-of-bounds index")
		}()
		node.getKvPos(4) // Should be out of bounds (3 keys only)
	})
}

func Test_getKey(t *testing.T) {
	node := BtreeNode{make([]byte, PageSize, PageSize)}
	node.setHeader(BTREE_LEAF_NODE, 2)

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}

	var offset uint16 = 0
	for i := uint16(0); i < 2; i++ {
		keyLen := uint16(len(keys[i]))
		valueLen := uint16(len(values[i]))
		pos := node.getKvPos(i)
		binary.LittleEndian.PutUint16(node.data[pos:], keyLen)
		binary.LittleEndian.PutUint16(node.data[pos+BTREE_KEY_LEN_SIZE:], valueLen)
		copy(node.data[pos+BTREE_KEY_LEN_SIZE+BTREE_VALUE_LEN_SIZE:], keys[i])
		copy(node.data[pos+BTREE_KEY_LEN_SIZE+BTREE_VALUE_LEN_SIZE+keyLen:], values[i])

		offset += keyLen + valueLen + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE
		node.setOffset(i+1, offset)
	}

	testAssert.Equal(t, keys[0], node.getKey(0))
	testAssert.Equal(t, keys[1], node.getKey(1))
}

func Test_getValue(t *testing.T) {
	node := BtreeNode{make([]byte, PageSize, PageSize)}
	node.setHeader(BTREE_LEAF_NODE, 2)

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}

	var offset uint16 = 0
	for i := uint16(0); i < 2; i++ {
		keyLen := uint16(len(keys[i]))
		valueLen := uint16(len(values[i]))
		pos := node.getKvPos(i)
		binary.LittleEndian.PutUint16(node.data[pos:], keyLen)
		binary.LittleEndian.PutUint16(node.data[pos+BTREE_KEY_LEN_SIZE:], valueLen)
		copy(node.data[pos+BTREE_KEY_LEN_SIZE+BTREE_VALUE_LEN_SIZE:], keys[i])
		copy(node.data[pos+BTREE_KEY_LEN_SIZE+BTREE_VALUE_LEN_SIZE+keyLen:], values[i])

		offset += keyLen + valueLen + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE
		node.setOffset(i+1, offset)
	}

	testAssert.Equal(t, values[0], node.getValue(0))
	testAssert.Equal(t, values[1], node.getValue(1))
}

func Test_size(t *testing.T) {
	t.Run("unintialized node", func(t *testing.T) {
		node := BtreeNode{}
		size := node.Size()
		testAssert.Equal(t, uint16(0), size)
	})
}
