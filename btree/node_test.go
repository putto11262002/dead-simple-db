package btree

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type nodeData struct {
	key []byte
	val []byte
	ptr uint64
}

func newBNode(t uint16, data []nodeData, size int) BNode {
	node := BNode{data: make([]byte, size)}
	node.setBtype(t)
	node.setBkeys(uint16(len(data)))
	idx := uint16(0)
	for _, nd := range data {
		nodeNewKV(node, idx, nd.ptr, nd.key, nd.val)
		idx++

	}
	return node
}

func Test_newNode(t *testing.T) {
	var nt uint16 = BNODE_LEAF
	data := []nodeData{
		{
			key: []byte("a"),
			val: []byte("a"),
			ptr: 0,
		},
		{
			key: []byte("b"),
			val: []byte("b"),
			ptr: 1,
		},
	}
	size := TestPageSize

	node := newBNode(nt, data, size)

	if node.btype() != nt {
		t.Errorf("type: expected: %v, got: %v", nt, node.btype())
	}

	if node.bkeys() != uint16(len(data)) {
		t.Errorf("nkeys: expected: %v, got: %v", len(data), node.bkeys())
	}

	for i, nd := range data {
		if node.getPtr(uint16(i)) != nd.ptr {
			t.Errorf("ptr: expected: %v, got: %v", nd.ptr, node.getPtr(uint16(i)))
		}
		if diff := cmp.Diff(node.getKey(uint16(i)), nd.key); diff != "" {
			t.Errorf("key: expected: %v, got: %v", nd.key, node.getKey(uint16(i)))
		}
		if diff := cmp.Diff(node.getVal(uint16(i)), nd.val); diff != "" {
			t.Errorf("val: expected: %v, got: %v", nd.val, node.getVal(uint16(i)))
		}
	}

}

func Test_nodeCopyKV(t *testing.T) {
	testCases := []struct {
		src      BNode
		dst      BNode
		srcOff   int
		dstOff   int
		n        int
		expDst   BNode
		panicMsg string
	}{
		// copy all
		{
			src: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
				{
					key: []byte("b"),
					val: []byte("b"),
					ptr: 1,
				},
			}, TestPageSize),
			dst:    newBNode(BNODE_LEAF, make([]nodeData, 2), TestPageSize),
			srcOff: 0,
			dstOff: 0,
			n:      2,
			expDst: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
				{
					key: []byte("b"),
					val: []byte("b"),
					ptr: 1,
				},
			}, TestPageSize),
		},
		// copy to offset
		{
			src: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
				{
					key: []byte("b"),
					val: []byte("b"),
					ptr: 1,
				},
			}, TestPageSize),
			dst:    newBNode(BNODE_LEAF, make([]nodeData, 2), TestPageSize),
			srcOff: 0,
			dstOff: 1,
			n:      1,
			expDst: newBNode(BNODE_LEAF, []nodeData{
				{},
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
			}, TestPageSize),
		},
		// copy from offset
		{
			src: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
				{
					key: []byte("b"),
					val: []byte("b"),
					ptr: 1,
				},
			}, TestPageSize),
			dst:    newBNode(BNODE_LEAF, make([]nodeData, 1), TestPageSize),
			srcOff: 1,
			dstOff: 0,
			n:      1,
			expDst: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("b"),
					val: []byte("b"),
					ptr: 1,
				},
			}, TestPageSize),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("srcOff=%d,dstOff=%d,n=%d", tc.srcOff, tc.dstOff, tc.n), func(t *testing.T) {
			nodeCopyKV(tc.dst, tc.src, uint16(tc.dstOff), uint16(tc.srcOff), uint16(tc.n))
			nodeEqual(t, tc.expDst, tc.dst)

			if !cmp.Equal(tc.expDst.data, tc.dst.data) {
				fmt.Errorf("expected: %v, got: %v", tc.expDst, tc.dst)
			}
		})
	}

}

func Test_nodeNewKV(t *testing.T) {
	td := struct {
		node     BNode
		idx      int
		ptr      uint64
		key      []byte
		val      []byte
		expNode  BNode
		panicMsg string
	}{
		node: newBNode(BNODE_LEAF, []nodeData{
			{
				key: []byte("a"),
				val: []byte("a"),
				ptr: 0,
			},
			{
				key: []byte("b"),
				val: []byte("b"),
				ptr: 1,
			},
		}, TestPageSize),
		idx: 1,
		ptr: 2,
		key: []byte("c"),
		val: []byte("c"),
		expNode: newBNode(BNODE_LEAF, []nodeData{
			{
				key: []byte("a"),
				val: []byte("a"),
				ptr: 0,
			},
			{
				key: []byte("c"),
				val: []byte("c"),
				ptr: 2,
			},
		}, TestPageSize),
	}

	nodeNewKV(td.node, uint16(td.idx), td.ptr, td.key, td.val)
	nodeEqual(t, td.expNode, td.node)

}
