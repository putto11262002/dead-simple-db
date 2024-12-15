package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var r = rand.New(rand.NewSource(99))

const (
	TestPageSize   = 4096
	TestMaxKeySize = 1000
	TestMaxValSize = 3000
)

func nBytesString(n int, prefix []byte) []byte {
	b := make([]byte, n)
	copy(b, prefix)

	for i := len(prefix); i < n; i++ {
		b[i] = byte(r.Intn(256))
	}

	return b
}

func nodeEqual(t *testing.T, exp, act BNode) {

	if exp.btype() != act.btype() {
		t.Errorf("btype: expected: %v, got: %v", exp.btype(), act.btype())
	}

	if exp.bkeys() != act.bkeys() {
		t.Errorf("nkeys: expected: %v, got: %v", exp.bkeys(), act.bkeys())
	}

	for i := uint16(0); i < exp.bkeys(); i++ {
		if exp.getPtr(i) != act.getPtr(i) {
			t.Errorf("ptr: expected: %v, got: %v", exp.getPtr(i), act.getPtr(i))
		}

		if diff := cmp.Diff(exp.getKey(i), act.getKey(i)); diff != "" {
			t.Errorf("key: expected: %v, got: %v", exp.getKey(i), act.getKey(i))
		}

		if diff := cmp.Diff(exp.getVal(i), act.getVal(i)); diff != "" {
			t.Errorf("val: expected: %v, got: %v", exp.getVal(i), act.getVal(i))
		}
	}

}

func setupBTree(t *testing.T) *BTree {

	alloc := NewMappAllocator(TestPageSize)

	return &BTree{
		alloc:      alloc,
		pageSize:   TestPageSize,
		maxKeySize: TestMaxKeySize,
		maxValSize: TestMaxValSize,
	}
}

func TestBtree_Get(t *testing.T) {

	tree := setupBTree(t)

	kvs := []struct {
		k, v []byte
	}{
		{
			k: nBytesString(TestMaxKeySize/4, []byte("ab")),
			v: nBytesString(TestMaxValSize/4, []byte("abv")),
		},
		{
			k: nBytesString(TestMaxKeySize/4, []byte("bb")),
			v: nBytesString(TestMaxValSize/4, []byte("bbv")),
		},
		{
			k: nBytesString(TestMaxKeySize/4, []byte("cc")),
			v: nBytesString(TestMaxValSize/4, []byte("ccv")),
		},
		{
			k: nBytesString(TestMaxKeySize*3/5, []byte("dd")),
			v: nBytesString(TestMaxValSize*3/5, []byte("ddv")),
		},
		{
			k: nBytesString(10, []byte("ee")),
			v: nBytesString(10, []byte("eev")),
		},
		{
			k: nBytesString(TestMaxKeySize/2, []byte("ff")),
			v: nBytesString(TestMaxValSize/2, []byte("ffv")),
		},
	}

	if _, err := tree.Get([]byte("foo")); err != ErrEmptyTree {
		t.Errorf("err: expected: %v, got: %v", ErrEmptyTree, err)
	}

	for _, kv := range kvs {
		tree.Insert(kv.k, kv.v)
		v, err := tree.Get(kv.k)
		if err != nil {
			t.Errorf("err: expected: %v, got: %v", nil, err)
		}
		if !bytes.Equal(v, kv.v) {
			t.Errorf("val: expected: %v, got: %v", kv.v, v)
		}
	}

	if _, err := tree.Get([]byte("foo")); err != ErrKeyNotFound {
		t.Errorf("err: expected: %v, got: %v", ErrKeyNotFound, err)
	}

}

func TestBtree_Insert(t *testing.T) {

	tree := setupBTree(t)

	kab := nBytesString(TestMaxKeySize/4, []byte("ab"))
	vab := nBytesString(TestMaxValSize/4, []byte("abv"))

	kbb := nBytesString(TestMaxKeySize/4, []byte("bb"))
	vbb := nBytesString(TestMaxValSize/4, []byte("bbv"))

	kcc := nBytesString(TestMaxKeySize/4, []byte("cc"))
	vcc := nBytesString(TestMaxValSize/4, []byte("ccv"))

	kdd := nBytesString(TestMaxKeySize*3/5, []byte("dd"))
	vdd := nBytesString(TestMaxValSize*3/5, []byte("ddv"))

	kee := nBytesString(10, []byte("ee"))
	vee := nBytesString(10, []byte("eev"))

	kff := nBytesString(TestMaxKeySize/2, []byte("ff"))
	vff := nBytesString(TestMaxValSize/2, []byte("ffv"))

	// before:
	// nil
	//
	// action: insert ab
	//  after:
	//     [nil, ab]
	//
	t.Run("insert into empty tree", func(t *testing.T) {
		tree.Insert(kab, vab)

		root := tree.alloc.Get(tree.Root)

		assertNodeHeader(t, root, BNODE_LEAF, 2)

		assertKV(t, root, kab, vab)

	})

	// action:
	//   insert bb
	//   insert cc
	//
	// after:
	//  [nil, ab, bb, cc]
	t.Run("insert into root", func(t *testing.T) {

		tree.Insert(kbb, vbb)
		tree.Insert(kcc, vcc)

		root := tree.alloc.Get(tree.Root)

		assertNodeHeader(t, root, BNODE_LEAF, 4)

		assertKV(t, root, kab, vab)
		assertKV(t, root, kbb, vbb)
		assertKV(t, root, kcc, vcc)

	})

	// action:
	// 	insert dd
	// after:
	//       | ptr_c1 | ptr_c2 |
	//          /         \
	//      [nil, ab, bb]      [cc, dd]

	t.Run("insert into root node + split (1 -> 2)", func(t *testing.T) {
		tree.Insert(kdd, vdd)
		root := tree.alloc.Get(tree.Root)

		assertNodeHeader(t, root, BNODE_NODE, 2)

		ptr1 := root.getPtr(0)
		ptr2 := root.getPtr(1)

		c1 := tree.alloc.Get(ptr1)
		c2 := tree.alloc.Get(ptr2)

		assertNodeHeader(t, c1, BNODE_LEAF, 3)
		assertKV(t, c1, kab, vab)
		assertKV(t, c1, kbb, vbb)

		assertNodeHeader(t, c2, BNODE_LEAF, 2)
		assertKV(t, c2, kcc, vcc)
		assertKV(t, c2, kdd, vdd)

	})

	// action:
	// insert ee
	// after:
	//  | ptr_c1 | ptr_c2 |
	//	 /         \
	// [nil, ab, bb]      [cc, dd, ee]
	t.Run("insert into leaf", func(t *testing.T) {
		tree.Insert(kee, vee)
		root := tree.alloc.Get(tree.Root)

		assertNodeHeader(t, root, BNODE_NODE, 2)

		ptr1 := root.getPtr(0)
		ptr2 := root.getPtr(1)

		c1 := tree.alloc.Get(ptr1)
		c2 := tree.alloc.Get(ptr2)

		assertNodeHeader(t, c1, BNODE_LEAF, 3)
		assertKV(t, c1, kab, vab)
		assertKV(t, c1, kbb, vbb)

		assertNodeHeader(t, c2, BNODE_LEAF, 3)
		assertKV(t, c2, kcc, vcc)
		assertKV(t, c2, kdd, vdd)
		assertKV(t, c2, kee, vee)

	})

	// action:
	// insert ff
	// after:
	//  | ptr_c1 | ptr_c2 | ptr_c3 |
	//	 /         |         \
	// [nil, ab, bb]  [cc, dd] [ee, ff]
	t.Run("insert into leaf + split (1 -> 2)", func(t *testing.T) {
		tree.Insert(kff, vff)

		root := tree.alloc.Get(tree.Root)

		assertNodeHeader(t, root, BNODE_NODE, 3)

		ptr1 := root.getPtr(0)
		ptr2 := root.getPtr(1)
		ptr3 := root.getPtr(2)

		c1 := tree.alloc.Get(ptr1)
		c2 := tree.alloc.Get(ptr2)
		c3 := tree.alloc.Get(ptr3)

		assertNodeHeader(t, c1, BNODE_LEAF, 3)
		assertKV(t, c1, kab, vab)
		assertKV(t, c1, kbb, vbb)

		assertNodeHeader(t, c2, BNODE_LEAF, 2)
		assertKV(t, c2, kcc, vcc)
		assertKV(t, c2, kdd, vdd)

		assertNodeHeader(t, c3, BNODE_LEAF, 2)
		assertKV(t, c3, kee, vee)
		assertKV(t, c3, kff, vff)
	})

}

func TestBtree_Delete(t *testing.T) {

	t.Run("delete from empty tree", func(t *testing.T) {
		tree := setupBTree(t)
		if err := tree.Delete([]byte("a")); err != ErrEmptyTree {
			t.Errorf("err: expected: %v, got: %v", ErrEmptyTree, err)
		}

	})

	t.Run("delete non-exist key", func(t *testing.T) {
		tree := setupBTree(t)
		key := []byte("a")
		val := []byte("a")
		tree.Insert(key, val)
		if err := tree.Delete([]byte("random")); err != ErrKeyNotFound {
			t.Errorf("err: expected: %v, got: %v", ErrKeyNotFound, err)
		}

	})

	// before:
	// 	[nil, a, b]
	// action:
	//	delete a
	// after:
	// 	[nil, b]
	t.Run("delete from root", func(t *testing.T) {
		tree := setupBTree(t)
		tree.Insert([]byte("a"), nBytesString(TestMaxKeySize/2, []byte("a")))
		tree.Insert([]byte("b"), nBytesString(TestMaxKeySize/2, []byte("b")))

		if err := tree.Delete([]byte("a")); err != nil {
			t.Errorf("err: expected: %v, got: %v", nil, err)
		}

		root := tree.alloc.Get(tree.Root)
		idx := nodeLookupLE(root, []byte("a"))
		if idx != 0 {
			t.Errorf("idx: expected: %v, got: %v", 0, idx)
		}

	})

	// before:
	//    | ptr_c1 | ptr_c2 |
	//	 /            \
	//     [nil, a]      [b]
	// action:
	//    delete a
	// after:
	//	[nil, b]
	t.Run("delete from leaf + merge (2 -> 1)", func(t *testing.T) {

		tree := setupBTree(t)
		ka := nBytesString(TestMaxKeySize/8, []byte("a"))
		va := nBytesString(TestMaxValSize/8, []byte("a"))
		tree.Insert(ka, va)

		kb := nBytesString(TestMaxKeySize, []byte("b"))
		vb := nBytesString(TestMaxValSize, []byte("b"))
		tree.Insert(kb, vb)

		root := tree.alloc.Get(tree.Root)
		c1 := tree.alloc.Get(root.getPtr(0))
		c2 := tree.alloc.Get(root.getPtr(1))

		assertKV(t, c1, ka, va)
		assertKV(t, c2, kb, vb)

		if err := tree.Delete(kb); err != nil {
			t.Errorf("err: expected: %v, got: %v", nil, err)
		}

		root = tree.alloc.Get(tree.Root)
		assertKV(t, root, ka, va)
		assertNotKV(t, root, kb, vb)

	})
}

func Test_leafInsert(t *testing.T) {
	old := newBNode(BNODE_LEAF, []nodeData{
		{
			key: []byte("a"),
			val: []byte("a"),
			ptr: 0,
		},
	}, TestPageSize)

	new := BNode{data: make([]byte, TestPageSize)}

	idx := uint16(1)
	key := []byte("b")
	val := []byte("b")

	expNew := newBNode(BNODE_LEAF, []nodeData{

		{
			key: []byte("a"),
			val: []byte("a"),
			ptr: 0,
		},
		{
			key: key,
			val: val,
		},
	}, TestPageSize)

	leaftInsert(new, old, idx, key, val)

	nodeEqual(t, expNew, new)

}

func Test_leafUpdate(t *testing.T) {
	tcs := []struct {
		name   string
		old    BNode
		new    BNode
		idx    uint16
		key    []byte
		val    []byte
		expNew BNode
	}{
		{
			name: "node with one key",
			old: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
					ptr: 0,
				},
			}, TestPageSize),
			new: BNode{data: make([]byte, TestPageSize)},
			idx: 0,
			key: []byte("b"),
			val: []byte("b"),

			expNew: newBNode(BNODE_LEAF, []nodeData{

				{
					key: []byte("b"),
					val: []byte("b"),
				},
			}, TestPageSize),
		},

		{
			name: "update last",
			old: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
				},

				{
					key: []byte("b"),
					val: []byte("b"),
				},
			}, TestPageSize),
			new: BNode{data: make([]byte, TestPageSize)},
			idx: 1,
			key: []byte("c"),
			val: []byte("c"),

			expNew: newBNode(BNODE_LEAF, []nodeData{

				{
					key: []byte("a"),
					val: []byte("a"),
				},

				{
					key: []byte("c"),
					val: []byte("c"),
				},
			}, TestPageSize),
		},
		{
			name: "update first",
			old: newBNode(BNODE_LEAF, []nodeData{
				{
					key: []byte("a"),
					val: []byte("a"),
				},

				{
					key: []byte("b"),
					val: []byte("b"),
				},
			}, TestPageSize),
			new: BNode{data: make([]byte, TestPageSize)},
			idx: 0,
			key: []byte("c"),
			val: []byte("c"),

			expNew: newBNode(BNODE_LEAF, []nodeData{

				{
					key: []byte("c"),
					val: []byte("c"),
				},

				{
					key: []byte("b"),
					val: []byte("b"),
				},
			}, TestPageSize),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			leafUpdate(tc.new, tc.old, tc.idx, tc.key, tc.val)
			nodeEqual(t, tc.expNew, tc.new)
		})

	}

}

func Test_nodeReplaceChildN(t *testing.T) {

	alloc := NewMappAllocator(TestPageSize)
	tree := NewBtree(0, TestPageSize, alloc)

	old := newBNode(BNODE_NODE, []nodeData{
		{
			ptr: 1,
		},
		{
			ptr: 2,
		},
	}, TestPageSize)

	new := BNode{data: make([]byte, TestPageSize)}

	// children
	child1 := newBNode(BNODE_NODE, []nodeData{
		{
			key: []byte("a"),
		},
	}, TestPageSize)
	child2 := newBNode(BNODE_NODE, []nodeData{
		{
			key: []byte("b"),
		},
	}, TestPageSize)
	c1ptr := alloc.New(child1)
	c2ptr := alloc.New(child2)
	childrens := []BNode{
		child1,
		child2,
	}

	expNew := newBNode(BNODE_NODE, []nodeData{
		{
			ptr: 1,
		},
		{
			ptr: c1ptr,
			key: []byte("a"),
		},
		{
			ptr: c2ptr,
			key: []byte("b"),
		}}, TestPageSize)

	nodeReplaceChildN(tree, new, old, 1, childrens...)

	nodeEqual(t, expNew, new)

}

func Test_nodeSplitLeftRight(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	v1 := nBytesString(3000, []byte("v1"))
	v2 := nBytesString(3000, []byte("v2"))
	v3 := nBytesString(1500, []byte("v3"))
	tcs := []struct {
		left, right, old, expLeft, expRight BNode
	}{
		{
			left:  BNode{data: make([]byte, 3*TestPageSize)},
			right: BNode{data: make([]byte, TestPageSize)},
			old: newBNode(BNODE_LEAF, []nodeData{
				{
					ptr: 1,
					key: k1,
					val: v1,
				},
				{
					ptr: 2,
					key: k2,
					val: v2,
				},
				{
					ptr: 3,
					key: k3,
					val: v3,
				},
			}, 3*TestPageSize),
			expLeft: newBNode(BNODE_LEAF, []nodeData{
				{
					ptr: 1,
					key: k1,
					val: v1,
				},
				{
					ptr: 2,
					key: k2,
					val: v2,
				},
			}, 3*TestPageSize),
			expRight: newBNode(BNODE_LEAF, []nodeData{
				{
					ptr: 3,
					key: k3,
					val: v3,
				},
			}, TestPageSize),
		},
	}

	for _, ts := range tcs {
		t.Run(fmt.Sprintf("node size: %d", ts.old.nbytes()), func(t *testing.T) {

			nodeSplitLeftRight(ts.left, ts.right, ts.old, TestPageSize)
			nodeEqual(t, ts.expLeft, ts.left)
			nodeEqual(t, ts.expRight, ts.right)

		})

	}

}

func assertNotKV(t *testing.T, node BNode, k, v []byte) {
	idx := nodeLookupLE(node, k)
	if _k := node.getKey(idx); bytes.Equal(k, _k) {
		t.Errorf("key: expected: %v, got: %v", k, _k)
	}
	if _v := node.getVal(idx); bytes.Equal(v, _v) {
		t.Errorf("val: expected: %v, got: %v", v, _v)
	}
}
func assertKV(t *testing.T, node BNode, k, v []byte) {
	idx := nodeLookupLE(node, k)
	if _k := node.getKey(idx); !bytes.Equal(k, _k) {
		t.Errorf("key: expected: %v, got: %v", k, _k)
	}
	if _v := node.getVal(idx); !bytes.Equal(v, _v) {
		t.Errorf("val: expected: %v, got: %v", v, _v)
	}
}

func assertNodeHeader(t *testing.T, node BNode, ntype uint16, nkeys uint16) {
	if node.btype() != ntype {
		t.Errorf("node.btype: expected: %d, got: %d", ntype, node.btype())
	}
	if node.bkeys() != nkeys {
		t.Errorf("node.bkeys: expected: %d, got: %d", nkeys, node.bkeys())
	}
}
