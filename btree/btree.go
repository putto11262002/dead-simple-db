package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyTree   = errors.New("tree is empty")
	ErrEmptyKey    = errors.New("key is empty")
	ErrKeyTooLarge = errors.New("key size exceeds maximum key size")
	ErrValTooLarge = errors.New("value size exceeds maximum value size")
)

// BNodeAllocator is an interface for allocating and deallocating BNodes in the buffer.
type BNodeAllocator interface {
	Get(uint64) BNode
	New(BNode) uint64
	Del(uint64)
}

type BTree struct {
	// pointer to the Root node
	Root uint64

	alloc    BNodeAllocator
	pageSize uint16
	// maximum key size
	maxKeySize uint16
	// maximum value size.
	maxValSize uint16
}

func NewBtree(root uint64, pageSize uint16, alloc BNodeAllocator) *BTree {
	return &BTree{
		Root:       root,
		pageSize:   pageSize,
		maxKeySize: (pageSize - HEADER - POINTER_SIZE - OFFSET_SIZE) * 1 / 3,
		maxValSize: (pageSize - HEADER - POINTER_SIZE - OFFSET_SIZE) * 2 / 3,
		alloc:      alloc,
	}
}

func (tree *BTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	if len(key) > int(tree.maxKeySize) {
		return nil, ErrKeyTooLarge
	}

	if tree.Root == 0 {
		return nil, ErrEmptyTree
	}

	if val, ok := treeGet(tree, tree.alloc.Get(tree.Root), key); ok {
		return val, nil
	}

	return nil, ErrKeyNotFound
}

// Delete deletes a key from the tree.
//
// The tree is shrink when:
// - the root node is not a leaf.
// - the root node has only one child
func (tree *BTree) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(key) > int(tree.maxKeySize) {
		return ErrKeyTooLarge
	}
	if tree.Root == 0 {
		return ErrEmptyTree
	}

	updated := treeDelete(tree, tree.alloc.Get(tree.Root), key)
	if len(updated.data) == 0 {
		return ErrKeyNotFound
	}

	tree.alloc.Del(tree.Root)
	if updated.btype() == BNODE_NODE && updated.bkeys() == 1 {
		tree.Root = updated.getPtr(0)
	} else {
		tree.Root = tree.alloc.New(updated)
	}
	return nil

}

//	func treeGet(tree *BTree, node BNode, key []byte) ([]byte, error) {
//		idx := nodeLookupLE(node, key)
//		switch node.btype() {
//		case BNODE_LEAF:
//			if bytes.Equal(key, node.getKey(idx)) {
//				return node.getVal(idx), nil
//			}
//			return nil, ErrKeyNotFound
//		case BNODE_NODE:
//			return treeGet(tree, tree.allocator.Get(node.getPtr(idx)), key)
//		default:
//			panic("invalid node type")
//
//		}
//	}
func treeGet(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BNODE_NODE:
		return treeGet(tree, tree.alloc.Get(node.getPtr(idx)), key)
	default:
		panic("invalid node type")
	}
}

// Insert inserts a KV into the tree.
//
// If the tree is empty a new leaf node is created, the value is inserted and the node is set to the root.
//
// If the tree alread exist the KV is inserted into the appropriate leaf node. Then we walk up the tree check if
// any of the parent node has to be splited.
// If root node has to be split a new level is added.
func (tree *BTree) Insert(key, val []byte) error {
	if len(key) < 0 {
		return ErrEmptyKey
	}

	if len(key) > int(tree.maxKeySize) {
		return ErrKeyTooLarge
	}

	if len(val) > int(tree.maxValSize) {
		return ErrValTooLarge
	}

	if tree.Root == 0 {
		// when the tree is empty
		root := BNode{data: make([]byte, tree.pageSize)}
		root.setBtype(BNODE_LEAF)
		root.setBkeys(2)

		// a dummy key, this makes the tree cover the whole key space
		// thus, a lookup can always find a containging key
		nodeNewKV(root, 0, 0, nil, nil)
		nodeNewKV(root, 1, 0, key, val)
		tree.Root = tree.alloc.New(root)

		return nil
	}

	node := tree.alloc.Get(tree.Root)
	tree.alloc.Del(tree.Root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit(node, tree.pageSize)
	if nsplit > 1 {
		// the root was split add a new level
		root := BNode{data: make([]byte, tree.pageSize)}
		root.setBtype(BNODE_NODE)
		root.setBkeys(nsplit)
		for i, cnode := range splitted[:nsplit] {
			ptr := tree.alloc.New(cnode)
			key := cnode.getKey(0)
			nodeNewKV(root, uint16(i), ptr, key, nil)
		}
		tree.Root = tree.alloc.New(root)
	} else {
		tree.Root = tree.alloc.New(splitted[0])
	}
	return nil
}

// treeInsert inserts a KV into a node.
// The result might be split into 2 nodes.
// It is the caller responsible to:
//   - deallocating the iput node
//   - splitting and allocating the result nodes.
//
// - if the node is a leaf node
//   - if the key already exists update the value
//   - if the key does not exist insert it
//
// - if the node is an internal node
//   - recursively insert the KV into the child node
//   - if the child node is split update the parent node
func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// the result node may be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*tree.pageSize)}

	// where to insert the key
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// is key exist update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// if key does not exist insert it after idx
			leaftInsert(new, node, idx+1, key, val)
		}

	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("invalid node")

	}
	return new

}

// leaftInsert inserts a new KV into the leaf node
func leaftInsert(new BNode, old BNode, idx uint16, key, value []byte) {
	new.setBtype(BNODE_LEAF)
	new.setBkeys(old.bkeys() + 1)
	nodeCopyKV(new, old, 0, 0, idx)
	nodeNewKV(new, idx, 0, key, value)
	nodeCopyKV(new, old, idx+1, idx, old.bkeys()-idx)

}

// leafUpdate updates a KV in the leaf node.
func leafUpdate(new, old BNode, idx uint16, key, val []byte) {
	new.setBtype(BNODE_LEAF)
	new.setBkeys(old.bkeys())
	// Copy KVs before the target KV
	nodeCopyKV(new, old, 0, 0, idx)
	// Put new KV at idx
	nodeNewKV(new, idx, 0, key, val)
	if idx+1 < old.bkeys() {
		// Copy KVS after the target KV
		nodeCopyKV(new, old, idx+1, idx+1, old.bkeys()-(idx+1))
	}
}

func nodeInsert(
	tree *BTree, new, node BNode, idx uint16, key, val []byte) {
	// get and deallocate the child node
	cptr := node.getPtr(idx)
	cnode := tree.alloc.Get(cptr)
	tree.alloc.Del(cptr)

	// recursive insertion to the child node
	cnode = treeInsert(tree, cnode, key, val)

	// split the result
	nsplit, splited := nodeSplit(cnode, tree.pageSize)

	// update the child links
	nodeReplaceChildN(tree, new, node, idx, splited[:nsplit]...)

}

// nodeReplaceChildN replace a pointer at idx with multiple pointers.
func nodeReplaceChildN(tree *BTree, new, old BNode, idx uint16, children ...BNode) {
	inc := uint16(len(children))

	new.setBtype(BNODE_NODE)
	new.setBkeys(old.bkeys() + inc - 1)
	nodeCopyKV(new, old, 0, 0, idx)
	for i, c := range children {
		nodeNewKV(new, idx+uint16(i), tree.alloc.New(c), c.getKey(0), nil)
	}
	nodeCopyKV(new, old, idx+inc, idx+1, old.bkeys()-(idx+1))

}

func nodeSplit(old BNode, pageSize uint16) (uint16, [3]BNode) {
	if old.nbytes() <= pageSize {
		old.data = old.data[:pageSize]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*pageSize)}
	right := BNode{make([]byte, pageSize)}
	nodeSplitLeftRight(left, right, old, pageSize)
	if left.nbytes() <= pageSize {
		left.data = left.data[:pageSize]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large
	leftleft := BNode{make([]byte, pageSize)}
	middle := BNode{make([]byte, pageSize)}
	nodeSplitLeftRight(leftleft, middle, left, pageSize)
	if leftleft.nbytes() > pageSize {
		panic("node still exceed page size after second split")

	}
	return 3, [3]BNode{leftleft, middle, right}
}

// nodeSplitLeftRight splits old into left and right.
// If old node fits in page it panics.
// The right is garantee to fit in a page but not the left node.
func nodeSplitLeftRight(left, right, old BNode, pageSize uint16) {
	if old.nbytes() <= pageSize {
		panic("cannot split node that already fits in a page")
	}
	eRSize := HEADER
	rKVOffset := old.bkeys() - 1
	for {
		KVpos := old.kvPos(rKVOffset)
		klen := binary.LittleEndian.Uint16(old.data[KVpos:])
		vlen := binary.LittleEndian.Uint16(old.data[KVpos+2:])
		delta := OFFSET_SIZE + POINTER_SIZE + KLEN_SIZE + VLEN_SIZE + int(klen) + int(vlen)
		if eRSize+delta > int(pageSize) {
			break
		}
		eRSize += delta
		rKVOffset--

	}
	left.setBtype(old.btype())
	left.setBkeys(rKVOffset + 1)
	right.setBtype(old.btype())
	right.setBkeys(old.bkeys() - rKVOffset - 1)

	// copy the right node
	nodeCopyKV(right, old, 0, rKVOffset+1, old.bkeys()-rKVOffset-1)

	// copy the rest to the left node
	nodeCopyKV(left, old, 0, 0, rKVOffset+1)

}

// treeDelete deletes a key from the tree
//
// - If the node is a leaf node remove the KV from the leaf node and return the new node.
// - If the node is an internal node recursively call treeDelete on the child node.
//   - If the updated child node can be merged with a sibling, merge them. See [shouldMerge] for when to merge.
func treeDelete(tree *BTree, node BNode, key []byte) BNode {

	// index of the given key or the key less than the given key
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// no exact match on the key
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		new := BNode{data: make([]byte, tree.pageSize)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("invalid node type")

	}
}

// leafDelete delete KV at a given index from the leaf node.
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setBtype(BNODE_LEAF)
	new.setBkeys(old.bkeys() - 1)
	nodeCopyKV(new, old, 0, 0, idx)
	nodeCopyKV(new, old, idx, idx+1, old.bkeys()-(idx+1))
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	cptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.alloc.Get(cptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.alloc.Del(cptr)

	new := BNode{data: make([]byte, tree.pageSize)}

	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated, tree.pageSize)
	switch mergeDir {
	case MERGE_LEFT:
		merged := BNode{data: make([]byte, tree.pageSize)}
		nodeMerge(merged, sibling, updated)
		tree.alloc.Del(node.getPtr(idx - 1))
		nodeReplaceChildrensWithMergedChild(new, node, idx-1, tree.alloc.New(merged), merged.getKey(0))
	case MERGE_RIGHT:
		merged := BNode{data: make([]byte, tree.pageSize)}
		nodeMerge(merged, sibling, updated)
		tree.alloc.Del(node.getPtr(idx + 1))
		nodeReplaceChildrensWithMergedChild(new, node, idx, tree.alloc.New(merged), merged.getKey(0))
	case NO_MERGE:
		if updated.bkeys() <= 1 {
			panic("unexpected nkeys")
		}
		nodeReplaceChildN(tree, new, node, idx, updated)

	}

	return new
}

// nodeReplaceChildrensWithMergedChild replaces the existing children of an internal node with the merged child.
func nodeReplaceChildrensWithMergedChild(new, node BNode, idx uint16, ptr uint64, key []byte) {
	new.setBtype(BNODE_NODE)
	new.setBkeys(node.bkeys() - 1)
	nodeCopyKV(new, node, 0, 0, idx)
	nodeNewKV(new, idx, ptr, key, nil)
	if idx+1 < new.bkeys() {
		nodeCopyKV(new, node, idx+1, idx+1, node.bkeys()-(idx+1))
	}
}

// nodeMerge merges two nodes into one.
func nodeMerge(new, left, right BNode) {
	new.setBtype(left.btype())
	new.setBkeys(left.bkeys() + right.bkeys())
	nodeCopyKV(new, left, 0, 0, left.bkeys())
	nodeCopyKV(new, right, left.bkeys(), 0, right.bkeys())
}

const (
	MERGE_RIGHT = 1
	MERGE_LEFT  = -1
	NO_MERGE    = 0
)

// shouldMerge determines if the updated children should be merged with a sibling.
// It returns an integer which indicates which sibling to merge with - -1 right sibling, +1 left sibling
// and 0 for no mering required -, the sibling node to merge with.
//
// The conditions for merging are:
// - if the node size is smaller than 1/4 of the page size.
// - if it has a sibling that when merged with the current node the combined size does not exceed the page size.
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode, pageSize uint16) (int, BNode) {
	if updated.nbytes() > pageSize/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		// when it is not the left-most node
		sibling := tree.alloc.Get(node.getPtr(idx - 1))
		mergedSize := sibling.nbytes() + updated.btype() - HEADER
		if mergedSize <= pageSize {
			return MERGE_LEFT, sibling
		}

	}

	if idx+1 < node.bkeys() {
		// when it is not the right-most node
		sibling := tree.alloc.Get(node.getPtr(idx + 1))
		mergedSize := sibling.nbytes() + updated.btype() - HEADER
		if mergedSize <= pageSize {
			return MERGE_RIGHT, sibling
		}
	}
	return NO_MERGE, BNode{}
}
