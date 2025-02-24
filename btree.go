package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"os"
)

// Btree Node Layout
// group:   | header          | pointers       | offsets        | packed keys-values
// data:    | type   | nkeys  | pointers       | offsets        | packed keys-values
// size:    | 2B     | 2B     | nkeys * 8B     | nkeys*2B       | nkeys * (key_len + value_len + key + value)
// go type: | uint16 | uint16 | nkeys * uint64 | nkeys * uint16 | nkeys * (uint16 + uint16 + key + value)
//
// Packed keys-values layout
// | key_len | value_len | key | value
// | 2B      | 2B        | key | value
// | uint16  | uint16    | key | value
//
// Encoding:
// All usigned integers are encoded in little-endian format
// All signed integers are encoded in two's complement little-endian format

const (
	BTREE_INTERNAL_NODE uint16 = 1
	BTREE_LEAF_NODE     uint16 = 2

	BTREE_NODE_HEADER_SIZE = 4
	BTREE_POINTER_SIZE     = 8
	BTREE_OFFSET_SIZE      = 2
	BTREE_KEY_LEN_SIZE     = 2
	BTREE_VALUE_LEN_SIZE   = 2
)

var (
	PageSize          int
	BtreeMaxKeySize   int
	BtreeMaxValueSize int
)

func init() {
	PageSize = os.Getpagesize()
	// This is when there is only one key-value pair in the node
	remaining := (PageSize - (BTREE_NODE_HEADER_SIZE + BTREE_POINTER_SIZE + BTREE_OFFSET_SIZE + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE))
	BtreeMaxKeySize = remaining / 3
	BtreeMaxValueSize = remaining - BtreeMaxKeySize
}

type Btree struct {
	root uint64
	// fetch dereferences a pointer to a page
	fetch func(uint64) BtreeNode
	// alloc allocates a new page and returns a pointer to it
	alloc func(BtreeNode) uint64
	// free deallocates a page
	free func(uint64)
}

func (tree *Btree) Get(key []byte) ([]byte, bool) {
	if len(key) == 0 || len(key) > BtreeMaxKeySize {
		return nil, false
	}
	if tree.root == 0 {
		return nil, false
	}
	return treeGet(tree, tree.fetch(tree.root), key)
}

// The tree will shrink when:
// 1. The root node is not a leaf
// 2. The root node has only one child
func (tree *Btree) Delete(key []byte) bool {
	assert(len(key) != 0, "key cannot be empty")
	assert(len(key) <= BtreeMaxKeySize, "key exceeded size limit %d", BtreeMaxKeySize)
	if tree.root == 0 {
		return false
	}
	newRoot := treeDelete(tree, tree.fetch(tree.root), key)
	if newRoot == nil {
		return false
	}
	tree.free(tree.root)
	if newRoot.getNkeys() == BTREE_INTERNAL_NODE && newRoot.getNkeys() == 1 {
		tree.root = newRoot.getPointer(0)
	} else {
		tree.root = tree.alloc(*newRoot)
	}
	return true
}

func (tree *Btree) Insert(key, value []byte) {
	assert(len(key) != 0, "key cannot be empty")
	assert(len(key) <= BtreeMaxKeySize, "key exceeded size limit %d", BtreeMaxKeySize)
	assert(len(value) <= BtreeMaxValueSize, "value exceeded size limit %d", BtreeMaxValueSize)

	if tree.root == 0 {
		// create the root node
		root := newBtreeNode()
		// Insert a empty key as the first key as it is the lowest possible key.
		// Any new key added will greater than it so making LookupLessThanOrEqual always succeed
		root.setHeader(BTREE_LEAF_NODE, 2)
		nodeWriteAt(root, 0, 0, nil, nil)
		nodeWriteAt(root, 1, 0, key, value)
		tree.root = tree.alloc(root)
		return
	}

	node := tree.fetch(tree.root)
	tree.free(tree.root)

	node = treeInsert(tree, node, key, value)
	nsplit, splitted := nodeSplit(node)
	if nsplit > 1 {
		root := newBtreeNode()
		root.setHeader(BTREE_INTERNAL_NODE, nsplit)
		for i, child := range splitted[:nsplit] {
			nodeWriteAt(root, uint16(i), tree.alloc(child), child.getKey(0), nil)
		}
		tree.root = tree.alloc(root)
	} else {
		tree.root = tree.alloc(splitted[0])
	}
}

func treeGet(tree *Btree, node BtreeNode, key []byte) ([]byte, bool) {
	idx := findLessThanOrEqualTo(node, key)
	if node.getNodeType() == BTREE_LEAF_NODE {
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getValue(idx), true
		} else {
			return nil, false
		}
	} else if node.getNodeType() == BTREE_INTERNAL_NODE {
		return treeGet(tree, tree.fetch(node.getPointer(idx)), key)
	} else {
		panic("invalid node type")
	}

}

// treeDelete deletes the key-value pair from the subtree rooted at the node.
// It returns the new node after the deletion. If no value was deleted it returns nil.
// It is the caller's responsibility to free the old node and merge the node if it is too small.
func treeDelete(tree *Btree, node BtreeNode, key []byte) *BtreeNode {
	idx := findLessThanOrEqualTo(node, key)
	// base case: when the leaf node is reached delete the key-value pair
	if node.getNodeType() == BTREE_LEAF_NODE {
		// if the key is not found return nil
		if !bytes.Equal(key, node.getKey(idx)) {
			return nil
		}
		new := BtreeNode{data: make([]byte, PageSize)}
		leafDeleteKV(new, node, idx)
		return &new
	} else if node.getNodeType() == BTREE_INTERNAL_NODE {
		childPtr := node.getPointer(idx)
		child := tree.fetch(childPtr)
		newChild := treeDelete(tree, child, key)
		if newChild == nil {
			return nil
		}
		tree.free(childPtr)

		new := newBtreeNode()

		mergeDir, sibling := shouldMerge(tree, node, idx, *newChild)
		if mergeDir == mergeNone {
			updateChildren(tree, new, node, idx, idx+1, *newChild)
			return &new
		}
		merged := BtreeNode{data: make([]byte, PageSize)}
		mergeNode(merged, *sibling, *newChild)
		if mergeDir == mergeLeft {
			tree.free(node.getPointer(idx - 1))
			// replace the left sibling and the child pointer with the merged node
			updateChildren(tree, new, node, idx-1, idx+1, merged)
		} else {
			tree.free(node.getPointer(idx + 1))
			updateChildren(tree, new, node, idx, idx+2, merged)
		}
		return &new

	} else {
		panic("invalid node type")
	}
}

// treeInsert inserts a key-value pair into the subtree rooted at the node.
// It returns the new node after the insertion, the node is not guaranteed to fit in a page.
// It is the caller's responsibility to free the old node and split the node if it is too large.
func treeInsert(tree *Btree, node BtreeNode, key []byte, val []byte) BtreeNode {
	new := newBtreeNodeWithPageSize(2)
	// get the index at which the key must be inserted with respect to the ordering.
	idx := findLessThanOrEqualTo(node, key)

	if node.getNodeType() == BTREE_LEAF_NODE {
		// base case: when the leaf node is reached insert the key-value pair
		if bytes.Equal(key, node.getKey(idx)) {
			// if the key is equal to the existing key overwrite it
			leafUpdateKV(new, node, idx, key, val)
		} else {
			// the key found is less than the key to insert
			// insert the key after the key found
			leafInsertKV(new, node, idx+1, key, val)
		}
	} else if node.getNodeType() == BTREE_INTERNAL_NODE {
		// resursively insert the key-value pair into the child node
		childPtr := node.getPointer(idx)
		child := tree.fetch(childPtr)
		child = treeInsert(tree, child, key, val)
		tree.free(childPtr)
		// split the child node if it is too large
		nsplit, splited := nodeSplit(child)
		updateChildren(tree, new, node, idx, idx+1, splited[:nsplit]...)
	} else {
		panic("invalid node")
	}

	return new
}

// nodeSplit splits the node into two or three nodes so that they all fit in a page
// while preserving the order of the key-value pairs.
// It returns the number splits and the split nodes.
func nodeSplit(node BtreeNode) (uint16, [3]BtreeNode) {
	// if the node fits in a page return the node truncate the overly allocated slice.
	if node.Size() <= uint16(PageSize) {
		node.shrinkToFit()
		return 1, [3]BtreeNode{node}
	}

	left := BtreeNode{data: make([]byte, 2*PageSize)}
	right := BtreeNode{data: make([]byte, PageSize)}

	nodeLeftRightSplit(left, right, node)

	// if the left node fits in a page return the left and right node
	if left.Size() <= uint16(PageSize) {
		left.shrinkToFit()
		return 2, [3]BtreeNode{left, right}
	}

	// if the left node does not fit in a page split the left node
	newLeft := BtreeNode{data: make([]byte, PageSize)}
	middle := BtreeNode{data: make([]byte, PageSize)}
	nodeLeftRightSplit(newLeft, middle, left)
	assert(newLeft.Size() <= uint16(PageSize), "left still does not fit after 3 splits")
	return 3, [3]BtreeNode{newLeft, middle, right}
}

// nodeLeftRightSplit splits the node into two nodes while preserving the order of the key-value pairs.
// The both splits are not guaranteed to fit in a page only right node is guaranteed to fit in a page.
// This is due to the segmentation and fixed order constraint of the key-value pairs.
// For exmaple, if the node has 3 key-value pairs where the first and third key-value pair size is about 1/3 of the maximum key-value pair size
// and the second key-value pair size is the maximum key-value pair size.
// There is no way to split the node into two nodes that fit in the page size without rearranging the key-value pairs.
//
// nodeLeftRightSplit expects the left node have been allocated as much space as the original node,
// if this is not the case the function will panic.
func nodeLeftRightSplit(left, right, node BtreeNode) {
	rightSize := uint16(BTREE_NODE_HEADER_SIZE)
	var rightIdx uint16

	for i := node.getNkeys() - 1; i >= 0; i-- {
		kvPos := node.getKvPos(i)
		extra := BTREE_POINTER_SIZE + BTREE_OFFSET_SIZE + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE +
			binary.LittleEndian.Uint16(node.data[kvPos:]) + binary.LittleEndian.Uint16(node.data[kvPos+BTREE_KEY_LEN_SIZE:])
		if rightSize+extra > uint16(PageSize) {
			rightIdx = i + 1
			break
		}
		rightSize += extra
	}

	right.setHeader(node.getNodeType(), node.getNkeys()-rightIdx)
	left.setHeader(node.getNodeType(), rightIdx)
	nodeCopyN(right, node, 0, rightIdx, node.getNkeys()-rightIdx)
	nodeCopyN(left, node, 0, 0, rightIdx)
}

// updateChildren allocates the new children nodes and overwrites the old children pointers
// between [start, end) with the new children pointers. If end - start < len(children) the remaining
// children pointers are shifted to the right to make room for the new children pointers.
func updateChildren(tree *Btree, new, old BtreeNode, start, end uint16, children ...BtreeNode) {
	assert(old.getNodeType() == BTREE_INTERNAL_NODE, "old is not an internal node")
	assert(start < end, "start should be less than end")
	assert(end <= old.getNkeys(), "end should be less than or equal to the number of keys in the node")
	newNKeys := old.getNkeys() - (end - start) + uint16(len(children))
	new.setHeader(BTREE_INTERNAL_NODE, newNKeys)
	nodeCopyN(new, old, 0, 0, start)
	for i, child := range children {
		nodeWriteAt(new, start+uint16(i), tree.alloc(child), child.getKey(0), nil)
	}
	nodeCopyN(new, old, start+uint16(len(children)), end, old.getNkeys()-end)
}

// mergeNode merges the left and right node into the merged node.
// Where the left node is copied to the merged node at the start and the right node is copied to the merged node after the left node.
func mergeNode(merged, left, right BtreeNode) {
	assert(left.getNodeType() == right.getNodeType(), "left and right node type mismatch")
	merged.setHeader(left.getNodeType(), left.getNkeys()+right.getNkeys())
	nodeCopyN(merged, left, 0, 0, left.getNkeys())
	nodeCopyN(merged, right, left.getNkeys(), 0, right.getNkeys())
}

type mergeOption uint8

const (
	mergeLeft mergeOption = iota
	mergeRight
	mergeNone
)

// shouldMerge determines if the node should be merged with its sibling.
// Conditions for merging are:
// 1. The node is smaller than 1/4 page.
// 2. The node has a sibling that when merged will fit in a page.
func shouldMerge(tree *Btree, parent BtreeNode, idx uint16, child BtreeNode) (mergeOption, *BtreeNode) {
	if child.Size() >= uint16(PageSize)/4 {
		return mergeNone, nil
	}
	if idx > 0 {
		sibling := tree.fetch(parent.getPointer(idx - 1))
		mergedSize := sibling.Size() + child.Size() - BTREE_NODE_HEADER_SIZE
		if mergedSize <= uint16(PageSize) {
			return mergeLeft, &sibling
		}

	}
	if idx+1 < parent.getNkeys() {
		sibling := tree.fetch(parent.getPointer(idx + 1))
		mergedSize := sibling.Size() + child.Size() - BTREE_NODE_HEADER_SIZE
		if mergedSize <= uint16(PageSize) {
			return mergeRight, &sibling
		}
	}

	return mergeNone, nil
}

// findLessThanOrEqualTo searches for the largest key within the node that is less than or equal to the
// key and return its index.
//
// The first key is always going to be less than or equals to the key as it is copied from the parent node.
// So it is going to be returned if no other key matches the condition.
func findLessThanOrEqualTo(n BtreeNode, key []byte) uint16 {
	nkeys := n.getNkeys()
	// The most recent key that is less than or equals to the key
	idx := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		if cmp := bytes.Compare(n.getKey(i), key); cmp <= 0 {
			idx = i
		} else if cmp >= 0 {
			// encountered a key that is greater than the key
			break
		}

	}
	return idx
}

// leafInsertKV write a key value pair at i-th by shift the surrounding key-value pairs to make room for the new key-value pair.
func leafInsertKV(new, old BtreeNode, idx uint16, key, value []byte) {
	assert(old.getNodeType() == BTREE_LEAF_NODE, "old node is not a leaf node")
	new.setHeader(BTREE_LEAF_NODE, old.getNkeys()+1)
	nodeCopyN(new, old, 0, 0, idx)
	nodeWriteAt(new, idx, 0, key, value)
	nodeCopyN(new, old, idx+1, idx, old.getNkeys()-idx)

}

// leafUpdateKV  write a key-value pair at i-th by overwriting the existing key-value pair.
func leafUpdateKV(new, old BtreeNode, idx uint16, key, value []byte) {
	assert(old.getNodeType() == BTREE_LEAF_NODE, "old node is not a leaf node")
	new.setHeader(BTREE_LEAF_NODE, old.getNkeys())
	nodeCopyN(new, old, 0, 0, idx)
	nodeWriteAt(new, idx, 0, key, value)
	nodeCopyN(new, old, idx+1, idx+1, old.getNkeys()-idx-1)
}

// leafDeleteKV delete the i-th key-value pair.
func leafDeleteKV(new, old BtreeNode, idx uint16) {
	assert(old.getNodeType() == BTREE_LEAF_NODE, "old is not a leaf node")
	new.setHeader(old.getNodeType(), old.getNkeys()-1)
	nodeCopyN(new, old, 0, 0, idx)
	nodeCopyN(new, old, idx, idx+1, old.getNkeys()-(idx+1))

}
