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
	// deref dereferences a pointer to a page
	deref func(uint64) BtreeNode
	// alloc allocates a new page and returns a pointer to it
	alloc func(BtreeNode) uint64
	// dealloc deallocates a page
	dealloc func(uint64)
}

func treeInsertNode(tree *Btree, node BtreeNode, key []byte, val []byte) BtreeNode {
	new := BtreeNode{data: make([]byte, 2*PageSize)}
	// where to insert the key in the node?
	idx := node.LoopupLessThanOrEqual(key)

	switch node.getNodeType() {
	case BTREE_LEAF_NODE:
		if bytes.Equal(key, node.getKey(idx)) {
			// if the key is equal to the existing key overwrite it
			leafUpdateKV(new, node, idx, key, val)
		} else {
			// the key found is less than the key to insert
			// insert the key after the key found
			leafInsertKV(new, node, idx+1, key, val)

		}
	case BTREE_INTERNAL_NODE:
		internalNodeInsert(tree, new, node, idx, key, val)
	default:
		panic("invalid node")
	}
	return new
}

func internalNodeInsert(tree *Btree, new, node BtreeNode, idx uint16, key, val []byte) {
	childPtr := node.getPointer(idx)
	child := tree.deref(childPtr)
	// TODO: why deallocate the child node here?
	tree.dealloc(childPtr)

	// recursively insert the key-value pair into the child node
	child = treeInsertNode(tree, child, key, val)

	nsplit, splited := nodeSplit(child)

	nodeReplaceOneWithMany(tree, new, node, idx, splited[:nsplit]...)
}

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
// There is no way to split the node into two equal nodes without rearranging the key-value pairs.
//
// nodeLeftRightSplit expects the left node have been allocated as much space as the original node,
// if this is not the case the function will panic.
func nodeLeftRightSplit(left, right, node BtreeNode) {
	rightSize := uint16(BTREE_NODE_HEADER_SIZE)
	var rightIdx uint16

	for i := node.getNkeys() - 1; i > 0; i-- {
		kvPos := node.getKvPos(i)
		extra := BTREE_POINTER_SIZE + BTREE_OFFSET_SIZE + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE +
			binary.LittleEndian.Uint16(node.data[kvPos:]) + binary.LittleEndian.Uint16(node.data[kvPos+BTREE_KEY_LEN_SIZE:])
		if rightSize+extra > uint16(PageSize) {
			rightIdx = i
			break
		}
		rightSize += extra
	}

	right.setHeader(node.getNodeType(), node.getNkeys()-rightIdx)
	left.setHeader(node.getNodeType(), rightIdx)
	nodeCopyN(right, node, 0, rightIdx, node.getNkeys()-rightIdx)
	nodeCopyN(left, node, 0, 0, rightIdx)
}

func nodeReplaceOneWithMany(tree *Btree, new, old BtreeNode, idx uint16, childrens ...BtreeNode) {
	assert(old.getNodeType() == BTREE_INTERNAL_NODE, "old is not an internal node")
	delta := uint16(len(childrens))
	new.setHeader(old.getNodeType(), old.getNkeys()+delta-1)
	nodeCopyN(new, old, 0, 0, idx)
	for i, child := range childrens {
		nodeWriteAt(new, idx+uint16(i), tree.alloc(child), child.getKey(0), nil)
	}
	nodeCopyN(new, old, idx+delta, idx+1, old.getNkeys()-(idx+1))
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
	new.setHeader(BTREE_LEAF_NODE, old.getNkeys()+1)
	nodeCopyN(new, old, 0, 0, idx)
	nodeWriteAt(new, idx, 0, key, value)
	nodeCopyN(new, old, idx+1, idx+1, old.getNkeys()-idx-1)
}
