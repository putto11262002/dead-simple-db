package deadsimpledb

import (
	"bytes"
)

type IterNodeState struct {
	node BtreeNode
	// idx is the index of the key within the node.
	idx uint16
}

type BtreeIter struct {
	btree  *Btree
	stack  []IterNodeState
	cursor *IterNodeState
}

// stackPush pushes a new item onto the iterator stack and updates the cursor to point to the new item.
func (iter *BtreeIter) stackPush(n BtreeNode, idx uint16) {
	iter.stack = append(iter.stack, IterNodeState{node: n, idx: idx})
	iter.cursor = &iter.stack[len(iter.stack)-1]
}

// stackPopN removes n items from the top of the stack and updates the cursor accordingly.
func (iter *BtreeIter) stackPopN(n int) {
	if n <= 0 {
		return
	}
	if n > len(iter.stack) {
		return
	}
	iter.stack = iter.stack[:len(iter.stack)-int(n)]
	if len(iter.stack) > 0 {
		iter.cursor = &iter.stack[len(iter.stack)-1]
	} else {
		iter.cursor = nil
	}
}

// next moves the cursor to the next key. If there is no next key, false is returned, and the iterator becomes invalid.
// The algorithm performs the following steps:
//  1. If the current idx is less than nkeys-1, it moves to the next key in the current node.
//  2. If the current idx equals nkeys-1, it moves to the leftmost key in the right sibling node by
//     first moving to the parent node and continuing until the parent node's idx is less than nkeys-1,
//     then it keeps moving to the right sibling node until a leaf node is reached.
func (iter *BtreeIter) next() bool {
	if !iter.isIterable() {
		return false
	}

	if iter.cursor.idx < iter.cursor.node.getNkeys()-1 {
		iter.cursor.idx++
		return true
	}

	stack := iter.stack
	for len(stack) > 0 {
		level := stack[len(stack)-1]
		if level.idx < level.node.getNkeys()-1 {
			stack[len(stack)-1].idx++
			break
		}
		stack = stack[:len(stack)-1]
	}

	// if len(stack) == 0 {
	// 	return false
	// }

	iter.stackPopN(len(iter.stack) - len(stack))
	if iter.cursor == nil {
		return false
	}

	for iter.cursor.node.getNodeType() == BTREE_INTERNAL_NODE {
		childPtr := iter.cursor.node.getPointer(iter.cursor.idx)
		new := IterNodeState{node: iter.btree.pager.load(childPtr).asBtreeNode(), idx: 0}
		iter.stackPush(new.node, new.idx)
	}
	assert(iter.cursor.node.getNodeType() == BTREE_LEAF_NODE, "cursor node must be a leaf node")
	return true
}

// isIterable returns true if the iterator is in a valid state; otherwise, it returns false.
// The iterator is considered invalid when:
// 1. The cursor is nil.
// 2. The cursor points to an internal node.
// 3. The cursor's index is out of bounds.
func (iter BtreeIter) isIterable() bool {
	if iter.cursor == nil {
		return false
	}
	if iter.cursor.node.getNodeType() == BTREE_INTERNAL_NODE {
		return false
	}
	if iter.cursor.idx < 0 || iter.cursor.idx >= iter.cursor.node.getNkeys() {
		return false
	}
	return true
}

// Cur returns the current key and value the iterator points to. If the iterator is invalid, it returns nil values and false.
func (iter BtreeIter) Cur() ([]byte, []byte, bool) {
	if !iter.isIterable() || isDummyKey(iter.cursor.node, iter.cursor.idx) {
		return nil, nil, false
	}
	key := iter.cursor.node.getKey(iter.cursor.idx)
	value := iter.cursor.node.getValue(iter.cursor.idx)
	return key, value, true
}

// isDummyKey checks if the specified key in the node is considered a dummy key.
// A dummy key is characterized by having zero length for both key and value,
// and if it is located at the first index of an internal node, it also must have a pointer of zero.
func isDummyKey(node BtreeNode, idx uint16) bool {
	if idx != 0 {
		return false
	}

	keyLen := len(node.getKey(idx))
	valueLen := len(node.getValue(idx))
	ptr := node.getPointer(idx)
	if node.getNodeType() == BTREE_INTERNAL_NODE {
		return keyLen == 0 && valueLen == 0 && ptr == 0
	}
	return keyLen == 0 && valueLen == 0
}

// prev moves the cursor to the previous key. If there is no previous key, false is returned and the iterator becomes invalid.
func (iter *BtreeIter) prev() bool {
	if !iter.isIterable() {
		return false
	}
	if isDummyKey(iter.cursor.node, iter.cursor.idx-1) {
		iter.stackPopN(len(iter.stack))
		return false
	}
	if iter.cursor.idx > 0 {
		iter.cursor.idx--
		return true
	}

	stack := iter.stack
	// Walk up the tree until a parent node with idx > 0 is found (indicating a left sibling exists).
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		if node.idx > 0 {
			stack[len(stack)-1].idx--
			break
		}
		stack = stack[:len(stack)-1]
	}

	iter.stackPopN(len(iter.stack) - len(stack))
	if iter.cursor == nil {
		return false
	}
	// Walk down the tree always taking the rightmost child.
	for iter.cursor.node.getNodeType() == BTREE_INTERNAL_NODE {
		childPtr := iter.cursor.node.getPointer(iter.cursor.idx)
		child := iter.btree.pager.load(childPtr).asBtreeNode()
		iter.stackPush(child, child.getNkeys()-1)
	}

	return true
}

// SeekLE creates an iterator that starts at the first key less than or equal to the given key.
func (tree *Btree) SeekLE(key []byte) *BtreeIter {
	iter := &BtreeIter{btree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.pager.load(ptr).asBtreeNode()
		idx := findLessThanOrEqualTo(node, key)
		iter.stackPush(node, idx)
		if node.getNodeType() == BTREE_INTERNAL_NODE {
			ptr = node.getPointer(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

type Cmp int8

const (
	CmpGE Cmp = 3  // Greater than or equal to
	CmpGT Cmp = 2  // Greater than
	CmpLT Cmp = -2 // Less than
	CmpLE Cmp = -3 // Less than or equal to
)

// Seek creates an iterator that starts at a key satisfying the given comparison.
// If no key satisfies the comparison, an invalid iterator is returned.
func (tree *Btree) Seek(key []byte, cmp Cmp) *BtreeIter {
	iter := tree.SeekLE(key)
	if cmp == CmpLE {
		return iter
	}
	if cmp > 0 && isDummyKey(iter.cursor.node, iter.cursor.idx) {
		iter.next()
	}
	k, _, ok := iter.Cur()
	if !ok {
		return iter
	}
	if !cmpOK(k, cmp, key) {
		if cmp > 0 {
			iter.next()
		} else {
			iter.prev()
		}
	}
	return iter
}

// cmpOK compares two keys based on the given comparison type and returns true if the comparison holds.
func cmpOK(k1 []byte, cmp Cmp, k2 []byte) bool {
	r := bytes.Compare(k1, k2)
	switch cmp {
	case CmpGE:
		return r >= 0
	case CmpGT:
		return r > 0
	case CmpLT:
		return r < 0
	case CmpLE:
		return r <= 0
	default:
		panic("invalid cmp")
	}
}
