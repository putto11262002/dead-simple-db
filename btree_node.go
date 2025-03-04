package deadsimpledb

import (
	"encoding/binary"
)

type BtreeNode struct {
	data []byte
}

// newBtreeNode creates a new BtreeNode a one page size.
func newBtreeNode() BtreeNode {
	return BtreeNode{make([]byte, PageSize)}
}

// newBtreeNodeWithPageSize creates a new BtreeNode with the given page size.
func newBtreeNodeWithPageSize(n int) BtreeNode {
	return BtreeNode{make([]byte, PageSize*n)}
}

func (n BtreeNode) asPage() Page {
	assert(len(n.data) <= PageSize, "node overflows page size")
	return Page{
		inner: n.data,
	}
}

func (n BtreeNode) getNodeType() uint16 {
	return binary.LittleEndian.Uint16(n.data[:2])
}

func (n BtreeNode) getNkeys() uint16 {
	return binary.LittleEndian.Uint16(n.data[2:4])
}

func (n BtreeNode) setHeader(nodeType uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(n.data[:2], nodeType)
	binary.LittleEndian.PutUint16(n.data[2:4], nkeys)
}

func (n BtreeNode) getPointer(i uint16) uint64 {
	assert(i < n.getNkeys(), "%d is out of bounds %d-%d", i, 0, n.getNkeys()-1)
	pos := BTREE_NODE_HEADER_SIZE + i*BTREE_POINTER_SIZE
	return binary.LittleEndian.Uint64(n.data[pos:])
}

func (n BtreeNode) setPointer(i uint16, p uint64) {
	assert(i < n.getNkeys(), "%d is out of bounds %d-%d", i, 0, n.getNkeys()-1)
	pos := BTREE_NODE_HEADER_SIZE + i*BTREE_POINTER_SIZE
	binary.LittleEndian.PutUint64(n.data[pos:], p)
}

func (n BtreeNode) getNoneZeroOffsetPos(i uint16) uint16 {
	nKeys := n.getNkeys()
	assert(i >= 1 && i <= nKeys,
		"%d out of bound %d-%d", i, 1, nKeys)
	return BTREE_NODE_HEADER_SIZE + nKeys*BTREE_POINTER_SIZE + (i-1)*BTREE_OFFSET_SIZE

}

// getOffset returns the offset of the i-th key-value pair in the node
// The offset index range is [0, nkeys].
// The 0-th offset is not stored as it is always 0.
// The nkeys-th offset is the end of the last key value whic is the end of the node.
func (n BtreeNode) getOffset(i uint16) uint16 {
	nKeys := n.getNkeys()
	assert(i <= nKeys, "%d out of bound %d-%d", i, 0, nKeys)
	if i == 0 {
		return 0
	}
	pos := n.getNoneZeroOffsetPos(i)
	return binary.LittleEndian.Uint16(n.data[pos:])
}

// setOffset sets i-th offset.
// Setting 0-th offset is a no-op.
func (n BtreeNode) setOffset(i uint16, offset uint16) {
	nKeys := n.getNkeys()
	assert(i <= nKeys, "%d out of bound %d-%d", i, 0, nKeys)
	if i == 0 {
		return
	}
	pos := n.getNoneZeroOffsetPos(i)
	binary.LittleEndian.PutUint16(n.data[pos:], offset)
}

// getKvPos get i-th kv position where i is in [0, nkeys].
// nkeys-ith points to the end of the node, hence the node size.
func (n *BtreeNode) getKvPos(i uint16) uint16 {
	nKeys := n.getNkeys()
	assert(i <= nKeys, "%d out of bound %d-%d", i, 0, nKeys)
	return BTREE_NODE_HEADER_SIZE + n.getNkeys()*BTREE_POINTER_SIZE + n.getNkeys()*BTREE_OFFSET_SIZE + n.getOffset(i)
}

func (n BtreeNode) getKey(i uint16) []byte {
	nKeys := n.getNkeys()
	assert(i < nKeys, "%d out of bound %d-%d", i, 0, nKeys-1)
	pos := n.getKvPos(i)
	keyLen := binary.LittleEndian.Uint16(n.data[pos:])
	return n.data[pos+BTREE_VALUE_LEN_SIZE+BTREE_KEY_LEN_SIZE:][:keyLen]
}

func (n BtreeNode) getValue(i uint16) []byte {
	nKeys := n.getNkeys()
	assert(i < nKeys, "%d out of bound %d-%d", i, 0, nKeys-1)
	pos := n.getKvPos(i)
	keyLen := binary.LittleEndian.Uint16(n.data[pos:])
	valueLen := binary.LittleEndian.Uint16(n.data[pos+BTREE_KEY_LEN_SIZE:])
	return n.data[pos+BTREE_KEY_LEN_SIZE+BTREE_VALUE_LEN_SIZE+keyLen:][:valueLen]
}

func (n BtreeNode) Size() uint16 {
	return n.getKvPos(n.getNkeys())
}

// shrinkToFit shrinks the node to fit the actual size of the node.
// Inpired by Rust's Vec::shrink_to_fit
func (n *BtreeNode) shrinkToFit() {
	n.data = n.data[:n.Size()]
}

// nodeCopyN copys pointers, offsets, key-value pairs at [destIdx, destIdx+n) from dest to src at [srcIdx, srcIdx+n).
func nodeCopyN(dest, src BtreeNode, destIdx, srcIdx uint16, n uint16) {
	assert(destIdx+n <= dest.getNkeys(), "dest: index out of bound")
	assert(srcIdx+n <= src.getNkeys(), "src: index out of bound")
	if n == 0 {
		return
	}

	// copy over the pointers
	for i := uint16(0); i < n; i++ {
		dest.setPointer(destIdx+i, src.getPointer(srcIdx+i))
	}

	// copy over offsets
	srcBaseOffset := src.getOffset(srcIdx)
	destBaseOffset := dest.getOffset(destIdx)
	for i := uint16(1); i <= n; i++ {
		offset := destBaseOffset + src.getOffset(srcIdx+i) - srcBaseOffset
		dest.setOffset(destIdx+i, offset)
	}

	// copy over key-value pairs
	srcStart := src.getKvPos(srcIdx)
	srcEnd := src.getKvPos(srcIdx + n)
	copy(dest.data[dest.getKvPos(destIdx):], src.data[srcStart:srcEnd])
}

// nodeWriteAt writes pointer, key, and value to i-th index and updates the i+1-th offset.
// It is the caller's responsibility to ensure the remaining offsets beyond i+1 are updated.
func nodeWriteAt(node BtreeNode, i uint16, ptr uint64, key, value []byte) {
	node.setPointer(i, ptr)

	keyLen := uint16(len(key))
	valueLen := uint16(len(value))
	pos := node.getKvPos(i)

	binary.LittleEndian.PutUint16(node.data[pos:], keyLen)
	binary.LittleEndian.PutUint16(node.data[pos+BTREE_KEY_LEN_SIZE:], valueLen)
	kvDataStart := pos + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE
	copy(node.data[kvDataStart:], key)
	copy(node.data[kvDataStart+keyLen:], value)

	newOffset := node.getOffset(i) + BTREE_KEY_LEN_SIZE + BTREE_VALUE_LEN_SIZE + keyLen + valueLen
	node.setOffset(i+1, newOffset)
}
