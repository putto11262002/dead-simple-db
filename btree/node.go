package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	BNODE_NODE = 1 // internal nodes without values, only keys and pointers
	BNODE_LEAF = 2

	HEADER       = 4
	POINTER_SIZE = 8 // the number of bytes used to store a pointer
	OFFSET_SIZE  = 2 // the number of bytes used to store an offset
	KLEN_SIZE    = 2 // the number of bytes used to store klen
	VLEN_SIZE    = 2 // the number of bytes used to store vlen

)

// BNode represents a node in BTree. BNode is store in memory as a byte slice.
//
// # Wire Format
//
// | type | nkeys | pointers   | offsets     | key-values
// | 2B   | 2B    | nkeys * 8B | nkeys * 2B  | ....
//
// type:
// - a fixed-size header that indicates the type of the node.
//
// nkeys: number of keys in the node
//
// pointers:
// - a lsit of pointers to the children nodes
//
// offsets:
// - list of offsets pointing to each key-value pair
// - off set is relvative to the position of the first KV pair
// - the offset of the first KV pair is always zero, so it is not stored
// - store the offset to the end of the last KV pair in the offset list - indicate the size of the node
//
// key-values:
//   - packed key-value pairs
//   - format
//     | klen | vlen | key | value
//     | 2B   | 2B   | ... | ...
type BNode struct {
	data []byte
}

func NewBNode(data []byte) BNode {
	return BNode{data}
}

func (n BNode) Bytes() []byte {
	return n.data
}

// btype returns the type of the node
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

// bkeys returns the number of keys in the node
func (node BNode) bkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setBtype(btype uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
}

func (node *BNode) setBkeys(bkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[2:4], bkeys)
}

// getPtr returns the pointer at the given index. If the index is out of bounds, it panics.
func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.bkeys() {
		panic("invalid idx")
	}
	offset := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[offset:])
}

// setPtr sets the pointer at the given index. If the index is out of bounds, it panics.
func (node *BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.bkeys() {
		panic("invalid idx")
	}
	offset := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[offset:], val)
}

// offsetPos returns the position of the offset at the given index.
// If the index is out of bounds, it panics.
func offsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.bkeys() {
		panic("invalid idx")
	}
	return HEADER + 8*node.bkeys() + 2*(idx-1)
}

// getOffset reLturns the offset at the given index.
// If the index is out of bounds, it panics.
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// setOffset sets the offset at the given index.
// The valid index is between 1 and nkeys.
// The reason why the first key (idx = 0) is skipped is that
// the offset of the first key is always zero.
func (node BNode) setOffset(idx, offset uint16) {
	if idx < 1 || idx > node.bkeys() {
		panic("invalid idx")
	}
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// kvPos returns the position of the key-value pair at the given index.
// If an invalid index is provided, it panics.
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.bkeys() {
		panic("invalid idx")
	}
	return HEADER + 8*node.bkeys() + 2*node.bkeys() + node.getOffset(idx)
}

// getKey returns the key at the given index.
// If an invalid index is provided, it panics.
func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.bkeys() {
		panic("invalid idx")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

// getVal returns the value at the given index.
// If an invalid index is provided, it panics.
func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.bkeys() {
		panic("invalid idx")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// nbytes returns the number of bytes used by the node
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.bkeys())
}

// nodeLookupLE returns the index of the key that is less than or equal to the given key.
// If the given key is less than the first key, it returns 0.
//
// Note: that the first key is skipped because it would have been compared from the parent node.
//
// TODO: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nnodes := node.bkeys()
	found := uint16(0)

	// the first key is copied from the parent node.
	// thus it's always less than or equal to the key
	for i := uint16(1); i < nnodes; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}
	return found

}

// nodeCopyKV copies n key-value pairs from the source node to the destination node.
// If the index is out of bounds - dstOff + n is greater than dst.nkeys() or srcOff + n is greater than src.nkeys() -, it panics.
func nodeCopyKV(dst BNode, src BNode, dstOff uint16, srcOff uint16, n uint16) {
	if dstOff+n > dst.bkeys() {
		panic(fmt.Sprintf("invalid range: valid range: %d-%d: got %d-%d", 0, dst.bkeys(), dstOff, dstOff+n))
	}

	if srcOff+n > src.bkeys() {
		panic(fmt.Sprintf("invalid range: valid range: %d-%d: got %d-%d", 0, src.bkeys(), srcOff, srcOff+n))
	}

	// copy pointers
	for i := uint16(0); i < n; i++ {
		dst.setPtr(dstOff+i, src.getPtr(srcOff+i))
	}

	// copy offsets
	dstStart := dst.getOffset(dstOff)
	srcStart := src.getOffset(srcOff)
	for i := uint16(1); i <= n; i++ {
		offset := dstStart + src.getOffset(srcOff+i) - srcStart
		dst.setOffset(dstOff+i, offset)
	}

	// copy KV pairs
	kvstart := src.kvPos(srcOff)
	kvend := src.kvPos(srcOff + n)
	copy(dst.data[dst.kvPos(dstOff):], src.data[kvstart:kvend])

}

// nodeNewKV inserts a new key-value pair at the given index.
// If there is already a key at the given index, it overwrites the key-value pair.
// If the index is out of bounds - if the idx is greater than new.nkeys() -, it panics.
func nodeNewKV(new BNode, idx uint16, ptr uint64, key, value []byte) {
	// append pointer
	new.setPtr(idx, ptr)

	// append KV pair
	pos := new.kvPos(idx)
	// put key length, value length
	klen := uint16(len(key))
	vlen := uint16(len(value))
	binary.LittleEndian.PutUint16(new.data[pos:][:2], klen)
	binary.LittleEndian.PutUint16(new.data[pos+2:][:2], vlen)

	// put key, value data
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+klen:], value)

	// update offset for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+klen+vlen)
}
