package deadsimpledb

import (
	"encoding/binary"
	"fmt"
)

var (
	freeListNodeType       = 3
	freeListHeaderSize int = 2 + 2 + 8 + 8
	// freeListCap is the maximum number of pointers a free list node can store.
	freeListCap int
)

// Free list node format:
// | header 					| body
// | type | size | total (only for head) | next | pointers
// | 2B   | 2B   | 8B     		 | 8B 	| size * 8B

// Free list disk layout
// head -> node1 -> node2 -> ... -> nodeN
// the head is where we pop and push the free pages

type freeListNode struct {
	data []byte
}

func newFreeListNode() *freeListNode {
	node := &freeListNode{
		data: make([]byte, PageSize, PageSize),
	}
	binary.LittleEndian.PutUint16(node.data, uint16(freeListNodeType))
	return node
}

// size returns the number of pointers in the node.
func (n freeListNode) size() int {
	return int(binary.LittleEndian.Uint16(n.data[2:]))
}

func (n freeListNode) setSize(size uint16) {
	binary.LittleEndian.PutUint16(n.data[2:], size)
}

func (n freeListNode) next() uint64 {
	return binary.LittleEndian.Uint64(n.data[12:])
}

func (n freeListNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(n.data[12:], next)
}

func (n freeListNode) getPtr(idx int) uint64 {
	assert(idx < n.size(), "%d is out of bound %d-%d", idx, 0, n.size())
	return binary.LittleEndian.Uint64(n.data[freeListHeaderSize+(idx*8):])
}

func (n freeListNode) setPtr(idx int, ptr uint64) {
	assert(idx < n.size(), "%d is out of bound %d-%d", idx, 0, n.size())
	binary.LittleEndian.PutUint64(n.data[freeListHeaderSize+(idx*8):], ptr)
}

func (n freeListNode) getTotal() uint64 {
	return binary.LittleEndian.Uint64(n.data[4:])
}

func (n freeListNode) setTotal(total uint64) {
	binary.LittleEndian.PutUint64(n.data[4:], total)
}

type freeList struct {
	head    uint64
	pending []uint64
	freed   []uint64
	popn    int
	cache   map[uint64]bool
	size    int
	pageIO  PageIO

	// callbacks for managing on disk pages
	page struct {
		get       func(uint64) freeListNode
		allocatae func(freeListNode) uint64
		write     func(uint64, freeListNode)
	}
}

func newFreeList() *freeList {
	return &freeList{
		cache: make(map[uint64]bool),
	}
}

func (fl *freeList) freeCount() int {
	return len(fl.freed)
}

func (fl *freeList) pendingCount() int {
	return len(fl.pending)
}

// pop returns the next free page from the free list.
func (fl *freeList) pop() (uint64, bool) {
	if len(fl.freed) == 0 {
		return 0, false
	}
	ptr := fl.freed[len(fl.freed)-1]
	fl.freed = fl.freed[:len(fl.freed)-1]
	fl.popn++
	delete(fl.cache, ptr)
	return ptr, true
}

func (fl *freeList) Free(ptr uint64) {
	if freed := fl.cache[ptr]; freed {
		panic(fmt.Sprintf("double free: %d", ptr))
	}
	fl.cache[ptr] = true
	fl.pending = append(fl.pending, ptr)
}

func (fl *freeList) getSize() int {
	if fl.head == 0 {
		return 0
	}
	return fl.size
}

func (fl *freeList) read(head uint64) {
	if head == 0 {
		return
	}
	fl.head = head
	headNode := fl.page.get(head)
	fl.size = int(headNode.getTotal())
	fl.freed = make([]uint64, fl.size)
	fl.cache = make(map[uint64]bool)
	fl.popn = 0

	freed := fl.freed

	// travese the linked list nodes to read to entire free list
	for head != 0 {
		node := fl.page.get(head)
		for i := 0; i < node.size(); i++ {
			ptr := node.getPtr(headNode.size() - i - 1)
			freed[len(freed)-1] = ptr
			freed = freed[:len(freed)-1]
			fl.cache[ptr] = true
		}
		head = headNode.next()
	}
	assert(len(freed) == 0, "free list is corrupted")
}

// write does the following
// remove pages that stored pointers that are in use. As they are removed, these pages are freed.
// prepend the pages that are pending to be freed to the free list
func (fl *freeList) write() {
	if fl.popn == 0 && len(fl.pending) == 0 {
		return
	}

	// size of the free list on disk

	assert(fl.popn <= fl.getSize(), "popn is greater than size")

	remaining := []uint64{}
	for fl.popn > 0 {
		assert(fl.head != 0, "free list is corrupted")
		node := fl.page.get(fl.head)
		fl.Free(fl.head)

		if fl.popn >= node.size() {
			fl.popn -= node.size()
		} else {
			// remain are the remaining free pages in the node
			nRemaining := node.size() - fl.popn
			fl.popn = 0
			for i := 0; i < nRemaining; i++ {
				remaining = append(remaining, node.getPtr(i))
			}

		}
		fl.head = node.next()
		fl.size -= node.size()
	}

	// nodeRemaining is the number of free pages in the remaining in current node
	reuse := []uint64{}
	for fl.freeCount() > 0 && len(reuse)*freeListCap < fl.pendingCount()+len(remaining) {
		if len(remaining) == 0 {
			assert(fl.head != 0, "free list is corrupted")

			node := fl.page.get(fl.head)
			fl.Free(fl.head)
			for i := 0; i < node.size(); i++ {
				remaining = append(remaining, node.getPtr(i))
			}
			fl.head = node.next()
			fl.size -= node.size()
		}
		ptr := fl.freed[len(fl.freed)-1]
		fl.freed = fl.freed[:len(fl.freed)-1]
		delete(fl.cache, ptr)
		remaining = remaining[:len(remaining)-1]
		reuse = append(reuse, ptr)

	}

	reuse = fl.writePtrs(remaining, reuse)

	// prepend the pages that are pending to be freed to the free list
	reuse = fl.writePtrs(fl.pending, reuse)

	fl.freed = append(fl.freed, fl.pending...)
	fl.pending = fl.pending[:0]

	fl.page.get(fl.head).setTotal(uint64(fl.size))
}

func (fl *freeList) writePtrs(ptrs []uint64, reuse []uint64) []uint64 {
	for len(ptrs) > 0 {
		new := newFreeListNode()
		size := len(ptrs)
		if size > freeListCap {
			size = freeListCap
		}
		new.setSize(uint16(size))
		new.setNext(fl.head)

		for i, ptr := range ptrs[:size] {
			new.setPtr(i, ptr)
		}
		ptrs = ptrs[size:]
		if len(reuse) > 0 {
			fl.head = reuse[0]
			reuse = reuse[1:]
			fl.page.write(fl.head, *new)
		} else {
			fl.head = fl.page.allocatae(*new)
		}
		fl.size += size

	}
	return reuse
}
