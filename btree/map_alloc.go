package btree

import (
	"fmt"
	"unsafe"
)

type MapAllocator struct {
	pages    map[uint64][]byte
	pageSize int
}

func NewMappAllocator(pageSize int) *MapAllocator {
	return &MapAllocator{
		pages:    make(map[uint64][]byte),
		pageSize: pageSize,
	}
}

func (a MapAllocator) Get(addr uint64) BNode {
	page, ok := a.pages[addr]
	if !ok {
		panic(fmt.Sprintf("page not found at %v", addr))
	}
	return BNode{data: page}
}

func (s *MapAllocator) New(node BNode) uint64 {
	if len(node.data) > s.pageSize {
		panic("page size exceeded")
	}
	addr := sAddr(node.data)
	s.pages[addr] = node.data
	return addr

}

func (s *MapAllocator) Del(addr uint64) {
	_, ok := s.pages[addr]
	if !ok {
		panic(fmt.Sprintf("page not found at %v", addr))
	}
	delete(s.pages, addr)
}

// sAddr returns the memory address of the slice
// If the slice is empty, it returns 0
func sAddr(page []byte) uint64 {
	var addr uint64
	if len(page) > 0 {
		addr = uint64(uintptr(unsafe.Pointer(&page[0])))
	} else {
		addr = 0
	}
	return addr
}
