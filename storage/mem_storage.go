package storage

import (
	"fmt"
)

type MemStorage struct {
	pageSize int
	pages    map[uint64][]byte
}

func NewMemStorage(pageSize int) *MemStorage {
	return &MemStorage{
		pageSize: pageSize,
		pages:    make(map[uint64][]byte),
	}
}

func (s MemStorage) SetRoot(addr uint64) {

}

func (s MemStorage) Open(path string) error {
	return nil
}

func (s MemStorage) Close() error {
	return nil
}

func (s MemStorage) Flush() error {
	return nil
}

func (s MemStorage) Root() uint64 {
	return 0
}

func (s MemStorage) Get(addr uint64) []byte {
	page, ok := s.pages[addr]
	if !ok {
		panic(fmt.Sprintf("page not found at %v", addr))
	}
	return page
}

func (s *MemStorage) New(page []byte) uint64 {
	if len(page) > s.pageSize {
		panic("page size exceeded")
	}
	addr := sAddr(page)
	s.pages[addr] = page
	return addr

}

func (s *MemStorage) Del(addr uint64) {
	_, ok := s.pages[addr]
	if !ok {
		panic(fmt.Sprintf("page not found at %v", addr))
	}
	delete(s.pages, addr)
}

func (s MemStorage) PageSize() int {
	return s.pageSize
}
