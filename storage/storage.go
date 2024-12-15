package storage

import "unsafe"

type Storage interface {
	Open(path string) error
	Close() error
	Flush() error
	SetRoot(addr uint64)
	// Root returns the memory address of root node page
	Root() uint64
	// Get retrieves a page at the given memory address
	// the len of the slice is equals the page size
	// If no page is found at the given address, it panics
	Get(addr uint64) (s []byte)

	// New allocates a page and return the memory address
	// If the slice exceeds the page size, it panics
	// If the slice is empty, the address is 0
	New(s []byte) (addr uint64)

	// Del deallocate a page at the given memory address
	// If no page is found at the given address, it panics
	Del(addr uint64)
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
