package deadsimpledb

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"

	"github.com/google/btree"
)

type Page struct {
	inner []byte
	ptr   uint64
}

func (p Page) getNodeType() uint16 {
	return binary.LittleEndian.Uint16(p.inner[:2])
}

func (p Page) Less(o btree.Item) bool {
	return p.ptr < o.(Page).ptr
}

func (p Page) copyFrom(page Page) {
	copy(p.inner, page.inner)

}

func (p Page) asBtreeNode() BtreeNode {
	if p.getNodeType() != BTREE_INTERNAL_NODE && p.getNodeType() != BTREE_LEAF_NODE {
		panic("page is not a btree node")
	}
	return BtreeNode{p.inner}
}

func (p Page) asFreeList() freeListNode {
	if p.getNodeType() != freeListNodeType {
		panic(fmt.Sprintf("page is not a free list node: %d", p.getNodeType()))
	}
	return freeListNode{p.inner}
}

type PagerMetadata struct {
	freeListHead uint64
	flushed      uint64
}

type Pager interface {
	allocate(page Page) uint64
	append(page Page) uint64
	write(page Page)
	free(uint64)
	load(uint64) Page
	flush() (*PagerMetadata, error)
	close() error
}

type MemoryPager struct {
	mem map[uint64]Page
	idx uint64
}

func newMemoryPager() *MemoryPager {
	return &MemoryPager{
		mem: make(map[uint64]Page),
		idx: 1,
	}
}

func (pager *MemoryPager) allocate(page Page) uint64 {
	return pager.append(page)
}

func (pager *MemoryPager) append(page Page) uint64 {
	assert(len(page.inner) <= PageSize, "page size exceeds PageSize")
	ptr := pager.idx
	pager.idx++
	_, ok := pager.mem[ptr]
	assert(ok == false, "page already exists")
	pager.mem[ptr] = page
	return ptr
}

func (pager *MemoryPager) free(ptr uint64) {
}

func (pager *MemoryPager) load(ptr uint64) Page {
	page, ok := pager.mem[ptr]
	assert(ok, "page not found")
	return page
}

func (pager *MemoryPager) write(page Page) {
	_, ok := pager.mem[page.ptr]
	assert(ok, "page not allocated")
	pager.mem[page.ptr] = page
}

func (pager *MemoryPager) flush() (*PagerMetadata, error) {
	return nil, nil
}

func (pager *MemoryPager) close() error {
	return nil
}

const (
	// pagerPageOffset is the offset page idx for the pager.
	// This is used to reserve pages for the master page.
	pagerPageOffset = 1
)

type MmapPager struct {
	// flushed is the number of pages that are flushed to disk
	flushed  uint64
	file     *os.File
	fileSize int
	mmapSize int
	// mmaps is a list of all the mmaped regions.
	// The is of each mmaped regions are multiples of PageSize.
	mmaps [][]byte

	// appended is a list of newly allocated pages that are not yet appended to the file
	appended *btree.BTree
	freeList *freeList
}

func newMmapPagerWithFreeList(file *os.File, flushed uint64, freeList uint64) (*MmapPager, error) {
	pager, err := newMmapPager(file, flushed)
	if err != nil {
		return nil, err
	}
	pager.freeList = newFreeList(pager)
	pager.freeList.read(freeList)
	return pager, nil
}

func newMmapPager(file *os.File, flushed uint64) (*MmapPager, error) {
	if file == nil {
		panic("does not currently support anonymous mmap")
	}

	if flushed < pagerPageOffset {
		flushed = pagerPageOffset
	}

	pager := &MmapPager{
		file:     file,
		flushed:  uint64(flushed),
		appended: btree.New(6),
	}

	if err := pager.initMmap(); err != nil {
		return nil, fmt.Errorf("intialising mmaap: %w", err)
	}
	// assert(int(pager.flushed)*PageSize <= pager.fileSize, "flushed pages exceeds file size")

	return pager, nil
}

func (pager *MmapPager) close() error {
	for _, mmap := range pager.mmaps {
		err := syscall.Munmap(mmap)
		if err != nil {
			return fmt.Errorf("syscall.Munmap: %w", err)
		}
	}
	return nil
}

func (pager *MmapPager) initMmap() error {
	fStat, err := os.Stat(pager.file.Name())
	if err != nil {
		return fmt.Errorf("os.Stat: %w", err)
	}

	if fStat.Size()%int64(PageSize) != 0 {
		return fmt.Errorf("file size is not a multiple of page size")
	}

	// initialize the initial mapping size to be at least 10 pages
	// and then double the size until it is greater than the file size
	mapSize := PageSize * 2
	for mapSize < int(fStat.Size()) {
		mapSize *= 2
	}

	mapping, err := syscall.Mmap(
		int(pager.file.Fd()),
		0,
		mapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("syscall.Mmap: %w", err)
	}
	pager.fileSize = int(fStat.Size())
	pager.mmapSize = mapSize
	pager.mmaps = [][]byte{mapping}
	return nil
}

func (pager *MmapPager) grow() error {
	npages := int(pager.flushed) + pager.appended.Len()
	if err := pager.growFile(npages); err != nil {
		return fmt.Errorf("growing file: %w", err)
	}
	if err := pager.growMmap(npages); err != nil {
		return fmt.Errorf("growing mmap: %w", err)
	}
	return nil
}

func (pager *MmapPager) growMmap(npages int) error {
	if pager.mmapSize >= npages*PageSize {
		return nil
	}
	mmap, err := syscall.Mmap(
		int(pager.file.Fd()),
		int64(pager.mmapSize),
		pager.mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	pager.mmaps = append(pager.mmaps, mmap)
	pager.mmapSize *= 2
	return nil
}

func (pager *MmapPager) growFile(npages int) error {
	fPages := pager.fileSize / PageSize
	if fPages >= npages {
		return nil
	}

	for fPages < npages {
		inc := fPages / 8
		if inc < 1 {
			inc = 1
		}
		fPages += inc
	}
	fSize := fPages * PageSize
	if err := pager.file.Truncate(int64(fSize)); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	pager.fileSize = fSize
	return nil
}

func (pager *MmapPager) free(ptr uint64) {
	if pager.freeList != nil {
		pager.mustPtrValid(ptr)
		pager.freeList.free(ptr)
	}
}

func (pager *MmapPager) load(ptr uint64) Page {
	pager.mustPtrValid(ptr)

	if ptr >= pager.flushed {
		p := pager.appended.Get(Page{ptr: ptr})
		assert(p != nil, "appened cache corrupted")
		return p.(Page)
	}

	return pager.getFlushedPage(ptr)
}

func (pager *MmapPager) getFlushedPage(ptr uint64) Page {
	start := uint64(0)
	for _, mmap := range pager.mmaps {
		end := start + uint64(len(mmap)/PageSize)
		if ptr < end {
			offset := (ptr - start) * uint64(PageSize)
			return Page{inner: mmap[offset : offset+uint64(PageSize)], ptr: ptr}
		}
		start = end
	}
	panic(fmt.Sprintf("invalid ptr: %x", ptr))
}

func (pager *MmapPager) allocate(page Page) uint64 {
	pager.mustValidSize(page)
	var ptr uint64
	if pager.freeList != nil && pager.freeList.freeCount() > 0 {
		var ok bool
		ptr, ok = pager.freeList.pop()
		assert(ok, "free list is currupted")
		page.ptr = ptr
		pager.write(page)
	} else {
		ptr = pager.append(page)
	}
	return ptr
}

func (pager *MmapPager) append(page Page) uint64 {
	pager.mustValidSize(page)
	ptr := pager.flushed + uint64(pager.appended.Len())
	page.ptr = ptr
	pager.appended.ReplaceOrInsert(page)
	return ptr
}

func (pager *MmapPager) mustValidSize(page Page) {
	assert(len(page.inner) <= PageSize, "page size execeed PageSize")
}

func (pager *MmapPager) mustPtrValid(ptr uint64) {
	assert(ptr >= pagerPageOffset && ptr < pager.flushed+uint64(pager.appended.Len()), "invalid ptr: %x", ptr)
}

func (pager *MmapPager) write(page Page) {
	pager.mustValidSize(page)
	pager.mustPtrValid(page.ptr)
	if page.ptr < pager.flushed {
		pager.load(page.ptr).copyFrom(page)
	} else {
		pager.appended.ReplaceOrInsert(page)
	}
}

func (pager *MmapPager) flush() (*PagerMetadata, error) {
	if pager.freeList != nil {
		pager.freeList.write()
	}

	if err := pager.grow(); err != nil {
		return nil, fmt.Errorf("growing file: %w", err)
	}

	pager.appended.Ascend(func(item btree.Item) bool {
		p := item.(Page)
		pager.getFlushedPage(p.ptr).copyFrom(p)
		return true
	})

	if err := pager.file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync: %w", err)
	}

	pager.flushed += uint64(pager.appended.Len())
	pager.appended.Clear(true)
	return &PagerMetadata{
		flushed:      pager.flushed,
		freeListHead: pager.freeList.head,
	}, nil
}
