package deadsimpledb

import (
	"fmt"
	"os"
	"syscall"

	"github.com/google/btree"
)

type Pager struct {
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

func newPager(file *os.File, flushed int) (*Pager, error) {
	if file == nil {
		panic("does not currently support anonymous mmap")
	}

	pager := &Pager{
		file:    file,
		flushed: uint64(flushed),
	}

	if err := pager.initMmap(); err != nil {
		return nil, fmt.Errorf("intialising mmaap: %w", err)
	}
	assert(int(pager.flushed)*PageSize <= pager.fileSize, "flushed pages exceeds file size")

	return pager, nil
}

func (pager *Pager) close() error {
	for _, mmap := range pager.mmaps {
		err := syscall.Munmap(mmap)
		if err != nil {
			return fmt.Errorf("syscall.Munmap: %w", err)
		}
	}
	return nil
}

func (pager *Pager) initMmap() error {
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

func (pager *Pager) grow() error {
	npages := int(pager.flushed) + pager.appended.Len()
	if err := pager.growFile(npages); err != nil {
		return fmt.Errorf("growing file: %w", err)
	}
	if err := pager.growMmap(npages); err != nil {
		return fmt.Errorf("growing mmap: %w", err)
	}
	return nil
}

func (pager *Pager) growMmap(npages int) error {
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

func (pager *Pager) growFile(npages int) error {
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
