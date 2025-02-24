package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

var (
	sig = []byte("deadsimpledb")
)

type DB struct {
	// flushed is the number of pages that are flushed to disk
	flushed uint64
	// dirty is a list of pages that need to be flushed to disk
	dirty    [][]byte
	file     *os.File
	fileSize int
	mmapSize int
	// mmaps is a list of all the mmaped regions.
	// The is of each mmaped regions are multiples of PageSize.
	mmaps [][]byte
	tree  *Btree
	path  string
}

func NewDB(path string) *DB {
	return &DB{
		path: path,
		tree: &Btree{},
	}
}

func (db *DB) Open() error {
	fail := func(err error) error {
		db.Close()
		return err
	}

	f, err := os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("os.OpenFile: %w", err)
	}
	db.file = f

	if err := db.initMmap(); err != nil {
		return fail(err)
	}

	if db.fileSize != 0 {
		if err := db.loadMasterPage(); err != nil {
			return fail(err)
		}
	} else {
		db.flushed = 1
	}

	// initialise the btree
	db.tree.fetch = func(ptr uint64) BtreeNode {
		page := db.getPage(ptr)
		return BtreeNode{page}
	}
	db.tree.alloc = func(node BtreeNode) uint64 {
		return db.allocatePage(node.data)
	}
	db.tree.free = func(ptr uint64) {
		db.freePage(ptr)
	}

	return nil

}

func (db *DB) Close() error {
	for _, mmap := range db.mmaps {
		err := syscall.Munmap(mmap)
		if err != nil {
			return fmt.Errorf("syscall.Munmap: %w", err)
		}
	}
	return db.file.Close()
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *DB) Set(key, value []byte) error {
	db.tree.Insert(key, value)
	return db.flushPages()
}

func (db *DB) Del(key []byte) (bool, error) {
	ok := db.tree.Delete(key)
	if !ok {
		return false, nil
	}
	return ok, db.flushPages()
}

func (db *DB) initMmap() error {
	fStat, err := os.Stat(db.file.Name())
	if err != nil {
		return fmt.Errorf("os.Stat: %w", err)
	}

	if fStat.Size()%int64(PageSize) != 0 {
		return fmt.Errorf("file size is not a multiple of page size")
	}

	// initialize the initial mapping size to be at least 2 pages
	// and then double the size until it is greater than the file size
	mapSize := PageSize * 2
	for mapSize < int(fStat.Size()) {
		mapSize *= 2
	}

	mapping, err := syscall.Mmap(
		int(db.file.Fd()),
		0,
		mapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("syscall.Mmap: %w", err)
	}
	db.fileSize = int(fStat.Size())
	db.mmapSize = mapSize
	db.mmaps = [][]byte{mapping}
	return nil

}

func (db *DB) extendMmap(npages int) error {
	if db.mmapSize >= npages*PageSize {
		return nil
	}
	mmap, err := syscall.Mmap(
		int(db.file.Fd()),
		int64(db.mmapSize),
		db.mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmaps = append(db.mmaps, mmap)
	db.mmapSize *= 2
	return nil
}

func (db *DB) extendFile(npages int) error {
	fPages := db.fileSize / PageSize
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
	if err := db.file.Truncate(int64(fSize)); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	db.fileSize = fSize
	return nil
}

func (db *DB) extend(npages int) error {
	if err := db.extendFile(npages); err != nil {
		return fmt.Errorf("growing file: %w", err)
	}
	if err := db.extendMmap(npages); err != nil {
		return fmt.Errorf("growing mmap: %w", err)
	}
	return nil
}

func (db *DB) getPage(ptr uint64) []byte {
	start := uint64(0)
	for _, mmap := range db.mmaps {
		end := start + uint64(len(mmap)/PageSize)
		if ptr < end {
			offset := (ptr - start) * uint64(PageSize)
			return mmap[offset : offset+uint64(PageSize)]
		}
		start = end
	}
	panic("invalid ptr")
}

func (db *DB) allocatePage(page []byte) uint64 {
	// TODO: reuse deallocated pages
	assert(len(page) <= PageSize, "page data is larger than PageSize")
	ptr := db.flushed + uint64(len(db.dirty))
	db.dirty = append(db.dirty, page)
	return ptr
}

func (db *DB) freePage(ptr uint64) error {
	return nil
}

func (db *DB) loadMasterPage() error {
	data := db.mmaps[0]
	_sig := data[0:16]
	root := binary.LittleEndian.Uint64(data[16:])
	npages := binary.LittleEndian.Uint64(data[24:])

	if !bytes.Equal(sig, _sig[:len(sig)]) {
		return errors.New("invalid signature")
	}

	bad := (npages < 1) || (npages > uint64(db.fileSize/PageSize)) || (root < 0) || (root >= npages)
	if bad {
		return errors.New("invalid master page")
	}
	db.tree.root = root
	db.flushed = npages
	return nil

}

func (db *DB) writeMasterPage() error {
	var data [32]byte
	copy(data[0:], sig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.flushed)
	_, err := db.file.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

func (db *DB) flushPages() error {
	npages := int(db.flushed) + len(db.dirty)
	if err := db.extend(npages); err != nil {
		return err
	}

	for i, page := range db.dirty {
		ptr := db.flushed + uint64(i)
		copy(db.getPage(ptr), page)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("fsync dirty pages: %w", err)
	}
	db.flushed += uint64(len(db.dirty))
	db.dirty = db.dirty[:0]

	// write the master page
	if err := db.writeMasterPage(); err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("fsync master page: %w", err)
	}
	return nil

}
