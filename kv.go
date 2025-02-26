package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"

	"github.com/google/btree"
)

var (
	sig = []byte("deadsimpledb")
)

type KV struct {
	// flushed is the number of pages that are flushed to disk
	flushed uint64
	// nfree is the number of pages token from the free list
	nfree int
	// updates are newly allocated pages or deallocated pages keyed by the pointer.
	// A nil value means the page is deallocated.
	file     *os.File
	fileSize int
	mmapSize int
	// mmaps is a list of all the mmaped regions.
	// The is of each mmaped regions are multiples of PageSize.
	mmaps [][]byte
	tree  *Btree
	path  string

	// appended is a list of newly allocated pages that are not yet appended to the file
	appended *btree.BTree

	freeList *freeList
	logger   *slog.Logger
}

func NewKV(path string) *KV {
	return &KV{
		path:     path,
		tree:     &Btree{},
		freeList: newFreeList(),
		appended: btree.New(6),
		logger:   slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

}

func (db *KV) Open() error {
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
	db.freeList.page.get = func(u uint64) freeListNode {
		page := db.getPage(u)
		return freeListNode{page}
	}
	db.freeList.page.allocatae = func(fln freeListNode) uint64 {
		return db.appendPage(fln.data)
	}
	db.freeList.page.write = func(u uint64, fln freeListNode) {
		db.writePage(u, fln.data)
	}

	if db.fileSize != 0 {
		if err := db.loadMasterPage(); err != nil {
			return fail(err)
		}
	} else {
		db.flushed = 1
	}

	return nil

}

func (db *KV) Close() error {
	for _, mmap := range db.mmaps {
		err := syscall.Munmap(mmap)
		if err != nil {
			return fmt.Errorf("syscall.Munmap: %w", err)
		}
	}
	return db.file.Close()
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, value []byte) error {
	db.tree.Insert(key, value)
	return db.flushPages()
}

func (db *KV) Del(key []byte) (bool, error) {
	ok := db.tree.Delete(key)
	if !ok {
		return false, nil
	}
	return ok, db.flushPages()
}

func (db *KV) initMmap() error {
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

func (db *KV) extendMmap(npages int) error {
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

func (db *KV) extendFile(npages int) error {
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

func (db *KV) extend() error {
	npages := int(db.flushed) + db.appended.Len()
	if err := db.extendFile(npages); err != nil {
		return fmt.Errorf("growing file: %w", err)
	}
	if err := db.extendMmap(npages); err != nil {
		return fmt.Errorf("growing mmap: %w", err)
	}
	return nil
}

func (db *KV) getPage(ptr uint64) []byte {
	assert(ptr > 0 && ptr < db.flushed+uint64(db.appended.Len()), "invalid pt: %x", ptr)

	if ptr >= db.flushed {
		p := db.appended.Get(newPage(ptr, nil))
		assert(p != nil, "appended cache corrupted")
		return p.(*page).content
	}
	return db.mmapGetPage(ptr)
}

func (db *KV) mmapGetPage(ptr uint64) []byte {
	start := uint64(0)
	for _, mmap := range db.mmaps {
		end := start + uint64(len(mmap)/PageSize)
		if ptr < end {
			offset := (ptr - start) * uint64(PageSize)
			return mmap[offset : offset+uint64(PageSize)]
		}
		start = end
	}
	panic(fmt.Sprintf("invalid ptr: %x", ptr))

}

func (db *KV) allocatePage(page []byte) uint64 {
	assert(len(page) <= PageSize, "page data is larger than PageSize")
	ptr := uint64(0)
	if db.freeList.freeCount() > 0 {
		var ok bool
		ptr, ok = db.freeList.pop()
		assert(ok, "free list corrupted")
		db.writePage(ptr, page)
		db.logger.Debug(fmt.Sprintf("reused page: %x", ptr))
	} else {
		ptr = db.appendPage(page)
		db.logger.Debug(fmt.Sprintf("appended page: %x", ptr))
	}
	return ptr
}

func (db *KV) freePage(ptr uint64) {
	assert(ptr > 0 && ptr < db.flushed, "invalid ptr")
	db.logger.Debug("freeing page", slog.Any("ptr", ptr))
	db.freeList.Free(ptr)
}

type page struct {
	content []byte
	ptr     uint64
}

func newPage(ptr uint64, content []byte) *page {
	return &page{content, ptr}
}

func (p page) Less(o btree.Item) bool {
	return p.ptr < o.(*page).ptr
}

func (db *KV) appendPage(page []byte) uint64 {
	assert(len(page) <= PageSize, "page data is larger than PageSize")
	ptr := db.flushed + uint64(db.appended.Len())
	db.appended.ReplaceOrInsert(newPage(ptr, page))
	return ptr
}

func (db *KV) writePage(ptr uint64, page []byte) {
	assert(len(page) <= PageSize, "page data is larger than PageSize")
	assert(ptr > 0 && ptr < db.flushed+uint64(db.appended.Len()), "invalid ptr")
	if ptr < db.flushed {
		_page := db.getPage(ptr)
		copy(_page, page)
	} else {
		db.appended.ReplaceOrInsert(newPage(ptr, page))
	}
}

func (db *KV) loadMasterPage() error {
	data := db.mmaps[0]
	_sig := data[0:16]
	root := binary.LittleEndian.Uint64(data[16:])
	npages := binary.LittleEndian.Uint64(data[24:])
	freeListHead := binary.LittleEndian.Uint64(data[32:])

	if !bytes.Equal(sig, _sig[:len(sig)]) {
		return errors.New("invalid signature")
	}

	bad := (npages < 1) || (npages > uint64(db.fileSize/PageSize)) || (root < 0) || (root >= npages)
	if bad {
		return errors.New("invalid master page")
	}
	db.flushed = npages
	db.tree.root = root
	db.freeList.read(freeListHead)
	return nil

}

func (db *KV) writeMasterPage() error {
	data := make([]byte, PageSize)
	copy(data[0:], sig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	fmt.Printf("writing: db.flushed: %d\n", db.flushed)
	binary.LittleEndian.PutUint64(data[24:], db.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.freeList.head)

	_, err := db.file.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

func (db *KV) flushPages() error {
	db.logger.Debug("flushing pages")
	db.freeList.write()

	if err := db.extend(); err != nil {
		return err
	}

	db.appended.Ascend(func(item btree.Item) bool {
		p := item.(*page)
		db.logger.Debug("appending page to file", slog.Any("ptr", p.ptr))

		// Get the actual mmap page and copy data to it
		mmapPage := db.mmapGetPage(p.ptr)
		copy(mmapPage, p.content)
		return true
	})

	// Write the appended pages directly to the mmapped region
	// for i, page := range {
	// 	ptr := db.flushed + uint64(i)
	// 	db.logger.Debug("appending page to file", slog.Any("ptr", ptr))
	//
	// 	// Get the actual mmap page and copy data to it
	// 	mmapPage := db.mmapGetPage(ptr)
	// 	n := copy(mmapPage, page)
	// 	assert(n == PageSize, "failed writing to page")
	//
	// 	fmt.Printf("ptr %d content: %v\n", ptr, mmapPage[:10])
	// }

	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("fsync dirty pages: %w", err)
	}

	db.flushed += uint64(db.appended.Len())
	db.appended.Clear(true)

	// write the master page
	if err := db.writeMasterPage(); err != nil {
		return fmt.Errorf("write master page: %w", err)
	}

	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("fsync master page: %w", err)
	}

	return nil
}
