package deadsimpledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

var (
	sig = []byte("dead simple db \000")
)

func init() {
	assert(len(sig) == 16, "invalid signature length")
}

type KV struct {
	file   *os.File
	tree   *Btree
	path   string
	pager  Pager
	logger *slog.Logger
}

func NewKV(path string) *KV {
	return &KV{
		path:   path,
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

}

func (db *KV) Open() error {
	fail := func(err error) error {
		if db.pager != nil {
			db.pager.close()
		}
		db.Close()
		return err
	}

	f, err := os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fail(fmt.Errorf("os.OpenFile: %w", err))
	}
	db.file = f
	header, err := db.loadMasterPage()
	if err != nil {
		return fail(fmt.Errorf("reading header: %w", err))
	}

	db.pager, err = newMmapPagerWithFreeList(db.file, header.flushed, header.freeList)
	if err != nil {
		return fail(fmt.Errorf("initializing pager: %w", err))
	}

	db.tree = newBtree(header.root, db.pager)

	return nil

}

func (db *KV) Close() error {
	if db.pager != nil {
		if err := db.pager.close(); err != nil {
			db.logger.Error(fmt.Sprintf("closing pager: %v", err))
		}
	}
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, value []byte) error {
	db.tree.Insert(key, value)
	return db.flush()
}

func (kv *KV) Update(key []byte, val []byte, mode InsertMode) (bool, error) {
	res := kv.tree.InsertEx(key, val, mode)
	var ok bool
	if mode == Insert {
		ok = res.Inserted
	}
	if mode == Update {
		ok = res.Updated
	}
	if mode == Upsert {
		ok = res.Inserted || res.Updated
	}
	return ok, kv.flush()
}

func (db *KV) Del(key []byte) (bool, error) {
	ok := db.tree.Delete(key)
	if !ok {
		return false, nil
	}
	return ok, db.flush()
}

type Header struct {
	flushed  uint64
	root     uint64
	freeList uint64
}

var defaultHeader Header = Header{
	flushed:  1,
	root:     0,
	freeList: 0,
}

func (db *KV) loadMasterPage() (Header, error) {
	stat, err := db.file.Stat()
	if err != nil {
		return defaultHeader, fmt.Errorf("os.File.Stat: %w", err)
	}
	fileSize := int(stat.Size())
	if fileSize == 0 {
		// if it is an empty file no-op
		return defaultHeader, nil
	}

	page := make([]byte, PageSize)
	n, err := db.file.ReadAt(page, 0)
	if err != nil {
		return defaultHeader, err
	}
	assert(n == PageSize, "invalid master page size")

	_sig := page[0:16]
	root := binary.LittleEndian.Uint64(page[16:])
	npages := binary.LittleEndian.Uint64(page[24:])
	freeListHead := binary.LittleEndian.Uint64(page[32:])

	if !bytes.Equal(sig, _sig[:len(sig)]) {
		return defaultHeader, errors.New("invalid signature")
	}

	if freeListHead < 0 || freeListHead >= npages {
		return defaultHeader, errors.New("invalid free list head")
	}

	bad := (npages < 1) || (npages > uint64(fileSize/PageSize)) || (root < 0) || (root >= npages)
	if bad {
		return defaultHeader, errors.New("invalid master page")
	}
	return Header{
		root:     root,
		freeList: freeListHead,
		flushed:  npages,
	}, nil
}

func (db *KV) writeMasterPage(header Header) error {
	data := make([]byte, PageSize)
	copy(data[0:], sig)
	binary.LittleEndian.PutUint64(data[16:], header.root)
	binary.LittleEndian.PutUint64(data[24:], header.flushed)
	binary.LittleEndian.PutUint64(data[32:], header.freeList)

	_, err := db.file.WriteAt(data, 0)
	if err != nil {
		return err
	}
	return nil
}

func (db *KV) flush() error {
	pagerMetadata, err := db.pager.flush()
	if err != nil {
		return fmt.Errorf("flushing pager: %w", err)
	}

	// write the master page
	if err := db.writeMasterPage(Header{
		root:     db.tree.root,
		flushed:  pagerMetadata.flushed,
		freeList: pagerMetadata.freeListHead,
	}); err != nil {
		return fmt.Errorf("write master page: %w", err)
	}

	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("fsync master page: %w", err)
	}

	return nil
}
