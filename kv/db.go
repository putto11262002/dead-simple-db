package kv

import (
	"os"
	"path"

	"example.com/db/btree"
	"example.com/db/storage"
)

type DB struct {
	s        storage.Storage
	btree    *btree.BTree
	pageSize int
}

type SAdapter struct {
	s storage.Storage
}

func (sa *SAdapter) Get(addr uint64) btree.BNode {
	b := sa.s.Get(addr)
	return btree.NewBNode(b)
}

func (sa *SAdapter) New(node btree.BNode) uint64 {
	b := node.Bytes()
	return sa.s.New(b)
}

func (sa *SAdapter) Del(addr uint64) {
	sa.s.Del(addr)
}

func (db *DB) Open(p string) error {
	pageSize := os.Getpagesize()
	dir := path.Dir(p)
	fs := os.DirFS(dir)
	s := storage.NewMmapStorage(pageSize, fs, &storage.SyscallMemoryMapper{})
	if err := s.Open(p); err != nil {
		return err
	}
	sa := &SAdapter{s}
	db.s = s
	db.btree = btree.NewBtree(s.Root(), uint16(pageSize), sa)
	return nil
}

func (db *DB) Close() error {
	return db.s.Close()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.btree.Get(key)
}

func (db *DB) Set(key, value []byte) error {
	db.btree.Insert(key, value)
	db.s.SetRoot(db.btree.Root)
	return db.s.Flush()
}

func (db *DB) Del(key []byte) (bool, error) {
	if err := db.btree.Delete(key); err != nil {
		return false, err
	}
	if err := db.s.Flush(); err != nil {
		return false, err
	}
	return true, nil
}
