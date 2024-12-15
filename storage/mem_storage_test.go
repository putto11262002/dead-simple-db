package storage

import (
	"bytes"
	"testing"
)

func TestNewMemStorage(t *testing.T) {
	pageSize := 1024
	storage := NewMemStorage(pageSize)
	if storage.PageSize() != pageSize {
		t.Errorf("expected page size %d, got %d", pageSize, storage.PageSize())
	}
}

func TestMemStorage_Get(t *testing.T) {
	storage := NewMemStorage(1024)
	pageData := []byte("test page data")
	addr := storage.New(pageData)

	t.Run("get existing page", func(t *testing.T) {
		got := storage.Get(addr)
		if !bytes.Equal(pageData, got) {
			t.Errorf("expected page data %q, got %q", pageData, got)
		}
	})

	t.Run("get non-existing page should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic but did not panic")
			}
		}()
		_ = storage.Get(69) // non-existent address
	})
}

func TestMemStorage_New(t *testing.T) {
	storage := NewMemStorage(10)
	pageData := []byte("short")

	t.Run("new page within size limit", func(t *testing.T) {
		addr := storage.New(pageData)
		if !bytes.Equal(pageData, storage.Get(addr)) {
			t.Errorf("expected page data %q, got %q", pageData, storage.Get(addr))
		}
	})

	t.Run("new page exceeding size limit should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic but did not panic")
			}
		}()
		storage.New([]byte("this page is too large"))
	})
}

func TestMemStorage_Del(t *testing.T) {
	storage := NewMemStorage(1024)
	pageData := []byte("test page")
	addr := storage.New(pageData)

	t.Run("delete existing page", func(t *testing.T) {
		storage.Del(addr)
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic when accessing deleted page but did not panic")
			}
		}()
		_ = storage.Get(addr) // This should panic as the page was deleted
	})

	t.Run("delete non-existing page should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic but did not panic")
			}
		}()
		storage.Del(69) // non-existent address
	})
}
