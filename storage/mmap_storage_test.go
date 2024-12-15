package storage

import (
	"os"
	"testing"
)

var pageSize = 4096

func testOpenFile(t *testing.T, path string) {

	s := &MmapStorage{}

	if err := s.openFile(path); err != nil {
		t.Fatalf("open file: %v", err)
	}

	if s.file == nil {
		t.Errorf("file is nil")
	}

	stat, err := s.file.Stat()
	if err != nil {
		t.Fatalf("file stat: %v", err)
	}

	if int64(s.fileSize) != stat.Size() {
		t.Errorf("file size: got %d, want %d", s.fileSize, stat.Size())
	}

	s.Close()
}

func TestMmapStorage_openFile(t *testing.T) {

	t.Run("create file if not exists", func(t *testing.T) {

		path := t.TempDir() + "/test.db"

		testOpenFile(t, path)

		// check if file is created
		stat, err := os.Stat(path)
		if err != nil {
			t.Fatalf("os.Stat: %v", err)
		}

		if os.ErrNotExist == err {
			t.Errorf("file not created")
		}

		if stat.Mode() != 0644 {
			t.Errorf("file mode: got %d, want %d", stat.Mode(), 0644)
		}

	})

	t.Run("open existing file", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatalf("open file: %v", err)
		}
		file.Close()

		testOpenFile(t, path)

	})

}

func TestMmapStorage_createMmap(t *testing.T) {

	path := t.TempDir() + "/test.db"
	fileSize := pageSize * 2

	//  Create a pre-defined size file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer file.Close()
	file.Truncate(int64(fileSize))

	s := &MmapStorage{pageSize: pageSize, fileSize: fileSize, file: file}

	if err := s.createMmap(); err != nil {
		t.Fatalf("create mapping: %v", err)
	}

	if s.mmapSize < fileSize {
		t.Errorf("mmap size: got %d, want >= %d", s.mmapSize, fileSize)
	}

	if len(s.mapChunks) != 1 {
		t.Errorf("mmap chunks: got %d, want %d", len(s.mapChunks), 1)
	}

}
