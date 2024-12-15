package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
)

const SIG = "d65351918f3670e5"

type File interface {
	io.Reader
	io.WriterAt
	io.Closer
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
	Fd() uintptr
}

type MemoryMapper interface {
	Map(fd uintptr, offset int64, length int, prot int, flags int) ([]byte, error)
	Unmap(b []byte) error
}

type SyscallMemoryMapper struct{}

func (m SyscallMemoryMapper) Map(fd uintptr, offset int64, length int, prot int, flags int) ([]byte, error) {
	return syscall.Mmap(int(fd), offset, length, prot, flags)
}

func (m SyscallMemoryMapper) Unmap(b []byte) error {
	return syscall.Munmap(b)
}

type MmapStorage struct {
	// address of the Root node
	root     uint64
	pageSize int
	file     File
	// The size of the underlying file.
	fileSize  int
	mmapSize  int
	mapChunks [][]byte
	// The pages that has been allocated but hasn't been flushed to disk.
	tempPages [][]byte
	// The number of pages that have been flushed to disk.
	nFlushed int
	// The number of pages that have been allocated.

	fs     fs.FS
	mapper MemoryMapper
}

func NewMmapStorage(pageSize int, fs fs.FS, mapper MemoryMapper) *MmapStorage {
	return &MmapStorage{
		pageSize: pageSize,
		fs:       fs,
		mapper:   mapper,
	}
}

func (s *MmapStorage) SetRoot(addr uint64) {
	s.root = addr
}

func (s *MmapStorage) Open(path string) (err error) {

	defer func() {
		if err != nil {
			if _err := s.Close(); _err != nil {
				err = fmt.Errorf("MmapStorage.Close: %w: %w", err, _err)
			}
		}
	}()

	if err = s.openFile(path); err != nil {
		return fmt.Errorf("openFile: %v", err)
	}

	// create intial mmap
	err = s.createMmap()
	if err != nil {
		return fmt.Errorf("initMmap: %v", err)
	}

	// load metadata
	err = s.loadMaster()
	if err != nil {
		return fmt.Errorf("loadMaster: %v", err)
	}

	return nil
}

func (s *MmapStorage) Close() (err error) {
	// Unmap the chunks
	for _, chunk := range s.mapChunks {
		err = syscall.Munmap(chunk)
		if err != nil {
			return fmt.Errorf("Munmap: %w", err)
		}
	}
	if err = s.file.Close(); err != nil {
		return fmt.Errorf("file.Close: %w", err)
	}
	return nil

}

func (s *MmapStorage) Flush() error {
	if err := s.writePages(); err != nil {
		return fmt.Errorf("writePages: %w", err)
	}
	return s.syncPages()

}

func (s *MmapStorage) Get(addr uint64) []byte {
	return s.getPage(addr)
}

func (s *MmapStorage) New(data []byte) uint64 {
	return s.newPage(data)
}

func (s *MmapStorage) Del(addr uint64) {
	s.delPage(addr)
}

func (s *MmapStorage) Root() uint64 {
	return s.root
}

func (s *MmapStorage) writePages() error {
	// extend file if needed
	if err := s.extendFile(); err != nil {
		return fmt.Errorf("extendFile: %w", err)
	}

	// extend mmap if needed
	if err := s.extendMmap(); err != nil {
		return fmt.Errorf("extendMmap: %w", err)
	}

	// copy data from temp buffer to mapped memory
	for i, page := range s.tempPages {
		ptr := uint64(s.nFlushed + i)
		copy(s.getPage(ptr), page)
	}
	return nil
}

// syncPages flushes data to disk then updates the master page.
func (s *MmapStorage) syncPages() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("Sync: %w", err)
	}

	s.nFlushed += len(s.tempPages)
	s.tempPages = s.tempPages[:0]

	if err := s.writeMaster(); err != nil {
		return fmt.Errorf("writeMaster: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("Sync: %w", err)
	}
	return nil
}

// getPage retreives the page at the given pointer from the mapped file.
func (s *MmapStorage) getPage(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range s.mapChunks {
		// the end of the current chunk
		end := start + uint64(len(chunk))/uint64(s.pageSize)
		// no need to check for ptr > start because it is guaranteed by the loop invariant
		if ptr < end {
			// ptr is in the current chunk
			offset := uint64(s.pageSize) * (ptr - start)
			return chunk[offset : offset+uint64(s.pageSize)]

		}
		start = end

	}
	panic("invalid ptr")
}

// newPage allocates a new page and returns the pointer to it.
// It does not write the data to disk.
func (s *MmapStorage) newPage(data []byte) uint64 {
	if len(data) > s.pageSize {
		panic("data is too large")
	}
	// TODO: reuse deallocated pages
	ptr := uint64(s.nFlushed + len(s.tempPages))
	s.tempPages = append(s.tempPages, data)
	return ptr
}

func (s *MmapStorage) delPage(ptr uint64) {
	// TODO: implement it
	fmt.Println("delPage: not implemented")

}

func (s *MmapStorage) extendFile() error {
	nPages := s.nFlushed + len(s.tempPages)
	filePages := s.fileSize / s.pageSize
	if filePages > nPages {
		return nil
	}

	for filePages < nPages {
		inc := filePages / 4
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	newFileSize := filePages * s.pageSize
	if err := s.file.Truncate(int64(newFileSize)); err != nil {
		return fmt.Errorf("Truncate: %w", err)
	}
	s.fileSize = newFileSize

	return nil
}

// loadMaster reads the metadata from the master page and populates the fields of the storage.
//
// The master page is the first page of the file.
// it contains the following information:
// - the 16-byte signature
// - the pointer to the root node
// - the number of pages used
//
// Format:
// | sig | btree_root | page_used
// | 16B  | 8B         | 8B
func (s *MmapStorage) loadMaster() error {
	// create master page if it does not exist
	if s.fileSize == 0 {
		s.nFlushed = 1
		return nil
	}

	data := s.mapChunks[0]

	sig, root, used, err := readMaster(data)
	if err != nil {
		return fmt.Errorf("readMaster: %w", err)
	}

	fpages := uint64(s.fileSize / s.pageSize)
	if err := validateMaster(sig, root, used, fpages); err != nil {
		return fmt.Errorf("validateMaster: %w", err)
	}

	s.nFlushed = int(used)
	s.root = root
	return nil
}

func readMaster(b []byte) (sig []byte, root uint64, used uint64, err error) {
	if len(b) < 32 {
		return nil, 0, 0, fmt.Errorf("invalid master page size")
	}
	sig = b[:16]
	root = binary.LittleEndian.Uint64(b[16:])
	used = binary.LittleEndian.Uint64(b[24:])

	return sig, root, used, nil
}

func validateMaster(sig []byte, root, used uint64, fpages uint64) error {
	if !bytes.Equal(sig, []byte(SIG)) {
		return fmt.Errorf("invalid signature")
	}

	if used < 1 || used > fpages {
		return fmt.Errorf("invalid number of pages used")
	}

	if root < 0 || root >= used {
		return fmt.Errorf("invalid root pointer")
	}

	return nil
}

// writeMaster writes the metadata to the master page.
// This operation is atomic because writes that do not cross page boundaries are atomic.
func (s *MmapStorage) writeMaster() error {
	var data [32]byte
	copy(data[:16], []byte(SIG))
	binary.LittleEndian.PutUint64(data[16:], s.root)
	binary.LittleEndian.PutUint64(data[24:], uint64(s.nFlushed))
	if _, err := s.file.WriteAt(data[:], 0); err != nil {
		return fmt.Errorf("WriteAt: %w", err)
	}
	return nil
}

// createMmap create an initial mmap of the file.
// The mapping region is at least the size of the file, and is a multiple of the page size.
// If the file size is not a multiple of the page size, an error is returned.
func (s *MmapStorage) createMmap() error {
	if s.fileSize%s.pageSize != 0 {
		return fmt.Errorf("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20

	for mmapSize < s.fileSize {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(
		int(s.file.Fd()),
		0,
		mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	s.mapChunks = append(s.mapChunks, chunk)
	s.mmapSize = mmapSize

	if err != nil {
		return fmt.Errorf("Mmap: %w", err)
	}
	return nil
}

// extendMmap extend mmapby adding new mapping. It doubles the size of address space of the current mapping.
//
// This approach is used instead of using syscall.Mremap because
// it does not guarantee that the startting address of the mapping will remain the same
// when extending range by remapping.
func (s *MmapStorage) extendMmap() error {
	nPages := s.nFlushed + len(s.tempPages)
	if s.mmapSize >= nPages*s.pageSize {
		return nil
	}

	// double the adress space
	chunk, err := syscall.Mmap(
		int(s.file.Fd()),
		int64(s.mmapSize),
		s.mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("Mmap: %w", err)
	}

	s.mmapSize += s.mmapSize
	s.mapChunks = append(s.mapChunks, chunk)

	return nil
}

func (s *MmapStorage) openFile(path string) error {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %v", err)
	}
	s.file = file

	fstate, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("Stat: %w", err)
	}

	s.fileSize = int(fstate.Size())

	return nil

}
