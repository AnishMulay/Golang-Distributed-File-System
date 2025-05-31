// fileops.go

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// File operation flags - using standard Go os package constants
const (
	O_RDONLY int = os.O_RDONLY // Open for reading only
	O_WRONLY int = os.O_WRONLY // Open for writing only
	O_RDWR   int = os.O_RDWR   // Open for reading and writing
	O_APPEND int = os.O_APPEND // Append on each write
	O_CREATE int = os.O_CREATE // Create file if it does not exist
	O_EXCL   int = os.O_EXCL   // Used with O_CREATE, file must not exist
	O_TRUNC  int = os.O_TRUNC  // Truncate size to 0
)

// OpenFile represents an open file in the distributed file system
type OpenFile struct {
	server     *FileServer
	path       string
	flags      int
	mode       os.FileMode
	mutex      sync.Mutex
	offset     int64
	closed     bool
	buffer     *strings.Builder // Buffer for write operations
	needsFlush bool
}

// NewOpenFile creates a new OpenFile instance
func NewOpenFile(server *FileServer, path string, flags int, mode os.FileMode) *OpenFile {
	return &OpenFile{
		server: server,
		path:   path,
		flags:  flags,
		mode:   mode,
		offset: 0,
		closed: false,
		buffer: &strings.Builder{},
	}
}

// Read reads from the file
func (f *OpenFile) Read(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// Check if file is readable
	if f.flags&O_RDONLY == 0 && f.flags&O_RDWR == 0 {
		return 0, os.ErrPermission
	}

	// Flush any pending writes first
	if f.needsFlush {
		if err := f.flush(); err != nil {
			return 0, err
		}
	}

	// Get the file reader
	reader, err := f.server.GetFile(f.path)
	if err != nil {
		return 0, err
	}

	// If reader supports seeking, seek to current offset
	if seeker, ok := reader.(io.Seeker); ok {
		if _, err := seeker.Seek(f.offset, io.SeekStart); err != nil {
			return 0, err
		}
	} else {
		// Otherwise, read and discard bytes until reaching offset
		if f.offset > 0 {
			_, err := io.CopyN(io.Discard, reader, f.offset)
			if err != nil {
				return 0, err
			}
		}
	}

	// Read data
	n, err = reader.Read(p)
	f.offset += int64(n)

	// Update access time
	meta, metaErr := f.server.pathStore.Get(f.path)
	if metaErr == nil {
		meta.AccessTime = time.Now()
		f.server.pathStore.Set(f.path, meta.ContentKey, meta.Size, meta.Mode, meta.Type)
	}

	return n, err
}

// Write writes to the file
func (f *OpenFile) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// Check if file is writable
	if f.flags&O_WRONLY == 0 && f.flags&O_RDWR == 0 {
		return 0, os.ErrPermission
	}

	// Handle append mode
	if f.flags&O_APPEND != 0 {
		// Get current file size
		meta, err := f.server.pathStore.Get(f.path)
		if err == nil {
			f.offset = meta.Size
		}
	}

	// Write to buffer
	n, err = f.buffer.Write(p)
	f.offset += int64(n)
	f.needsFlush = true

	return n, err
}

// flush writes buffered data to the file
func (f *OpenFile) flush() error {
	if !f.needsFlush || f.buffer.Len() == 0 {
		return nil
	}

	// Get current content
	var content string

	// If not truncating, and not at beginning, we need to merge content
	if f.flags&O_TRUNC == 0 {
		reader, err := f.server.GetFile(f.path)
		if err == nil {
			data, err := io.ReadAll(reader)
			if err == nil {
				content = string(data)
			}
		}
	}

	// Handle write based on offset
	bufferContent := f.buffer.String()
	if int64(len(content)) < f.offset-int64(len(bufferContent)) {
		// Pad with zeros if needed
		padding := strings.Repeat("\x00", int(f.offset)-len(content)-len(bufferContent))
		content = content + padding + bufferContent
	} else {
		// Replace portion of content
		prefix := ""
		if f.offset-int64(len(bufferContent)) > 0 {
			prefix = content[:f.offset-int64(len(bufferContent))]
		}
		suffix := ""
		if f.offset < int64(len(content)) {
			suffix = content[f.offset:]
		}
		content = prefix + bufferContent + suffix
	}

	// Store the file
	if err := f.server.StoreFile(f.path, strings.NewReader(content), f.mode); err != nil {
		return err
	}

	// Reset buffer
	f.buffer.Reset()
	f.needsFlush = false
	return nil
}

// Close closes the file
func (f *OpenFile) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return nil
	}

	// Flush any pending writes
	if f.needsFlush {
		if err := f.flush(); err != nil {
			return err
		}
	}

	f.closed = true
	log.Printf("Closed file %s", f.path)
	return nil
}

// Seek sets the offset for the next Read or Write
func (f *OpenFile) Seek(offset int64, whence int) (int64, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	// Flush any pending writes
	if f.needsFlush {
		if err := f.flush(); err != nil {
			return 0, err
		}
	}

	// Calculate new offset
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		// Get file size
		meta, err := f.server.pathStore.Get(f.path)
		if err != nil {
			return 0, err
		}
		newOffset = meta.Size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	// Ensure offset is not negative
	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	f.offset = newOffset
	return f.offset, nil
}
