// metadata.go

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileType represents the type of file
type FileType int

const (
	FileTypeRegular FileType = iota
	FileTypeDirectory
	FileTypeSymlink
)

// metadata.go
type FileMetadata struct {
	Path       string      `json:"path"`
	ContentKey string      `json:"contentKey"` // Empty for directories
	Size       int64       `json:"size"`       // 0 for directories
	Mode       os.FileMode `json:"mode"`
	ModTime    time.Time   `json:"modTime"`
	CreateTime time.Time   `json:"createTime"`
	AccessTime time.Time   `json:"accessTime"`
	Type       FileType    `json:"type"`
	Owner      string      `json:"owner"`
	Group      string      `json:"group"`
	IsDir      bool        `json:"isDir"`    // Explicitly mark directories
	IsHidden   bool        `json:"isHidden"` // For hidden/system dirs
	Children   []string    `json:"children"` // For directory listing
}

// PathStore manages file path to content key mapping and metadata
type PathStore struct {
	mutex      sync.RWMutex
	rootDir    string
	pathToMeta map[string]*FileMetadata
}

// NewPathStore creates a new PathStore
func NewPathStore(rootDir string) (*PathStore, error) {
	ps := &PathStore{
		rootDir:    rootDir,
		pathToMeta: make(map[string]*FileMetadata),
	}

	// Create metadata directory if it doesn't exist
	metaDir := filepath.Join(rootDir, "metadata")
	if err := os.MkdirAll(metaDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating metadata directory: %w", err)
	}

	// Load existing metadata
	if err := ps.loadMetadata(); err != nil {
		return nil, fmt.Errorf("loading metadata: %w", err)
	}

	return ps, nil
}

// loadMetadata loads metadata from disk
func (ps *PathStore) loadMetadata() error {
	metaDir := filepath.Join(ps.rootDir, "metadata")

	// Read metadata directory
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Load metadata files
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		metaPath := filepath.Join(metaDir, entry.Name())
		data, err := os.ReadFile(metaPath)
		if err != nil {
			return err
		}

		var meta FileMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return err
		}

		ps.pathToMeta[meta.Path] = &meta
	}

	log.Printf("Loaded %d metadata entries from %s", len(ps.pathToMeta), metaDir)
	return nil
}

// saveMetadata saves metadata to disk
func (ps *PathStore) saveMetadata(path string) error {
	meta, ok := ps.pathToMeta[path]
	if !ok {
		return os.ErrNotExist
	}

	metaDir := filepath.Join(ps.rootDir, "metadata")
	metaPath := filepath.Join(metaDir, hashKey(path)+".json")

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metaPath, data, 0644)
}

// Get gets metadata for a path
func (ps *PathStore) Get(path string) (*FileMetadata, error) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	path = filepath.Clean(path)

	meta, ok := ps.pathToMeta[path]
	if !ok {
		return nil, os.ErrNotExist
	}

	return meta, nil
}

// Set sets or updates metadata for a path
func (ps *PathStore) Set(path string, contentKey string, size int64, mode os.FileMode, fileType FileType) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	path = filepath.Clean(path)

	now := time.Now()
	meta, exists := ps.pathToMeta[path]

	if !exists {
		meta = &FileMetadata{
			Path:       path,
			ContentKey: contentKey,
			Size:       size,
			Mode:       mode,
			CreateTime: now,
			ModTime:    now,
			AccessTime: now,
			Type:       fileType,
			Owner:      "default", // Would normally come from user context
			Group:      "default", // Would normally come from user context
		}
	} else {
		meta.ContentKey = contentKey
		meta.Size = size
		meta.Mode = mode
		meta.ModTime = now
		meta.Type = fileType
	}

	ps.pathToMeta[path] = meta
	return ps.saveMetadata(path)
}

// Delete deletes metadata for a path
func (ps *PathStore) Delete(path string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if _, ok := ps.pathToMeta[path]; !ok {
		return os.ErrNotExist
	}

	metaDir := filepath.Join(ps.rootDir, "metadata")
	metaPath := filepath.Join(metaDir, hashKey(path)+".json")

	delete(ps.pathToMeta, path)

	// Delete the file, but ignore if it doesn't exist
	err := os.Remove(metaPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Exists checks if a path exists
func (ps *PathStore) Exists(path string) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	_, ok := ps.pathToMeta[path]
	return ok
}

// ListDir lists entries in a directory
// metadata.go
func (ps *PathStore) ListDir(dirPath string, filter func(*FileMetadata) bool) ([]FileMetadata, error) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	dirPath = filepath.Clean(dirPath)
	if meta, ok := ps.pathToMeta[dirPath]; !ok || !meta.IsDir {
		return nil, os.ErrNotExist
	}

	var entries []FileMetadata
	for path, meta := range ps.pathToMeta {
		if filepath.Dir(path) == dirPath {
			if filter == nil || filter(meta) {
				entries = append(entries, *meta)
			}
		}
	}
	return entries, nil
}

// metadata.go
func (ps *PathStore) CreateDir(path string, mode os.FileMode, isHidden bool) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	path = filepath.Clean(path)
	if ps.pathToMeta[path] != nil {
		return os.ErrExist
	}

	// Ensure parent exists
	parent := filepath.Dir(path)
	if parent != "." && parent != "/" && !ps.pathToMeta[parent].IsDir {
		return os.ErrNotExist
	}

	// Create metadata for the directory
	now := time.Now()
	meta := &FileMetadata{
		Path:       path,
		Mode:       mode | os.ModeDir,
		ModTime:    now,
		CreateTime: now,
		AccessTime: now,
		IsDir:      true,
		IsHidden:   isHidden,
		Owner:      "default", // TODO: from user context
		Group:      "default", // TODO: from user context
	}
	ps.pathToMeta[path] = meta

	// Update parent's children list
	if parent != "." && parent != "/" {
		ps.pathToMeta[parent].Children = append(ps.pathToMeta[parent].Children, filepath.Base(path))
	}

	return ps.saveMetadata(path)
}

func (ps *PathStore) CreateDirRecursive(path string, mode os.FileMode, isHidden bool) error {
	path = filepath.Clean(path)
	parts := strings.Split(path, "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		if !ps.Exists(current) {
			if err := ps.CreateDir(current, mode, isHidden); err != nil {
				return err
			}
		}
	}
	return nil
}
