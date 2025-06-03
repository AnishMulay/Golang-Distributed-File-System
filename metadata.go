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

	// Determine if this is a directory
	isDir := fileType == FileTypeDirectory || mode&os.ModeDir != 0

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
			IsDir:      isDir,
			Children:   []string{},
		}
		
		// Update parent's children list for new entries
		parent := filepath.Dir(path)
		if parent != path && parent != "." && parent != "/" {
			if parentMeta, ok := ps.pathToMeta[parent]; ok && parentMeta.IsDir {
				baseName := filepath.Base(path)
				// Check if it's already in the children list
				found := false
				for _, child := range parentMeta.Children {
					if child == baseName {
						found = true
						break
					}
				}
				if !found {
					parentMeta.Children = append(parentMeta.Children, baseName)
					ps.saveMetadata(parent)
				}
			}
		}
	} else {
		meta.ContentKey = contentKey
		meta.Size = size
		meta.Mode = mode
		meta.ModTime = now
		meta.Type = fileType
		meta.IsDir = isDir
	}

	ps.pathToMeta[path] = meta
	return ps.saveMetadata(path)
}

// Delete deletes metadata for a path
func (ps *PathStore) Delete(path string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	meta, ok := ps.pathToMeta[path]
	if !ok {
		return os.ErrNotExist
	}
	
	// If it's a directory, check if it's empty
	if meta.IsDir {
		if len(meta.Children) > 0 {
			return fmt.Errorf("cannot delete directory %s: directory not empty", path)
		}
	}
	
	// Update parent's children list
	parent := filepath.Dir(path)
	if parent != path && parent != "." && parent != "/" {
		if parentMeta, ok := ps.pathToMeta[parent]; ok {
			baseName := filepath.Base(path)
			newChildren := make([]string, 0, len(parentMeta.Children))
			for _, child := range parentMeta.Children {
				if child != baseName {
					newChildren = append(newChildren, child)
				}
			}
			parentMeta.Children = newChildren
			ps.saveMetadata(parent)
		}
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
	meta, ok := ps.pathToMeta[dirPath]
	if !ok {
		return nil, os.ErrNotExist
	}
	
	if !meta.IsDir {
		return nil, fmt.Errorf("%s is not a directory", dirPath)
	}
	
	// Use the Children list for more efficient directory listing
	var entries []FileMetadata
	for _, childName := range meta.Children {
		childPath := filepath.Join(dirPath, childName)
		childMeta, exists := ps.pathToMeta[childPath]
		
		if exists {
			if filter == nil || filter(childMeta) {
				entries = append(entries, *childMeta)
			}
		} else {
			// Child in list but not in metadata - remove from children
			log.Printf("Warning: Child %s listed in directory %s but metadata not found", childName, dirPath)
			// We can't modify the children list here because we're holding a read lock
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
		if ps.pathToMeta[path].IsDir {
			return nil // Directory already exists, return success
		}
		return os.ErrExist
	}

	// Ensure parent exists
	parent := filepath.Dir(path)
	if parent != "." && parent != "/" && parent != path {
		parentMeta, exists := ps.pathToMeta[parent]
		if !exists {
			return fmt.Errorf("parent directory %s does not exist", parent)
		}
		if !parentMeta.IsDir {
			return fmt.Errorf("parent path %s exists but is not a directory", parent)
		}
	}

	// Create metadata for the directory
	now := time.Now()
	meta := &FileMetadata{
		Path:       path,
		ContentKey: "", // Empty for directories
		Size:       0,  // 0 for directories
		Mode:       mode | os.ModeDir,
		ModTime:    now,
		CreateTime: now,
		AccessTime: now,
		Type:       FileTypeDirectory,
		IsDir:      true,
		IsHidden:   isHidden,
		Owner:      "default", // TODO: from user context
		Group:      "default", // TODO: from user context
		Children:   []string{},
	}
	ps.pathToMeta[path] = meta

	// Update parent's children list
	if parent != "." && parent != "/" && parent != path {
		ps.pathToMeta[parent].Children = append(ps.pathToMeta[parent].Children, filepath.Base(path))
		// Save parent metadata as well
		ps.saveMetadata(parent)
	}

	return ps.saveMetadata(path)
}

func (ps *PathStore) CreateDirRecursive(path string, mode os.FileMode, isHidden bool) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	
	path = filepath.Clean(path)
	
	// If path already exists as a directory, return success
	if meta, exists := ps.pathToMeta[path]; exists && meta.IsDir {
		return nil
	}
	
	// Handle absolute paths correctly
	parts := strings.Split(path, string(filepath.Separator))
	current := ""
	
	// For absolute paths, start with "/"
	if path[0] == filepath.Separator {
		current = string(filepath.Separator)
	}
	
	for _, part := range parts {
		if part == "" {
			continue
		}
		
		prev := current
		if current == "/" {
			current = "/" + part
		} else {
			current = filepath.Join(current, part)
		}
		
		// Check if this path component exists
		meta, exists := ps.pathToMeta[current]
		
		if !exists {
			// Create this directory component
			now := time.Now()
			newMeta := &FileMetadata{
				Path:       current,
				ContentKey: "",
				Size:       0,
				Mode:       mode | os.ModeDir,
				ModTime:    now,
				CreateTime: now,
				AccessTime: now,
				Type:       FileTypeDirectory,
				IsDir:      true,
				IsHidden:   isHidden,
				Owner:      "default",
				Group:      "default",
				Children:   []string{},
			}
			ps.pathToMeta[current] = newMeta
			
			// Update parent's children list
			if prev != "" && prev != current {
				if parentMeta, ok := ps.pathToMeta[prev]; ok {
					parentMeta.Children = append(parentMeta.Children, part)
					ps.saveMetadata(prev)
				}
			}
			
			// Save this directory's metadata
			ps.saveMetadata(current)
		} else if !meta.IsDir {
			// Path exists but is not a directory
			return fmt.Errorf("path component %s exists but is not a directory", current)
		}
	}
	
	return nil
}
