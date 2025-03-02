package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "store"

// Content Addressable Storage (CAS) is a method of storing data
// such that the key is derived from the content itself.
func CASTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashString := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLen := len(hashString) / blockSize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		start := i * blockSize
		end := start + blockSize
		paths[i] = hashString[start:end]
	}

	// return strings.Join(paths, "/")
	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashString,
	}
}

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) > 0 {
		return paths[0]
	}
	return ""
}

// PathTransformFunc is a function that transforms a key into a path.
type PathTransformFunc func(string) PathKey

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreConfig struct {
	// Root is the root folder where the store will be created
	Root          string
	PathTransform PathTransformFunc
}

type Store struct {
	StoreConfig
}

func NewStore(opts StoreConfig) *Store {
	if opts.PathTransform == nil {
		opts.PathTransform = DefaultPathTransformFunc
	}
	if opts.Root == "" {
		opts.Root = defaultRootFolderName
	}
	return &Store{
		StoreConfig: opts,
	}
}

func (s *Store) Has(key string) bool {
	pathKey := s.PathTransform(key)
	_, err := os.Stat(pathKey.FullPath())
	if err == fs.ErrNotExist {
		return false
	}
	return true
}

// Delete deletes a file from the store
func (s *Store) Delete(key string) error {
	pathKey := s.PathTransform(key)
	defer func() {
		log.Println("Deleted", pathKey.Filename, "from disc")
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)
}

// Read reads the content of a file from the store
func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	return buf, err
}

// readStream reads the content of a file from the store
func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransform(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	return os.Open(fullPathWithRoot)
}

func (s *Store) writeStream(key string, r io.Reader) error {
	pathKey := s.PathTransform(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return err
	}

	fullPath := pathKey.FullPath()
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, fullPath)

	f, err := os.Create(fullPathWithRoot)
	if err != nil {
		return err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	log.Println("Wrote", n, "bytes to", fullPathWithRoot)

	return nil
}
