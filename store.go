package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
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
	Root string
	// ID is the unique identifier of the store
	Id            string
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
	if len(opts.Id) == 0 {
		opts.Id = generateId()
	}
	return &Store{
		StoreConfig: opts,
	}
}

func (s *Store) Has(key string) bool {
	pathKey := s.PathTransform(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, s.Id, pathKey.FullPath())
	_, err := os.Stat(fullPathWithRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	return true
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

// Delete deletes a file from the store
func (s *Store) Delete(key string) error {
	pathKey := s.PathTransform(key)
	defer func() {
		log.Println("Deleted", pathKey.Filename, "from disc")
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, s.Id, pathKey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)
}

// Read reads the content of a file from the store
func (s *Store) Read(key string) (int64, io.Reader, error) {
	return s.readStream(key)
}

// Write writes the content of a file to the store
func (s *Store) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Store) WriteDecryptStream(encKey []byte, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(key)
	if err != nil {
		return 0, err
	}

	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}

func (s *Store) openFileForWriting(key string) (*os.File, error) {
	pathKey := s.PathTransform(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, s.Id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPath := pathKey.FullPath()
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, s.Id, fullPath)

	return os.Create(fullPathWithRoot)
}

// readStream reads the content of a file from the store
func (s *Store) readStream(key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransform(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, s.Id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(key)
	if err != nil {
		return 0, err
	}

	return io.Copy(f, r)
}
