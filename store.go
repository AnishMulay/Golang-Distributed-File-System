package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

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
		Original: hashString,
	}
}

type PathKey struct {
	PathName string
	Original string
}

// PathTransformFunc is a function that transforms a key into a path.
type PathTransformFunc func(string) PathKey

var DefaultPathTransformFunc = func(key string) string {
	return key
}

func (p PathKey) Filename() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Original)
}

type StoreConfig struct {
	PathTransform PathTransformFunc
}

type Store struct {
	StoreConfig
}

func NewStore(opts StoreConfig) *Store {
	return &Store{
		StoreConfig: opts,
	}
}

func (s *Store) writeStream(key string, r io.Reader) error {
	pathKey := s.PathTransform(key)
	if err := os.MkdirAll(pathKey.PathName, os.ModePerm); err != nil {
		return err
	}

	pathAndFilename := pathKey.Filename()

	f, err := os.Create(pathAndFilename)
	if err != nil {
		return err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	log.Println("Wrote", n, "bytes to", pathAndFilename)

	return nil
}
