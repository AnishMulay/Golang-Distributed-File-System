package main

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"strings"
)

// Content Addressable Storage (CAS) is a method of storing data
// such that the key is derived from the content itself.
func CASTransformFunc(key string) string {
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

	return strings.Join(paths, "/")
}

// PathTransformFunc is a function that transforms a key into a path.
type PathTransformFunc func(string) string

var DefaultPathTransformFunc = func(key string) string {
	return key
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
	pathName := s.PathTransform(key)

	if err := os.MkdirAll(pathName, os.ModePerm); err != nil {
		return err
	}

	filename := "randomfilefornow"
	pathAndFilename := pathName + "/" + filename

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
