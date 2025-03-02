package main

import (
	"bytes"
	"io"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "anish"
	pathKey := CASTransformFunc(key)
	expectedFilename := "6c150f28a67bd7084899fc5ec19a5f87459dd653"
	expectedPathName := "6c150/f28a6/7bd70/84899/fc5ec/19a5f/87459/dd653"
	if pathKey.Filename != expectedFilename {
		t.Errorf("Expected %s, got %s", expectedFilename, pathKey.Filename)
	}
	if pathKey.PathName != expectedPathName {
		t.Errorf("Expected %s, got %s", expectedPathName, pathKey.PathName)
	}
}

func TestStore(t *testing.T) {
	storeconfig := StoreConfig{
		PathTransform: CASTransformFunc,
	}
	store := NewStore(storeconfig)

	key := "anish"
	data := []byte("Hello Anish")

	if err := store.writeStream(key, bytes.NewReader(data)); err != nil {
		t.Error(err)
	}

	r, err := store.Read(key)
	if err != nil {
		t.Error(err)
	}

	b, _ := io.ReadAll(r)
	if !bytes.Equal(b, data) {
		t.Errorf("Expected %s, got %s", data, b)
	}
}
