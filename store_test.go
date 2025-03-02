package main

import (
	"bytes"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "anish"
	pathKey := CASTransformFunc(key)
	expectedOriginal := "6c150f28a67bd7084899fc5ec19a5f87459dd653"
	expectedPathName := "6c150/f28a6/7bd70/84899/fc5ec/19a5f/87459/dd653"
	if pathKey.Original != expectedOriginal {
		t.Errorf("Expected %s, got %s", expectedOriginal, pathKey.Original)
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

	data := bytes.NewReader([]byte("Hello Anish"))
	if err := store.writeStream("randompathfornow", data); err != nil {
		t.Error(err)
	}
}
