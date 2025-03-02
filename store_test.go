package main

import (
	"bytes"
	"testing"
)

func TestStore(t *testing.T) {
	storeconfig := StoreConfig{
		PathTransform: DefaultPathTransformFunc,
	}

	store := NewStore(storeconfig)

	data := bytes.NewReader([]byte("Hello Anish"))
	if err := store.writeStream("randompathfornow", data); err != nil {
		t.Error(err)
	}
}
