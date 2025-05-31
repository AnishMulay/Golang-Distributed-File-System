//go:build unit

// metadata_test.go
package main

import (
	"os"
	"testing"
)

func TestPathStoreBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	ps, err := NewPathStore(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	testPath := "/testfile.txt"
	contentKey := "abc123"
	size := int64(1024)
	mode := os.FileMode(0644)

	// Test Set
	err = ps.Set(testPath, contentKey, size, mode, FileTypeRegular)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	// Test Exists
	if !ps.Exists(testPath) {
		t.Error("Exists returned false for existing path")
	}

	// Test Get
	meta, err := ps.Get(testPath)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	if meta.ContentKey != contentKey {
		t.Errorf("ContentKey mismatch: got %s want %s", meta.ContentKey, contentKey)
	}

	// Test Delete
	err = ps.Delete(testPath)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	if ps.Exists(testPath) {
		t.Error("File still exists after deletion")
	}
}

func TestPathStorePersistence(t *testing.T) {
	tempDir := t.TempDir()

	// First instance
	ps1, _ := NewPathStore(tempDir)
	ps1.Set("/persistent.txt", "def456", 2048, 0644, FileTypeRegular)

	// Second instance should load existing data
	ps2, _ := NewPathStore(tempDir)
	if !ps2.Exists("/persistent.txt") {
		t.Error("Metadata not persisted between instances")
	}
}
