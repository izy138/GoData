package main

import (
	"os"
	"testing"
)

// Helper function to create a temporary database file for testing
func setupTestDB(t *testing.T) (*Storage, string) {
	tmpFile := "test_" + t.Name() + ".db"
	storage, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return storage, tmpFile
}

// Helper function to cleanup test database
func cleanupTestDB(t *testing.T, filename string) {
	if err := os.Remove(filename); err != nil {
		t.Logf("Warning: failed to remove test file %s: %v", filename, err)
	}
}

func TestNewStorage_CreateNewDatabase(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Verify initial state
	if storage.totalPages != 0 {
		t.Errorf("Expected totalPages to be 0, got %d", storage.totalPages)
	}
	if storage.nextPageID != 0 {
		t.Errorf("Expected nextPageID to be 0, got %d", storage.nextPageID)
	}
}

func TestPutAndGet_BasicOperations(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Test Put
	key := "user:1"
	value := "isabella"
	if err := storage.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	retrieved, err := storage.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved != value {
		t.Errorf("Expected value %q, got %q", value, retrieved)
	}
}

func TestPut_UpdateExistingKey(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	key := "user:1"
	initialValue := "isabella"
	updatedValue := "leonor"

	// Put initial value
	if err := storage.Put(key, initialValue); err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Update the value
	if err := storage.Put(key, updatedValue); err != nil {
		t.Fatalf("Update Put failed: %v", err)
	}

	// Verify update
	retrieved, err := storage.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved != updatedValue {
		t.Errorf("Expected updated value %q, got %q", updatedValue, retrieved)
	}
}

func TestGet_NonExistentKey(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	_, err := storage.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent key, got nil")
	}
}

func TestDelete_BasicOperation(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	key := "user:1"
	value := "isabella"

	// Put a value
	if err := storage.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete it
	if err := storage.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, err := storage.Get(key)
	if err == nil {
		t.Error("Expected error after delete, got nil")
	}
}

func TestDelete_NonExistentKey(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	err := storage.Delete("nonexistent")
	if err == nil {
		t.Error("Expected error for deleting non-existent key, got nil")
	}
}

func TestPersistence_ReopenDatabase(t *testing.T) {
	filename := "test_persistence.db"
	defer cleanupTestDB(t, filename)

	// Create database and add data
	storage1, err := NewStorage(filename)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	storage1.Put("user:1", "isabella")
	storage1.Put("user:2", "cam")
	storage1.Close()

	// Reopen database
	storage2, err := NewStorage(filename)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer storage2.Close()

	// Verify data persisted
	value1, err := storage2.Get("user:1")
	if err != nil {
		t.Fatalf("Failed to get user:1: %v", err)
	}
	if value1 != "isabella" {
		t.Errorf("Expected 'isabella', got %q", value1)
	}

	value2, err := storage2.Get("user:2")
	if err != nil {
		t.Fatalf("Failed to get user:2: %v", err)
	}
	if value2 != "cam" {
		t.Errorf("Expected 'cam', got %q", value2)
	}
}

func TestMultipleRecords_SamePage(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Add multiple records that should fit in one page
	records := map[string]string{
		"user:1": "isabella",
		"user:2": "cam",
		"user:3": "alice",
		"user:4": "bob",
	}

	// Put all records
	for key, value := range records {
		if err := storage.Put(key, value); err != nil {
			t.Fatalf("Put failed for %s: %v", key, err)
		}
	}

	// Verify all records
	for key, expectedValue := range records {
		value, err := storage.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("For key %s: expected %q, got %q", key, expectedValue, value)
		}
	}
}

func TestLargeValue(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Create a value that's large but fits in a page
	largeValue := make([]byte, 1000)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	key := "large:key"
	if err := storage.Put(key, string(largeValue)); err != nil {
		t.Fatalf("Put failed for large value: %v", err)
	}

	retrieved, err := storage.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved != string(largeValue) {
		t.Error("Large value mismatch")
	}
}

func TestEmptyKey(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Empty key should work
	if err := storage.Put("", "empty_key_value"); err != nil {
		t.Fatalf("Put with empty key failed: %v", err)
	}

	value, err := storage.Get("")
	if err != nil {
		t.Fatalf("Get with empty key failed: %v", err)
	}
	if value != "empty_key_value" {
		t.Errorf("Expected 'empty_key_value', got %q", value)
	}
}

func TestEmptyValue(t *testing.T) {
	storage, filename := setupTestDB(t)
	defer cleanupTestDB(t, filename)
	defer storage.Close()

	// Empty value should work
	if err := storage.Put("empty:value", ""); err != nil {
		t.Fatalf("Put with empty value failed: %v", err)
	}

	value, err := storage.Get("empty:value")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "" {
		t.Errorf("Expected empty string, got %q", value)
	}
}

func TestBuildIndex_AfterReopen(t *testing.T) {
	filename := "test_index.db"
	defer cleanupTestDB(t, filename)

	// Create and populate database
	storage1, _ := NewStorage(filename)
	storage1.Put("key1", "value1")
	storage1.Put("key2", "value2")
	storage1.Put("key3", "value3")
	storage1.Close()

	// Reopen - buildIndex should reconstruct the pageIndex
	storage2, _ := NewStorage(filename)
	defer storage2.Close()

	// All keys should be accessible
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		_, err := storage2.Get(key)
		if err != nil {
			t.Errorf("Key %s not found after reopen: %v", key, err)
		}
	}
}
