package deadsimpledb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	testAssert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeData(prefix string, size int) []byte {
	data := make([]byte, size)
	copy(data, []byte(prefix))
	// Fill the rest with incrementing values for uniqueness
	for i := len(prefix); i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}
func TestKV(t *testing.T) {
	// Create a temporary directory for test files
	testDir, err := os.MkdirTemp("", "kv-test-")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Helper function to create a new KV store for each test
	setupKV := func(t *testing.T, name string) *KV {
		dbPath := filepath.Join(testDir, fmt.Sprintf("%s.db", name))
		db := NewKV(dbPath)
		require.NoError(t, db.Open())
		return db
	}

	// Helper to create keys and values of specific sizes

	// Create test data with three size categories
	testCases := []struct {
		name      string
		keySize   int
		valueSize int
	}{
		{name: "Small", keySize: 16, valueSize: 32},
		{name: "Medium", keySize: BtreeMaxKeySize / 2, valueSize: BtreeMaxValueSize / 2},
		{name: "Large", keySize: BtreeMaxKeySize, valueSize: BtreeMaxValueSize},
	}

	t.Run("Set", func(t *testing.T) {
		db := setupKV(t, "set-test")
		defer db.Close()

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)
				value := makeData(fmt.Sprintf("value-%s-", tc.name), tc.valueSize)

				// Set the key-value pair
				err := db.Set(key, value)
				require.NoError(t, err)

				// Verify it was set correctly
				retrievedValue, exists := db.Get(key)
				testAssert.True(t, exists)
				testAssert.True(t, bytes.Equal(value, retrievedValue))
			})
		}

		// Test setting many key-value pairs to force tree splits and merges
		t.Run("ManyPairs", func(t *testing.T) {
			keyPrefix := "many-key-"
			valuePrefix := "many-value-"
			numPairs := 100 // Large enough to potentially cause tree operations

			keys := make([][]byte, numPairs)
			values := make([][]byte, numPairs)

			// First insert all pairs
			for i := 0; i < numPairs; i++ {
				keys[i] = makeData(fmt.Sprintf("%s%d-", keyPrefix, i), 16)
				values[i] = makeData(fmt.Sprintf("%s%d-", valuePrefix, i), 32)

				err := db.Set(keys[i], values[i])
				require.NoError(t, err)
			}

			// Then verify all were set correctly
			for i := 0; i < numPairs; i++ {
				retrievedValue, exists := db.Get(keys[i])
				testAssert.True(t, exists)
				testAssert.True(t, bytes.Equal(values[i], retrievedValue))
			}
		})
	})

	t.Run("Get", func(t *testing.T) {
		db := setupKV(t, "get-test")
		defer db.Close()

		// Setup key-value pairs for all test cases
		for _, tc := range testCases {
			key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)
			value := makeData(fmt.Sprintf("value-%s-", tc.name), tc.valueSize)
			require.NoError(t, db.Set(key, value))
		}

		// Test retrieving all the key-value pairs
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)
				expectedValue := makeData(fmt.Sprintf("value-%s-", tc.name), tc.valueSize)

				value, exists := db.Get(key)
				testAssert.True(t, exists)
				testAssert.True(t, bytes.Equal(expectedValue, value))
			})
		}

		// Test getting a non-existent key
		t.Run("NonExistentKey", func(t *testing.T) {
			nonExistentKey := []byte("this-key-does-not-exist")
			value, exists := db.Get(nonExistentKey)
			testAssert.False(t, exists)
			testAssert.Nil(t, value)
		})
	})

	t.Run("Update", func(t *testing.T) {
		db := setupKV(t, "update-test")
		defer db.Close()

		// Test all update modes: Insert, Update, Upsert
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)
				initialValue := makeData(fmt.Sprintf("initial-value-%s-", tc.name), tc.valueSize)
				updatedValue := makeData(fmt.Sprintf("updated-value-%s-", tc.name), tc.valueSize)

				// Test Insert mode
				t.Run("Insert", func(t *testing.T) {
					// Insert should succeed for a new key
					ok, err := db.Update(key, initialValue, Insert)
					require.NoError(t, err)
					testAssert.True(t, ok)

					// Verify the value was inserted
					retrievedValue, exists := db.Get(key)
					testAssert.True(t, exists)
					testAssert.True(t, bytes.Equal(initialValue, retrievedValue))

					// Insert should fail for an existing key
					ok, err = db.Update(key, updatedValue, Insert)
					require.NoError(t, err)
					testAssert.False(t, ok)

					// Value should remain unchanged
					retrievedValue, exists = db.Get(key)
					testAssert.True(t, exists)
					testAssert.True(t, bytes.Equal(initialValue, retrievedValue))
				})

				// Test Update mode
				t.Run("Update", func(t *testing.T) {
					// First insert a key-value pair
					newKey := makeData(fmt.Sprintf("update-key-%s-", tc.name), tc.keySize)
					newValue := makeData(fmt.Sprintf("update-initial-%s-", tc.name), tc.valueSize)

					ok, err := db.Update(newKey, newValue, Insert)
					require.NoError(t, err)
					testAssert.True(t, ok)

					// Update should succeed for an existing key
					updatedVal := makeData(fmt.Sprintf("update-changed-%s-", tc.name), tc.valueSize)
					ok, err = db.Update(newKey, updatedVal, Update)
					require.NoError(t, err)
					testAssert.True(t, ok)

					// Verify the value was updated
					retrievedValue, exists := db.Get(newKey)
					testAssert.True(t, exists)
					testAssert.True(t, bytes.Equal(updatedVal, retrievedValue))

					// Update should fail for a non-existent key
					nonExistentKey := []byte("non-existent-key")
					ok, err = db.Update(nonExistentKey, updatedVal, Update)
					require.NoError(t, err)
					testAssert.False(t, ok)
				})

				// Test Upsert mode
				t.Run("Upsert", func(t *testing.T) {
					// Upsert should succeed for a new key (insert)
					newKey := makeData(fmt.Sprintf("upsert-key-%s-", tc.name), tc.keySize)
					newValue := makeData(fmt.Sprintf("upsert-initial-%s-", tc.name), tc.valueSize)

					ok, err := db.Update(newKey, newValue, Upsert)
					require.NoError(t, err)
					testAssert.True(t, ok)

					// Verify the value was inserted
					retrievedValue, exists := db.Get(newKey)
					testAssert.True(t, exists)
					testAssert.True(t, bytes.Equal(newValue, retrievedValue))

					// Upsert should succeed for an existing key (update)
					updatedVal := makeData(fmt.Sprintf("upsert-changed-%s-", tc.name), tc.valueSize)
					ok, err = db.Update(newKey, updatedVal, Upsert)
					require.NoError(t, err)
					testAssert.True(t, ok)

					// Verify the value was updated
					retrievedValue, exists = db.Get(newKey)
					testAssert.True(t, exists)
					testAssert.True(t, bytes.Equal(updatedVal, retrievedValue))
				})
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		db := setupKV(t, "delete-test")
		defer db.Close()

		// Insert key-value pairs for all test cases
		for _, tc := range testCases {
			key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)
			value := makeData(fmt.Sprintf("value-%s-", tc.name), tc.valueSize)
			require.NoError(t, db.Set(key, value))
		}

		// Test deleting key-value pairs
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				key := makeData(fmt.Sprintf("key-%s-", tc.name), tc.keySize)

				// Verify key exists before deletion
				_, exists := db.Get(key)
				testAssert.True(t, exists)

				// Delete the key
				ok, err := db.Del(key)
				require.NoError(t, err)
				testAssert.True(t, ok)

				// Verify key no longer exists
				_, exists = db.Get(key)
				testAssert.False(t, exists)

				// Attempting to delete the key again should return false
				ok, err = db.Del(key)
				require.NoError(t, err)
				testAssert.False(t, ok)
			})
		}

		// Test deleting a non-existent key
		t.Run("NonExistentKey", func(t *testing.T) {
			nonExistentKey := []byte("this-key-does-not-exist")
			ok, err := db.Del(nonExistentKey)
			require.NoError(t, err)
			testAssert.False(t, ok)
		})
	})

	t.Run("Persistence", func(t *testing.T) {
		dbPath := filepath.Join(testDir, "persistence-test.db")

		// Setup: Create and populate a database
		{
			db := NewKV(dbPath)
			require.NoError(t, db.Open())

			// Insert a mix of key-value pairs of different sizes
			for i, tc := range testCases {
				key := makeData(fmt.Sprintf("persist-key-%d-", i), tc.keySize)
				value := makeData(fmt.Sprintf("persist-value-%d-", i), tc.valueSize)
				require.NoError(t, db.Set(key, value))
			}

			// Properly close the database
			require.NoError(t, db.Close())
		}

		// Test: Reopen the database and verify data persisted
		{
			db := NewKV(dbPath)
			require.NoError(t, db.Open())
			defer db.Close()

			// Verify all key-value pairs still exist
			for i, tc := range testCases {
				key := makeData(fmt.Sprintf("persist-key-%d-", i), tc.keySize)
				expectedValue := makeData(fmt.Sprintf("persist-value-%d-", i), tc.valueSize)

				value, exists := db.Get(key)
				testAssert.True(t, exists)
				testAssert.True(t, bytes.Equal(expectedValue, value))
			}
		}
	})
}
