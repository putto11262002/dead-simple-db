package deadsimpledb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_freeList_pop(t *testing.T) {
	// Setup
	freed := []uint64{1, 2, 3}
	fl := freeList{
		freed: freed,
		cache: make(map[uint64]bool),
		size:  len(freed),
	}

	// Table-driven test cases
	testCases := []struct {
		name        string
		expectOK    bool
		expectValue uint64
	}{
		{"First pop returns last item", true, freed[2]},
		{"Second pop returns middle item", true, freed[1]},
		{"Third pop returns first item", true, freed[0]},
		{"Fourth pop returns nothing", false, 0},
	}

	// Run test cases
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ptr, ok := fl.pop()

			// Verify results
			require.Equal(t, tc.expectOK, ok)
			if ok {
				require.Equal(t, tc.expectValue, ptr, "Popped incorrect value")
				require.Equal(t, i+1, fl.popn, "Pop counter not incremented correctly")
				require.False(t, fl.cache[ptr], "Popped value not added to cache")
				require.Equal(t, fl.size-fl.freeCount(), fl.popn, "Free count inconsistent with pop count")
			}
		})
	}
}

func Test_freeList_free(t *testing.T) {
	t.Run("Basic free operation", func(t *testing.T) {
		// Setup
		freed := []uint64{1, 2}
		fl := freeList{
			freed: freed,
			cache: make(map[uint64]bool),
			size:  len(freed),
		}

		// Test freeing a new page
		newPage := uint64(3)
		fl.free(newPage)

		// Verify state
		require.Equal(t, 1, fl.pendingCount(), "Pending count incorrect")
		require.True(t, fl.cache[newPage], "Cache should not contain freed page yet")
		require.Contains(t, fl.pending, newPage, "Pending list missing freed page")
	})

	t.Run("Double free detection", func(t *testing.T) {
		// Setup
		fl := freeList{
			cache: make(map[uint64]bool),
		}

		// Add a page to the pending list
		fl.free(3)
		initialPendingCount := fl.pendingCount()

		// Verify that double free causes panic
		require.Panics(t, func() {
			fl.free(3)
		}, "Expected panic on double free")

		// Verify state after panic recovery
		require.Equal(t, initialPendingCount, fl.pendingCount(), "Pending count should be unchanged")
		require.NotContains(t, fl.pending, 3, "Pending list should not contain the double-freed page")
	})
}

func compareFl(t *testing.T, fl1, fl2 *freeList) {
	require.Equal(t, fl1.getSize(), fl2.getSize(), "size mismatch")
	require.Equal(t, fl1.freeCount(), fl2.freeCount(), "free count mismatch")
	require.Equal(t, fl1.pendingCount(), fl2.pendingCount(), "pending count mismatch")
	require.Equal(t, fl1.popn, fl2.popn, "popn mismatch")
	require.Equal(t, fl1.head, fl2.head, "head mismatch")
	require.Equal(t, fl1.freed, fl2.freed, "freed mismatch")
	require.Equal(t, fl1.cache, fl2.cache, "cache mismatch")
}

// Test_freeList_writeRead tests the persistence of freelist data by:
// 1. Creating a freelist, modifying it, and writing to disk
// 2. Reading it back into a new freelist instance
// 3. Verifying both freelists match exactly
// This ensures serialization/deserialization works correctly through multiple operations
func Test_freeList_writeRead(t *testing.T) {
	// Setup test environment
	const testFreeListCap = 4
	freeListCap = testFreeListCap
	memPager := newMemoryPager()
	fl := newFreeList(memPager)
	fl2 := newFreeList(memPager)

	// Test case 1: Basic allocation and freeing
	t.Run("Basic allocation and freeing", func(t *testing.T) {
		// Allocate and free a batch of pages
		allocated := allocateTestPages(memPager, testFreeListCap)
		freeAllPages(fl, allocated)

		// Write first freelist to disk and read into second freelist
		fl.write()
		fl2.read(fl.head)

		// Verify both freelists match
		compareFl(t, fl, fl2)
	})

	// Test case 2: Adding more items to the freelist
	t.Run("Adding more items", func(t *testing.T) {
		// Allocate and free additional pages
		additionalPages := allocateTestPages(memPager, testFreeListCap)
		freeAllPages(fl, additionalPages)

		// Write and read again
		fl.write()
		fl2.read(fl.head)

		// Verify again
		compareFl(t, fl, fl2)
	})

	// Test case 3: Consuming items from the freelist
	t.Run("Consuming items", func(t *testing.T) {
		// Use all items in the freelist
		nfree := fl.freeCount()
		for i := 0; i < nfree; i++ {
			ptr, ok := fl.pop()
			require.True(t, ok, "Failed to pop element %d from freelist", i)
			require.NotZero(t, ptr, "Popped zero pointer from freelist")
		}
		// Write and read again
		fl.write()
		fl2.read(fl.head)

		// Verify once more
		compareFl(t, fl, fl2)
	})
}

// / allocateTestPages allocates a specified number of pages
func allocateTestPages(pager Pager, count int) []uint64 {
	allocated := make([]uint64, count)
	for i := range allocated {
		allocated[i] = pager.allocate(Page{inner: make([]byte, PageSize)})
	}
	return allocated
}

// freeAllPages adds all pages in the slice to the freelist
func freeAllPages(fl *freeList, pages []uint64) {
	for _, ptr := range pages {
		fl.free(ptr)
	}
}
