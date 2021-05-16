package memdb

import (
	"sync"

	"github.com/google/btree"
)

// tree is a thread safe wrapper aroung BTree.
// It prevents modifying and rebalancing the btree while other
// routines are reading it.
type tree struct {
	bt *btree.BTree

	m sync.RWMutex
}

func (t *tree) Get(key btree.Item) btree.Item {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.bt.Get(key)
}

func (t *tree) Delete(key btree.Item) btree.Item {
	t.m.Lock()
	defer t.m.Unlock()

	return t.bt.Delete(key)
}

func (t *tree) ReplaceOrInsert(key btree.Item) btree.Item {
	t.m.Lock()
	defer t.m.Unlock()

	return t.bt.ReplaceOrInsert(key)
}

func (t *tree) Ascend(iterator btree.ItemIterator) {
	t.m.RLock()
	defer t.m.RUnlock()

	t.bt.Ascend(func(i btree.Item) bool {
		t.m.RUnlock()
		defer t.m.RLock()

		return iterator(i)
	})
}

func (t *tree) AscendGreaterOrEqual(pivot btree.Item, iterator btree.ItemIterator) {
	t.m.RLock()
	defer t.m.RUnlock()

	t.bt.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		t.m.RUnlock()
		defer t.m.RLock()

		return iterator(i)
	})
}

func (t *tree) Descend(iterator btree.ItemIterator) {
	t.m.RLock()
	defer t.m.RUnlock()

	t.bt.Descend(func(i btree.Item) bool {
		t.m.RUnlock()
		defer t.m.RLock()

		return iterator(i)
	})
}

func (t *tree) DescendLessOrEqual(pivot btree.Item, iterator btree.ItemIterator) {
	t.m.RLock()
	defer t.m.RUnlock()

	t.bt.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		t.m.RUnlock()
		defer t.m.RLock()

		return iterator(i)
	})
}
