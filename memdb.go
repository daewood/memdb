package memdb

import (
	"context"
	"errors"
	"sync"

	"github.com/google/btree"
)

// Common errors returned by the engine implementations.
var (
	// ErrTransactionReadOnly is returned when attempting to call write methods on a read-only transaction.
	ErrTransactionReadOnly = errors.New("transaction is read-only")

	// ErrTransactionDiscarded is returned when calling Rollback or Commit after a transaction is no longer valid.
	ErrTransactionDiscarded = errors.New("transaction has been discarded")

	// ErrStoreNotFound is returned when the targeted store doesn't exist.
	ErrStoreNotFound = errors.New("store not found")

	// ErrStoreAlreadyExists must be returned when attempting to create a store with the
	// same name as an existing one.
	ErrStoreAlreadyExists = errors.New("store already exists")

	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

// An Iterator iterates on keys of a store in lexicographic order.
type Iterator interface {
	// Seek moves the iterator to the selected key. If the key doesn't exist, it must move to the
	// next smallest key greater than k.
	Seek(k []byte)
	// Next moves the iterator to the next item.
	Next()
	// Err returns an error that invalidated iterator.
	// If Err is not nil then Valid must return false.
	Err() error
	// Valid returns whether the iterator is positioned on a valid item or not.
	Valid() bool
	// Item returns the current item.
	Item() Item
	// Close releases the resources associated with the iterator.
	Close() error
}

// An Item represents a key-value pair.
type Item interface {
	// Key returns the key of the item.
	// The key is only guaranteed to be valid until the next call to the Next method of
	// the iterator.
	Key() []byte
	// ValueCopy copies the key to the given byte slice and returns it.
	// If the slice is not big enough, it must create a new one and return it.
	ValueCopy([]byte) ([]byte, error)
}

// The degree of btrees.
// This value is arbitrary and has been selected after
// a few benchmarks.
// It may be improved after thorough testing.
const btreeDegree = 12

// Engine is a simple memory engine implementation that stores data in
// an in-memory Btree. It is not thread safe.
type Engine struct {
	closed    bool
	stores    map[string]*tree
	sequences map[string]uint64
}

// NewEngine creates an in-memory engine.
func New() *Engine {
	return &Engine{
		stores:    make(map[string]*tree),
		sequences: make(map[string]uint64),
	}
}

// Begin creates a transaction.
func (ng *Engine) Begin(ctx context.Context, writable bool) (*Tx, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if ng.closed {
		return nil, errors.New("engine closed")
	}

	return &Tx{ctx: ctx, ng: ng, writable: writable}, nil
}

// Close the engine.
func (ng *Engine) Close() error {
	if ng.closed {
		return errors.New("engine already closed")
	}

	ng.closed = true
	return nil
}

// This implements the engine.Transaction type.
type Tx struct {
	ctx        context.Context
	ng         *Engine
	writable   bool
	onRollback []func() // called during a rollback
	onCommit   []func() // called during a commit
	terminated bool
	wg         sync.WaitGroup
}

// If the transaction is writable, rollback calls
// every function stored in the onRollback slice
// to undo every mutation done since the beginning
// of the transaction.
func (tx *Tx) Rollback() error {
	if tx.terminated {
		return ErrTransactionDiscarded
	}

	tx.terminated = true

	tx.wg.Wait()

	if tx.writable {
		for _, undo := range tx.onRollback {
			undo()
		}
	}

	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	return nil
}

// If the transaction is writable, Commit calls
// every function stored in the onCommit slice
// to finalize every mutation done since the beginning
// of the transaction.
func (tx *Tx) Commit() error {
	if tx.terminated {
		return ErrTransactionDiscarded
	}

	if !tx.writable {
		return ErrTransactionReadOnly
	}

	tx.wg.Wait()

	select {
	case <-tx.ctx.Done():
		return tx.Rollback()
	default:
	}

	tx.terminated = true

	for _, fn := range tx.onCommit {
		fn()
	}

	return nil
}

func (tx *Tx) GetStore(name []byte) (*Store, error) {
	select {
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	default:
	}

	tr, ok := tx.ng.stores[string(name)]
	if !ok {
		return nil, ErrStoreNotFound
	}

	return &Store{tx: tx, tr: tr, name: string(name)}, nil
}

func (tx *Tx) CreateStore(name []byte) error {
	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return ErrTransactionReadOnly
	}

	_, err := tx.GetStore(name)
	if err == nil {
		return ErrStoreAlreadyExists
	}

	tr := btree.New(btreeDegree)

	tx.ng.stores[string(name)] = &tree{bt: tr}

	// on rollback, remove the btree from the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		delete(tx.ng.stores, string(name))
	})

	return nil
}

func (tx *Tx) DropStore(name []byte) error {
	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return ErrTransactionReadOnly
	}

	rb, ok := tx.ng.stores[string(name)]
	if !ok {
		return ErrStoreNotFound
	}

	delete(tx.ng.stores, string(name))

	// on rollback put back the btree to the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		tx.ng.stores[string(name)] = rb
	})

	return nil
}

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
