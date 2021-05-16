package memdb

import (
	"context"
	"errors"
)

// Common errors returned by the DB implementations.
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

// DB is a simple memory DB implementation that stores data in
// an in-memory Btree. It is not thread safe.
type DB struct {
	closed    bool
	stores    map[string]*tree
	sequences map[string]uint64
}

// NewDB creates an in-memory DB.
func NewDB() *DB {
	return &DB{
		stores:    make(map[string]*tree),
		sequences: make(map[string]uint64),
	}
}

// Begin creates a transaction.
func (ng *DB) Begin(ctx context.Context, writable bool) (*Tx, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if ng.closed {
		return nil, errors.New("DB closed")
	}

	return &Tx{ctx: ctx, ng: ng, writable: writable}, nil
}

// Close the DB.
func (ng *DB) Close() error {
	if ng.closed {
		return errors.New("DB already closed")
	}

	ng.closed = true
	return nil
}

// Update the DB.
func (ng *DB) Update(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := ng.Begin(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}
