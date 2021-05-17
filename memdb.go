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

// DB is a simple memory DB implementation that stores data in
// an in-memory Btree. It is not thread safe.
type DB struct {
	closed bool
	stores map[string]*tree
}

// NewDB creates an in-memory DB.
func NewDB() *DB {
	return &DB{
		stores: make(map[string]*tree),
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
