package memdb

import (
	"context"
	"sync"

	"github.com/google/btree"
)

// This implements the DB.Transaction type.
type Tx struct {
	ctx        context.Context
	ng         *DB
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

// DB get memDB
func (tx *Tx) DB() *DB {
	return tx.ng
}

func (tx *Tx) Bucket(name []byte) (*Bucket, error) {
	select {
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	default:
	}

	tr, ok := tx.ng.stores[string(name)]
	if !ok {
		return nil, ErrStoreNotFound
	}
	return getBucket(tx, tr, name), nil
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	select {
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return nil, ErrTransactionReadOnly
	}

	_, err := tx.Bucket(name)
	if err == nil {
		return nil, ErrStoreAlreadyExists
	}

	btr := btree.New(btreeDegree)
	tr := &tree{bt: btr}

	tx.ng.stores[string(name)] = tr
	// on rollback, remove the bucket from the list of stores
	tx.onRollback = append(tx.onRollback, func() {
		delete(tx.ng.stores, string(name))
	})
	return getBucket(tx, tr, name), nil
}

func (tx *Tx) DeleteBucket(name []byte) error {
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
