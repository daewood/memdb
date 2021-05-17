package memdb

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/btree"
)

// item implements an Item.
// it is also used as a btree.Item.
type mItem struct {
	k, v []byte
	// set to true if the item has been deleted
	// during the current transaction
	// but before rollback or commit.
	deleted bool
	ttl     uint64
}

func (i *mItem) Key() []byte {
	return i.k
}

func (i *mItem) ValueCopy(buf []byte) ([]byte, error) {
	if len(buf) < len(i.v) {
		buf = make([]byte, len(i.v))
	}
	n := copy(buf, i.v)
	return buf[:n], nil
}

func (i *mItem) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*mItem).k) < 0
}

// Bucket implements an Bucket.
type Bucket struct {
	tr   *tree
	tx   *Tx
	name string
}

func newBucket(tx *Tx, tr *tree, name []byte) *Bucket {
	return &Bucket{tx: tx, tr: tr, name: string(name)}
}

func (b *Bucket) Count() int {
	return b.tr.Len()
}

func (b *Bucket) Has(k []byte) bool {
	return b.tr.Has(&mItem{k: k})
}

func (b *Bucket) Put(k, v []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	if !b.tx.writable {
		return ErrTransactionReadOnly
	}

	if len(k) == 0 {
		return errors.New("empty keys are forbidden")
	}

	if len(v) == 0 {
		return errors.New("empty values are forbidden")
	}

	it := &mItem{k: k}
	// if there is an existing value, fetch it
	// and overwrite it directly using the pointer.
	if i := b.tr.Get(it); i != nil {
		cur := i.(*mItem)

		oldv, oldDeleted := cur.v, cur.deleted
		cur.v = v
		cur.deleted = false

		// on rollback replace the new value by the old value
		b.tx.onRollback = append(b.tx.onRollback, func() {
			cur.v = oldv
			cur.deleted = oldDeleted
		})

		return nil
	}

	it.v = v
	// TODO: move to index
	it.ttl = uint64(time.Now().Unix())
	b.tr.ReplaceOrInsert(it)

	// on rollback delete the new item
	b.tx.onRollback = append(b.tx.onRollback, func() {
		b.tr.Delete(it)
	})

	return nil
}

func (b *Bucket) Get(k []byte) ([]byte, error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	it := b.tr.Get(&mItem{k: k})

	if it == nil {
		return nil, ErrKeyNotFound
	}

	i := it.(*mItem)
	// don't return items that have been deleted during
	// this transaction.
	if i.deleted {
		return nil, ErrKeyNotFound
	}

	return it.(*mItem).v, nil
}

// Delete marks k for deletion. The item will be actually
// deleted during the commit phase of the current transaction.
// The deletion is delayed to avoid a rebalancing of the tree
// every time we remove an item from it,
// which causes iterators to behave incorrectly when looping
// and deleting at the same time.
func (b *Bucket) Delete(k []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	if !b.tx.writable {
		return ErrTransactionReadOnly
	}

	it := b.tr.Get(&mItem{k: k})
	if it == nil {
		return ErrKeyNotFound
	}

	i := it.(*mItem)
	// items that have been deleted during
	// this transaction must be ignored.
	if i.deleted {
		return ErrKeyNotFound
	}

	// set the deleted flag to true.
	// this makes the item invisible during this
	// transaction without actually deleting it
	// from the tree.
	// once the transaction is commited, actually
	// remove it from the tree.
	i.deleted = true

	// on rollback set the deleted flag to false.
	b.tx.onRollback = append(b.tx.onRollback, func() {
		i.deleted = false
	})

	// on commit, remove the item from the tree.
	b.tx.onCommit = append(b.tx.onCommit, func() {
		if i.deleted {
			b.tr.Delete(i)
		}
	})
	return nil
}

// Truncate replaces the current tree by a new
// one. The current tree will be garbage collected
// once the transaction is commited.
func (b *Bucket) Truncate() error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	if !b.tx.writable {
		return ErrTransactionReadOnly
	}

	old := b.tr
	b.tr = &tree{bt: btree.New(btreeDegree)}

	// on rollback replace the new tree by the old one.
	b.tx.onRollback = append(b.tx.onRollback, func() {
		b.tr = old
	})

	return nil
}

// NextSequence returns a monotonically increasing integer.
func (b *Bucket) NextSequence() (uint64, error) {
	select {
	case <-b.tx.ctx.Done():
		return 0, b.tx.ctx.Err()
	default:
	}

	if !b.tx.writable {
		return 0, ErrTransactionReadOnly
	}

	return b.tr.NextSequence(), nil
}

func (b *Bucket) Sequence() uint64 {
	return b.tr.Sequence()
}

func (b *Bucket) SetSequence(seq uint64) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	if !b.tx.writable {
		return ErrTransactionReadOnly
	}
	b.tr.SetSequence(seq)
	return nil
}

func (b *Bucket) Tx() *Tx {
	return b.tx
}

func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates an iterator with the given optionb.
func (b *Bucket) Cursor(reverse bool) *Cursor {
	return &Cursor{
		tx:      b.tx,
		tr:      b.tr,
		reverse: reverse,
		ch:      make(chan *mItem),
		closed:  make(chan struct{}),
	}
}

// Cursor uses a goroutine to read from the tree on demand.
type Cursor struct {
	tx      *Tx
	reverse bool
	tr      *tree
	item    *mItem // current item
	ch      chan *mItem
	closed  chan struct{} // closed by the goroutine when it's shutdown
	ctx     context.Context
	cancel  func()
	err     error
}

// Seek moves the iterator to the selected key. If the key doesn't exist, it must move to the
// next smallest key greater than k.
func (c *Cursor) Seek(pivot []byte) {
	// make sure any opened goroutine
	// is closed before creating a new one
	if c.cancel != nil {
		c.cancel()
		<-c.closed
	}

	c.ch = make(chan *mItem)
	c.closed = make(chan struct{})
	c.ctx, c.cancel = context.WithCancel(c.tx.ctx)

	c.runIterator(pivot)

	c.Next()
}

// runIterator creates a goroutine that reads from the tree.
// Once the goroutine is done reading or if the context is canceled,
// both ch and closed channels will be closed.
func (c *Cursor) runIterator(pivot []byte) {
	c.tx.wg.Add(1)

	go func(ctx context.Context, ch chan *mItem, tr *tree) {
		defer c.tx.wg.Done()
		defer close(ch)
		defer close(c.closed)

		iter := btree.ItemIterator(func(i btree.Item) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}

			itm := i.(*mItem)
			if itm.deleted {
				return true
			}

			select {
			case <-ctx.Done():
				return false
			case ch <- itm:
				return true
			}
		})

		if c.reverse {
			if len(pivot) == 0 {
				tr.Descend(iter)
			} else {
				tr.DescendLessOrEqual(&mItem{k: pivot}, iter)
			}
		} else {
			if len(pivot) == 0 {
				tr.Ascend(iter)
			} else {
				tr.AscendGreaterOrEqual(&mItem{k: pivot}, iter)
			}
		}
	}(c.ctx, c.ch, c.tr)
}

// Valid returns whether the iterator is positioned on a valid item or not.
func (c *Cursor) Valid() bool {
	if c.err != nil {
		return false
	}
	select {
	case <-c.tx.ctx.Done():
		c.err = c.tx.ctx.Err()
	default:
	}

	return c.item != nil && c.err == nil
}

// Read the next item from the goroutine
func (c *Cursor) Next() {
	select {
	case c.item = <-c.ch:
	case <-c.tx.ctx.Done():
		c.err = c.tx.ctx.Err()
	}
}

// Err returns an error that invalidated iterator.
// If Err is not nil then Valid must return false.
func (c *Cursor) Err() error {
	return c.err
}

// Item returns the current item.
func (c *Cursor) Item() Item {
	return c.item
}

// Close the inner goroutine.
func (c *Cursor) Close() error {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
		<-c.closed
	}

	return nil
}
