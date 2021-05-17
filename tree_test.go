package memdb

import (
	"fmt"
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func TestTree(t *testing.T) {
	tr := newTree()
	b1 := []byte("hello1")
	tr.ReplaceOrInsert(&mItem{k: b1, v: b1})
	b2 := []byte("hello2")
	tr.ReplaceOrInsert(&mItem{k: b2, v: b2})
	require.Equal(t, 2, tr.Len())
	require.Equal(t, true, tr.Has(&mItem{k: b1}))
	it2 := tr.Get(&mItem{k: b2})
	require.Equal(t, b2, it2.(*mItem).v)
	iterator := func(i btree.Item) bool {
		it := i.(*mItem)
		fmt.Println(it)
		return true
	}
	tr.Ascend(iterator)
}
