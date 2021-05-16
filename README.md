# memdb
golang memory btree database, boltdb like interface

```
func main() {
	ng := memdb.New()
	defer ng.Close()

	//writable
	tx, err := ng.Begin(context.Background(), true)
	if err != nil {
		log.Fatal(err)
	}
	_, err = tx.CreateBucket([]byte("test"))
	if err != nil {
		log.Fatal(err)
	}

	st, err := tx.Bucket([]byte("test"))
	if err != nil {
		log.Fatal(err)
	}

	err = st.Put([]byte("foo"), []byte("FOO"))
	if err != nil {
		log.Fatal(err)
	}

	// delete the key
	err = st.Delete([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}

	_, err = st.Get([]byte("foo"))
	if err != memdb.ErrKeyNotFound {
		log.Fatal(err)
	}

	err = st.Put([]byte("foo"), []byte("bar"))
	if err != nil {
		log.Fatal(err)
	}

	v, err := st.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", v)

	// commit and reopen a transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	//read only
	tx, err = ng.Begin(context.Background(), false)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	st, err = tx.Bucket([]byte("test"))
	if err != nil {
		log.Fatal(err)
	}

	v, err = st.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", v)
}
```