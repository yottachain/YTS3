package goskipiter

import "github.com/ryszard/goskiplist/skiplist"

type Iterator struct {
	inner     skiplist.Iterator
	didSeek   bool
	seekWasOK bool
}

func New(inner skiplist.Iterator) *Iterator {
	return &Iterator{inner: inner}
}

func (iter *Iterator) Next() (ok bool) {
	if iter.didSeek {
		iter.didSeek = false
		return iter.seekWasOK
	} else {
		return iter.inner.Next()
	}
}

func (iter *Iterator) Previous() (ok bool) {
	if iter.didSeek {
		panic("not implemented")
	}
	return iter.inner.Previous()
}

func (iter *Iterator) Key() interface{} {
	return iter.inner.Key()
}

// Value returns the current value.
func (iter *Iterator) Value() interface{} {
	return iter.inner.Value()
}

func (iter *Iterator) Seek(key interface{}) (ok bool) {
	iter.didSeek = true
	ok = iter.inner.Seek(key)
	iter.seekWasOK = ok
	return ok
}

func (iter *Iterator) Close() {
	iter.inner.Close()
}
