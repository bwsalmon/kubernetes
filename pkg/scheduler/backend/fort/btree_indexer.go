package fort

import (
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/third_party/forked/golang/btree"
)

// CloneableIndexer extends cache.Indexer with a fast Clone operation.
type CloneableIndexer interface {
	cache.Indexer
	Clone() CloneableIndexer
}

type btreeItem struct {
	key string
	obj any
}

func btreeItemLess(a, b btreeItem) bool {
	return a.key < b.key
}

type btreeIndexer struct {
	keyFunc cache.KeyFunc
	tree    *btree.BTree[btreeItem]
	lastRV  string
}

// NewBTreeIndexer creates a new fast-cloneable indexer.
func NewBTreeIndexer(keyFunc cache.KeyFunc) CloneableIndexer {
	return &btreeIndexer{
		keyFunc: keyFunc,
		tree:    btree.New(2, btreeItemLess), // degree 2 means 2-3-4 tree
	}
}

func (i *btreeIndexer) Clone() CloneableIndexer {
	return &btreeIndexer{
		keyFunc: i.keyFunc,
		tree:    i.tree.Clone(),
		lastRV:  i.lastRV,
	}
}

func (i *btreeIndexer) Add(obj any) error {
	key, err := i.keyFunc(obj)
	if err != nil {
		return err
	}
	i.tree.ReplaceOrInsert(btreeItem{key: key, obj: obj})
	return nil
}

func (i *btreeIndexer) Update(obj any) error {
	return i.Add(obj)
}

func (i *btreeIndexer) Delete(obj any) error {
	key, err := i.keyFunc(obj)
	if err != nil {
		return err
	}
	i.tree.Delete(btreeItem{key: key})
	return nil
}

func (i *btreeIndexer) List() []any {
	res := make([]any, 0, i.tree.Len())
	i.tree.Ascend(func(item btreeItem) bool {
		res = append(res, item.obj)
		return true
	})
	return res
}

func (i *btreeIndexer) ListKeys() []string {
	res := make([]string, 0, i.tree.Len())
	i.tree.Ascend(func(item btreeItem) bool {
		res = append(res, item.key)
		return true
	})
	return res
}

func (i *btreeIndexer) Get(obj any) (item any, exists bool, err error) {
	key, err := i.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}
	return i.GetByKey(key)
}

func (i *btreeIndexer) GetByKey(key string) (item any, exists bool, err error) {
	it, ok := i.tree.Get(btreeItem{key: key})
	if !ok {
		return nil, false, nil
	}
	return it.obj, true, nil
}

func (i *btreeIndexer) Replace(objs []any, rv string) error {
	i.tree.Clear(false)
	i.lastRV = rv
	for _, obj := range objs {
		if err := i.Add(obj); err != nil {
			return err
		}
	}
	return nil
}

func (i *btreeIndexer) Resync() error {
	return nil
}

func (i *btreeIndexer) LastStoreSyncResourceVersion() string {
	return i.lastRV
}

func (i *btreeIndexer) Bookmark(rv string) {
	i.lastRV = rv
}

// Indexer methods stubs - currently not used by Fort queries.
func (i *btreeIndexer) Index(indexName string, obj any) ([]any, error) {
	return nil, fmt.Errorf("Index not implemented in BTreeIndexer")
}

func (i *btreeIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	return nil, fmt.Errorf("IndexKeys not implemented in BTreeIndexer")
}

func (i *btreeIndexer) ListIndexFuncValues(indexName string) []string {
	return nil
}

func (i *btreeIndexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	return nil, fmt.Errorf("ByIndex not implemented in BTreeIndexer")
}

func (i *btreeIndexer) GetIndexerResyncPeriod(indexName string) time.Duration {
	return 0
}

func (i *btreeIndexer) GetIndexers() cache.Indexers {
	return nil
}

func (i *btreeIndexer) AddIndexers(newIndexers cache.Indexers) error {
	if len(newIndexers) > 0 {
		return fmt.Errorf("AddIndexers not implemented in BTreeIndexer")
	}
	return nil
}
