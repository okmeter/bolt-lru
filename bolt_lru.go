// This package provides a simple LRU cache backed by boltdb It is based on the
// LRU implementation in groupcache:
// https://github.com/golang/groupcache/tree/master/lru
package boltlru

import (
	"container/list"
	"errors"
	"github.com/boltdb/bolt"
	"sync"
)

var bucketName = []byte("cache")

type Cache struct {
	size      int
	evictList *list.List
	items     map[string]*list.Element
	lock      sync.RWMutex
	db        *bolt.DB
}

func New(size int, dbPath string) (*Cache, error) {
	if size <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	db, err := bolt.Open(dbPath, 0640, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}
	c := &Cache{
		size:      size,
		evictList: list.New(),
		items:     make(map[string]*list.Element, size),
		db:        db,
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		cur := b.Cursor()
		var key string
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			key = string(k)
			el := c.evictList.PushFront(key)
			c.items[key] = el
		}
		return nil
	})
	return c, nil
}

func (c *Cache) Add(key string, value []byte) error {
	return c.AddMulti(map[string][]byte{key: value})
}

func (c *Cache) AddMulti(data map[string][]byte) error {
	keysToRemove := make([]string, 0)
	c.lock.Lock()
	for key, _ := range data {
		if el, ok := c.items[key]; ok {
			c.evictList.MoveToFront(el)
		} else {
			el := c.evictList.PushFront(key)
			c.items[key] = el
			evict := c.evictList.Len() > c.size
			if evict {
				el := c.evictList.Back()
				if el != nil {
					key := el.Value.(string)
					c.evictList.Remove(el)
					delete(c.items, key)
					keysToRemove = append(keysToRemove, key)
				}
			}
		}
	}
	c.lock.Unlock()
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		var err error
		for _, k := range keysToRemove {
			err = b.Delete([]byte(k))
			if err != nil {
				return err
			}
		}

		for key, value := range data {
			err = b.Put([]byte(key), value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Cache) Get(key string) (value []byte, err error) {
	data, err := c.MultiGet([]string{key})
	if err != nil {
		return
	}
	value, ok := data[key]
	if !ok {
		err = errors.New("not found")
	}
	return
}

func (c *Cache) MultiGet(keys []string) (values map[string][]byte, err error) {
	c.lock.Lock()
	existsKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if el, ok := c.items[k]; ok {
			existsKeys = append(existsKeys, k)
			c.evictList.MoveToFront(el)
		}
	}
	c.lock.Unlock()
	values = make(map[string][]byte, len(existsKeys))
	err = c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, k := range existsKeys {
			values[k] = b.Get([]byte(k))
		}
		return nil
	})
	return
}

func (c *Cache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.evictList.Len()
}

func (c *Cache) Close() {
	c.db.Close()
}