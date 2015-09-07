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
	c.lock.Lock()
	defer c.lock.Unlock()

	if el, ok := c.items[key]; ok {
		c.evictList.MoveToFront(el)
	} else {
		el := c.evictList.PushFront(key)
		c.items[key] = el
		evict := c.evictList.Len() > c.size
		if evict {
			err := c.removeOldest()
			if err != nil {
				return err
			}
		}
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Put([]byte(key), value)
	})
}

func (c *Cache) Get(key string) (value []byte, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if el, ok := c.items[key]; ok {
		err = c.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			value = b.Get([]byte(key))
			return nil
		})
		if err == nil {
			c.evictList.MoveToFront(el)
		}
		return
	}
	return nil, errors.New("not found")
}

func (c *Cache) removeOldest() error {
	el := c.evictList.Back()
	if el != nil {
		key := el.Value.(string)
		c.evictList.Remove(el)
		delete(c.items, key)
		return c.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			return b.Delete([]byte(key))
		})
	}
	return nil
}

func (c *Cache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.evictList.Len()
}
