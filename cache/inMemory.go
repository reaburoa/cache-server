package cache

import (
    "sync"
    "time"
)

type inMemoryCache struct {
    c map[string]value
    mutex sync.RWMutex
    Stat
    ttl time.Duration
}

func (c *inMemoryCache) Set(k string, v []byte) error {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    tmp, exists := c.c[k]
    if exists {
        c.del(k, tmp.v)
    }
    c.c[k] = value{v, time.Now()}
    c.Stat.add(k, v)
    return nil
}

func (c *inMemoryCache) Get(k string) ([]byte, error) {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    return c.c[k].v, nil
}

func (c *inMemoryCache) Del(k string) error {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    v, exists := c.c[k]
    if exists {
        delete(c.c, k)
        c.del(k, v.v)
    }
    return nil
}

func (c *inMemoryCache) GetStat() Stat {
    return c.Stat
}

func (c *inMemoryCache) expier() {
    for {
        time.Sleep(c.ttl)
        c.mutex.RLock()
        for k, v := range c.c {
            c.mutex.RUnlock()
            if v.created.Add(c.ttl).Before(time.Now()) {
                c.Del(k)
            }
            c.mutex.RLock()
        }
        c.mutex.RUnlock()
    }
}

func newInMemoryCache(ttl int) *inMemoryCache {
    c := &inMemoryCache{make(map[string]value), sync.RWMutex{}, Stat{}, time.Duration(ttl) * time.Second}
    if ttl > 0 {
        go c.expier()
    }
    return c
}
