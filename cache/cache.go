package cache

import (
    "log"
    "time"
)

type Stat struct {
    Count     int64
    KeySize   int64
    ValueSize int64
}

type value struct {
    v []byte
    created time.Time
}

type pair struct {
    k string
    v []byte
}

type Cache interface {
    Set(string, []byte) error
    Get(string) ([]byte, error)
    Del(string) error
    GetStat() Stat
    NewScanner() Scanner
}

func (s *Stat) add(k string, v []byte) {
    s.Count ++
    s.KeySize += int64(len(k))
    s.ValueSize += int64(len(v))
}

func (s *Stat) del(k string, v []byte) {
    s.Count --;
    s.KeySize -= int64(len(k))
    s.ValueSize -= int64(len(v))
}

func New(typ string, ttl int) Cache {
    var c Cache
    if typ == "inMemory" {
        c = newInMemoryCache(ttl)
    }
    if typ == "rocksdb" {
        c = newRocksDbCache(ttl)
    }
    if c == nil {
        panic("unknown cache type "+typ)
    }

    log.Println(typ, "ready to serve")

    return c
}
