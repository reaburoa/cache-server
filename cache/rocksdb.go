package cache

// #include "rocksdb/c.h"
// #include <stdlib.h>
// #cgo CFLAGS: -I${SRCDIR}/../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"

import (
    "runtime"
    "unsafe"
    "errors"
    "regexp"
    "strconv"
    "time"
)

const BATCH_SIZE = 100

type rocksDbCache struct {
    db *C.rocksdb_t
    ro *C.rocksdb_readoptions_t
    wo *C.rocksdb_writeoptions_t
    e *C.char
    ch chan *pair
}

func flush_batch(db *C.rocksdb_t, b *C.rocksdb_writebatch_t, o *C.rocksdb_writeoptions_t) {
    var e *C.char
    C.rocksdb_write(db, o, b, &e)
    if e != nil {
        panic(C.GoString(e))
    }
    C.rocksdb_writebatch_clear(b)
}

func write_func(db *C.rocksdb_t, c chan *pair, o *C.rocksdb_writeoptions_t) {
    count := 0
    t := time.NewTimer(time.Second)
    b := C.rocksdb_writebatch_create()
    for {
        select {
        case p := <-c:
            count ++
            key := C.CString(p.k)
            value := C.CBytes(p.v)
            C.rocksdb_writebatch_put(b, key, C.size_t(len(p.k)), (*C.char)(value), C.size_t(len(p.v)))
            C.free(unsafe.Pointer(key))
            C.free(value)
            if count == BATCH_SIZE {
                flush_batch(db, b, o)
                count = 0
            }
            if !t.Stop() {
                <- t.C
            }
            t.Reset(time.Second)
        case <- t.C:
            if count != 0 {
                flush_batch(db, b, o)
                count = 0
            }
            t.Reset(time.Second)
        }
    }
}

func newRocksDbCache(ttl int) *rocksDbCache {
    options := C.rocksdb_options_create()
    C.rocksdb_options_increase_parallelism(options, C.int(runtime.NumCPU()))
    C.rocksdb_options_set_create_if_missing(options, 1)
    var e *C.char
    //db := C.rocksdb_open(options, C.CString("/tmp/rocksdb"), &e)
    db := C.rocksdb_open_with_ttl(options, C.CString("/tmp/rocksdb"), C.int(ttl), &e)
    if e != nil {
        panic(C.GoString(e))
    }
    C.rocksdb_options_destroy(options)
    c := make(chan *pair, 5000)
    wo := C.rocksdb_writeoptions_create()
    go write_func(db, c, wo)
    return &rocksDbCache{db, C.rocksdb_readoptions_create(), wo, e, c}
}

func (c *rocksDbCache) Get(key string) ([]byte, error) {
    k := C.CString(key)
    defer C.free(unsafe.Pointer(k))
    var length C.size_t
    v := C.rocksdb_get(c.db, c.ro, k, C.size_t(len(key)), &length, &c.e)
    if c.e != nil {
        return nil, errors.New(C.GoString(c.e))
    }
    defer C.free(unsafe.Pointer(v))
    return C.GoBytes(unsafe.Pointer(v), C.int(length)), nil
}

func (c *rocksDbCache) Set(key string, value []byte) error {
    /*k := C.CString(key)
    defer C.free(unsafe.Pointer(k))
    v := C.CBytes(value)
    defer C.free(v)
    C.rocksdb_put(c.db, c.wo, k, C.size_t(len(key)), (*C.char)(v), C.size_t(len(value)), &c.e)
    if c.e != nil {
        return errors.New(C.GoString(c.e))
    }*/
    c.ch <- &pair{key, value}
    return nil
}

func (c *rocksDbCache) Del(key string) error {
    k := C.CString(key)
    defer C.free(unsafe.Pointer(k))
    C.rocksdb_delete(c.db, c.wo, k, C.size_t(len(key)), &c.e)
    if c.e != nil {
        return errors.New(C.GoString(c.e))
    }
    return nil
}

func (c *rocksDbCache) GetStat() Stat {
    k := C.CString("rocksdb.aggregated-table-properties")
    defer C.free(unsafe.Pointer(k))
    v := C.rocksdb_property_value(c.db, k)
    defer C.free(unsafe.Pointer(v))
    p := C.GoString(v)
    r := regexp.MustCompile(`([^;]+)=([^;]+);`)
    s := Stat{}
    for _, submatches := range r.FindAllStringSubmatch(p, -1) {
        if submatches[1] == " # entries" {
            s.Count, _ = strconv.ParseInt(submatches[2], 10, 64)
        }
        if submatches[1] == " raw key size" {
            s.KeySize, _ = strconv.ParseInt(submatches[2], 10, 64)
        }
        if submatches[1] == " raw value size" {
            s.ValueSize, _ = strconv.ParseInt(submatches[2], 10, 64)
        }
    }

    return s
}
