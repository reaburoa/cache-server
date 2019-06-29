package main

import (
    "./cache"
    "./httpServer"
    "./tcpServer"
    "./cluster"
    "flag"
    "log"
)

func main() {
    typ := flag.String("type", "inMemory", "cache type, inMemory|rocksdb")
    ttl := flag.Int("ttl", 30, "cache time to live")
    node := flag.String("node", "127.0.0.1", "node address")
    clus := flag.String("cluster", "", "cluster address")
    flag.Parse()
    log.Println("Type is ", *typ)
    log.Println("ttl is ", *ttl)
    log.Println("node is ", *node)
    log.Println("cluster is ", *clus)
    c := cache.New(*typ, *ttl)
    n, e := cluster.New(*node, *clus)
    if e != nil {
        panic(e)
    }
    go tcpServer.New(c, n).Listen()
    httpServer.New(c, n).Listen()
}
