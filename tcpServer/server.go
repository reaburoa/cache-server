package tcpServer

import (
    "../cache"
    "../cluster"
    "net"
)

type result struct {
    v []byte
    e error
}

type Server struct {
    cache.Cache
    cluster.Node
}

func (s *Server) Listen() {
    l, e := net.Listen("tcp", s.Addr() + ":12346")
    if e != nil {
        panic(e)
    }

    for {
        c, e := l.Accept()
        if e != nil {
            panic(e)
        }

        go s.process(c)
    }
}

func New(c cache.Cache, n cluster.Node) *Server {
    return &Server{c, n}
}
