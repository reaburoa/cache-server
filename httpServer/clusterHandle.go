package httpServer

import (
    "net/http"
    "encoding/json"
    "log"
)

type clusterHandler struct {
    *Server
}

func (h *clusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }
    m := h.Members()
    b, e := json.Marshal(m)
    if e != nil {
        log.Println(e)
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    w.Write(b)
}

func (s *Server) clusterHandler() http.Handler {
    return &clusterHandler{s}
}
