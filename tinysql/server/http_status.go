// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/gorilla/mux"
	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
)

const defaultStatusPort = 10080

func (s *Server) startStatusHTTP() {
	go s.startHTTPServer()
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()

	addr := fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		addr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, defaultStatusPort)
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	tidbDriver, ok := s.driver.(*TiDBDriver)
	if ok {
		tinykvStore, ok := tidbDriver.store.(*tikv.TinykvStore)
		if ok {
			regionHandler := newRegionHandler(tinykvStore.GetRegionCache())
			serverMux.HandleFunc("/regions", regionHandler.ServeHTTP)

			storeHanlder := newStoreHandler(tinykvStore.PdClient)
			serverMux.HandleFunc("/stores", storeHanlder.ServeHTTP)
		}
	}

	var (
		httpRouterPage bytes.Buffer
		pathTemplate   string
		err            error
	)

	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			logutil.BgLogger().Error("get HTTP router path failed", zap.Error(err))
		}
		name := route.GetName()
		// If the name attribute is not set, GetName returns "".
		if name != "" {
			httpRouterPage.WriteString("<tr><td><a href='" + pathTemplate + "'>" + name + "</a><td></tr>")
		}
		return nil
	})
	if err != nil {
		logutil.BgLogger().Error("generate root failed", zap.Error(err))
	}
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		_, err = responseWriter.Write([]byte(httpRouterPage.String()))
		if err != nil {
			logutil.BgLogger().Error("write HTTP index page failed", zap.Error(err))
		}
	})

	s.setupStatusServer(addr, serverMux)
}

func (s *Server) setupStatusServer(addr string, serverMux *http.ServeMux) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logutil.BgLogger().Info("listen failed", zap.Error(err))
		return
	}
	m := cmux.New(l)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())

	s.statusServer = &http.Server{Addr: addr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}

	go util.WithRecovery(func() {
		err := s.statusServer.Serve(httpL)
		logutil.BgLogger().Error("http server error", zap.Error(err))
	}, nil)
	err = m.Serve()
	if err != nil {
		logutil.BgLogger().Error("start status/rpc server error", zap.Error(err))
	}
}

type regionHandler struct {
	regionCache *tikv.RegionCache
}

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		writeError(w, err)
		return
	}
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
}

func newRegionHandler(regionCache *tikv.RegionCache) *regionHandler {
	handler := &regionHandler{
		regionCache: regionCache,
	}
	return handler
}

// ServeHTTP handles request of get all region information.
func (h *regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	ctx := context.Background()
	regions, err := h.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(ctx, 20000), []byte(""), []byte("z"))
	if err != nil {
		writeError(w, err)
	}
	regionInfo := make(map[uint64]interface{})
	type regionResult struct {
		Meta     *metapb.Region `json:"meta"`
		LeaderID uint64         `json:"leaderID"`
	}

	for _, region := range regions {
		regionInfo[region.GetID()] = &regionResult{
			Meta:     region.GetMeta(),
			LeaderID: region.GetLeaderID(),
		}
	}
	logutil.Logger(context.Background()).Info("handle http get region results", zap.Int("regions", len(regions)))
	writeData(w, regionInfo)
}

type storeHandler struct {
	client pd.Client
}

func newStoreHandler(pdClient pd.Client) *storeHandler {
	handler := &storeHandler{
		client: pdClient,
	}
	return handler
}

// ServeHTTP handles request of get all store information.
func (h *storeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	ctx := context.Background()
	stores, err := h.client.GetAllStores(ctx)
	if err != nil {
		writeError(w, err)
	}
	storeInfo := make(map[uint64]interface{})
	for _, store := range stores {
		storeInfo[store.GetId()] = store
	}
	logutil.Logger(context.Background()).Info("handle http get store results", zap.Int("stores", len(storeInfo)))
	writeData(w, storeInfo)
}
