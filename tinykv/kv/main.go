package main

import (
	"flag"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/server"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	schedulerAddr = flag.String("scheduler", "", "scheduler address")
	storeAddr     = flag.String("addr", "", "store address")
	dbPath        = flag.String("path", "", "directory path of db")
	logLevel      = flag.String("loglevel", "info", "the level of log")
)

func main() {
	flag.Parse()
	conf := config.NewDefaultConfig()
	if *schedulerAddr != "" {
		conf.SchedulerAddr = *schedulerAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	if *dbPath != "" {
		conf.DBPath = *dbPath
	}
	if *logLevel != "" {
		conf.LogLevel = *logLevel
	}

	log.SetLevel(logutil.StringToZapLogLevel(conf.LogLevel))
	log.Info(fmt.Sprintf("Server started with conf %+v", conf))

	var storage storage.Storage
	if conf.Raft {
		storage = raft_storage.NewRaftStorage(conf)
	} else {
		storage = standalone_storage.NewStandAloneStorage(conf)
	}
	schedulerClient, err := scheduler_client.NewClient(strings.Split(conf.SchedulerAddr, ","), "")
	if err != nil {
		log.Fatal("new scheduler client failed", zap.Error(err))
	}
	if err := storage.Start(schedulerClient); err != nil {
		log.Fatal("start failed", zap.Error(err))
	}
	server := server.NewServer(storage)

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(1<<30),
		grpc.InitialConnWindowSize(1<<30),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	tinykvpb.RegisterTinyKvServer(grpcServer, server)
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("listen failed", zap.Error(err))
	}
	handleSignal(grpcServer)

	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal("grpc serve failed", zap.Error(err))
	}
	log.Info("Server stopped.")
}

func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sigCh
		log.Info(fmt.Sprintf("Got signal [%s] to exit.", sig))
		grpcServer.Stop()
	}()
}
