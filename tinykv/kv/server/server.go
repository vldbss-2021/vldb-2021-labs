package server

import (
	"context"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/commands"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
// It implements the `TinyKvServer` interface, these interface will be used by the tinysql server or the tinykv
// client.
type Server struct {
	storage    storage.Storage
	Latches    *latches.Latches
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// Run runs a transactional command.
func (server *Server) Run(cmd commands.Command) (interface{}, error) {
	return commands.RunCommand(cmd, server.storage, server.Latches)
}

// The below functions are Server's gRPC API (implements TinyKvServer).
// Transactional API.
// The transaction version is used to guarantee the snapshot isolation level. For the transaction isolation level,
// check https://docs.pingcap.com/tidb/stable/transaction-isolation-levels for more information.

// KvGet returns the value of the key, the visibility is judged by the `Version` field of `GetRequest`.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	cmd := commands.NewGet(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.GetResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.GetResponse), err
}

// KvScan returns the valuee of all the keys in a specific range defined by the `startKey` of the ScanRequest.
// The visibility is judged by the `Version` field of `ScanRequest`.
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {

	cmd := commands.NewScan(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.ScanResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.ScanResponse), err
}

// KvPrewrite is the main entry of transactional write, the first stage of 2PC.
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	cmd := commands.NewPrewrite(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.PrewriteResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.PrewriteResponse), err
}

// KvCommit is the main entry of transactional write, the second stage of 2PC.
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	cmd := commands.NewCommit(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.CommitResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.CommitResponse), err
}

// KvCheckTxnStatus is the main entry of transaction conflict process. If the read/write is blocked by the ongoing
// transaction prewrite lock, the blocked transaction will use `KvCheckTxnStatus` to verify the status of the transaction
// owning the lock. If the blocking lock is decided to be commit/rollback, the blocked read/write transaction could resolve
// this lock then continues the read/write operations.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	cmd := commands.NewCheckTxnStatus(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.CheckTxnStatusResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.CheckTxnStatusResponse), err
}

// KvBatchRollback is used rollback the transaction lock keys if the transaction will NOT commit.
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	cmd := commands.NewRollback(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.BatchRollbackResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.BatchRollbackResponse), err
}

// KvResolveLock is used to resolve the prewrite lock if the related transaction status is decided(commit/rollback).
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	cmd := commands.NewResolveLock(req)
	resp, err := server.Run(&cmd)
	if err != nil {
		resp, err = regionError(err, new(kvrpcpb.ResolveLockResponse))
		if err != nil {
			return nil, err
		}
	}
	return resp.(*kvrpcpb.ResolveLockResponse), err
}

// Raw API. These commands are handled inline rather than by using Run and am implementation of the Commands interface.
// This is because these commands are fairly straightforward and do not share a lot of code with the transactional
// commands.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	response := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if !rawRegionError(err, response) {
		val, err := reader.GetCF(req.Cf, req.Key)
		if err != nil {
			rawRegionError(err, response)
		} else if val == nil {
			response.NotFound = true
		} else {
			response.Value = val
		}
	}

	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	response := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}}})
	rawRegionError(err, response)
	return response, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	response := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}}})
	rawRegionError(err, response)
	return response, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	response := new(kvrpcpb.RawScanResponse)

	reader, err := server.storage.Reader(req.Context)
	if !rawRegionError(err, response) {
		// To scan, we need to get an iterator for the underlying storage.
		it := reader.IterCF(req.Cf)
		defer it.Close()
		// Initialize the iterator. Termination condition is that the iterator is still valid (i.e.
		// we have not reached the end of the DB) and we haven't exceeded the client-specified limit.
		for it.Seek(req.StartKey); it.Valid() && len(response.Kvs) < int(req.Limit); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				rawRegionError(err, response)
				break
			} else {
				response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
					Key:   key,
					Value: value,
				})
			}
		}
	}

	return response, nil
}

// Raft commands (tinykv <-> tinykv); these are trivially forwarded to storage.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp, err := regionError(err, resp)
		if err != nil {
			return nil, err
		}
		return resp.(*coppb.Response), err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

// rawRegionError assigns region errors to a RegionError field, and other errors to the Error field,
// of resp. This is only a valid way to handle errors for the raw commands. Returns true if err is
// non-nil, false otherwise.
func rawRegionError(err error, resp interface{}) bool {
	if err == nil {
		return false
	}
	respValue := reflect.ValueOf(resp).Elem()
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionError.RequestErr))
	} else {
		respValue.FieldByName("Error").Set(reflect.ValueOf(err.Error()))
	}
	return true
}

// regionError is a help method for handling region errors. If error is a region error, then it is added to resp (which
// muse have a `regionError` field; the response is returned. If the error is not a region error, then regionError returns
// nil and the error.
func regionError(err error, resp interface{}) (interface{}, error) {
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		respValue := reflect.ValueOf(resp)
		regionErrorFieldVal := reflect.Indirect(respValue).FieldByName("RegionError")
		if !regionErrorFieldVal.IsValid() {
			log.Error("respValue.RequestErr is invalid")
			return nil, errors.New("unexpected error combine region error resp.FieldByName")
		}
		requestErrVal := reflect.ValueOf(regionError.RequestErr)
		if !requestErrVal.IsValid() {
			log.Error("regionError.RequestErr is invalid")
			return nil, errors.New("unexpected error combine region error regionError.RequestErr")
		}
		regionErrorFieldVal.Set(requestErrVal)
		return resp, nil
	}

	return nil, err
}
