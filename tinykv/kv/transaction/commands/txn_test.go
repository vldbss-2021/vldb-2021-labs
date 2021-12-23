package commands

import (
	"bytes"
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/kv/test_raftstore"
	latches2 "github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

const pathPrefix = "/tmp/badger_test_store"

func genPhyTS(phyTS uint64) uint64 {
	return phyTS << tsoutil.PhysicalShiftBits
}

func newTestConf() *config.Config {
	conf := config.NewDefaultConfig()
	t := time.Now()
	tstr := t.Format("20060102150405")
	dbPath := path.Join(pathPrefix, tstr)
	log.Info("test store path", zap.String("path", dbPath))
	conf.DBPath = dbPath
	return conf
}

func newTestStore() storage.Storage {
	conf := newTestConf()
	store := standalone_storage.NewStandAloneStorage(conf)
	err := store.Start(nil)
	if err != nil {
		log.Fatal("start test store failed", zap.Error(err))
	}
	return store
}

func newTestRaftStore() storage.Storage {
	conf := newTestConf()
	mockClient := test_raftstore.NewMockSchedulerClient(1, 1)
	raftStore := raft_storage.NewRaftStorage(conf)
	err := raftStore.Start(mockClient)
	if err != nil {
		log.Fatal("start raft store failed", zap.Error(err))
	}
	return raftStore
}

func TestBasicRaftStore(t *testing.T) {
	store := newTestRaftStore()
	stopTestStore(store)
}

func stopTestStore(store storage.Storage) {
	err := store.Stop()
	if err != nil {
		log.Fatal("stop store failed", zap.Error(err))
	}
	err = os.RemoveAll(pathPrefix)
	if err != nil {
		log.Error("failed to remove the test store directory", zap.Error(err))
	}
}

// newInitRegionKVCtx creates a rpc context for the initial region access, note the region, peer
// and term information should be compatible with the raft store implementation.
func newInitRegionKVCtx(client scheduler_client.Client) *kvrpcpb.Context {
	if client == nil {
		return nil
	}
	sleepDuration := 100 * time.Millisecond
	checkLimit := 50
	checkCnt := 0
	var region *metapb.Region
	var peer *metapb.Peer
	var err error
	for {
		region, peer, err = client.GetRegion(context.Background(), meta.PrepareBootstrapKey)
		if err != nil {
			log.Fatal("failed to get init region", zap.Error(err), zap.Stringer("region", region), zap.Stringer("peer", peer))
		}
		if region == nil || peer == nil {
			log.Debug("region info is unavailable now, possible it's not heartbeat yet", zap.Duration("sleep", sleepDuration))
			time.Sleep(sleepDuration)
			checkCnt++
			if checkCnt > checkLimit {
				log.Fatal("failed to get init region", zap.Error(err), zap.Stringer("region", region), zap.Stringer("peer", peer))
			}
			continue
		}
		log.Debug("the test region and peer", zap.Stringer("region", region), zap.Stringer("peer", peer))
		break
	}
	ctx := &kvrpcpb.Context{
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Peer:        peer,
		Term:        meta.RaftInitLogTerm,
	}
	return ctx
}

func mustPrewritePut(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte,
	value []byte, primaryKey []byte, startTS uint64, lockTTL uint64) {
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key,
		Value: value,
	}
	req := &kvrpcpb.PrewriteRequest{
		Context:      newInitRegionKVCtx(store.Client()),
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      lockTTL,
	}
	prewriteCmd := NewPrewrite(req)
	resp, err := RunCommand(&prewriteCmd, store, latches)
	assert.Nil(t, err)
	prewriteResp, ok := resp.(*kvrpcpb.PrewriteResponse)
	assert.True(t, ok)
	assert.Equal(t, 0, len(prewriteResp.GetErrors()))
	assert.Nil(t, prewriteResp.GetRegionError())
}

func mustPrewriteDel(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte,
	primaryKey []byte, startTS uint64, lockTTL uint64) {
	mut := &kvrpcpb.Mutation{
		Op:  kvrpcpb.Op_Del,
		Key: key,
	}
	req := &kvrpcpb.PrewriteRequest{
		Context:      newInitRegionKVCtx(store.Client()),
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      lockTTL,
	}
	prewriteCmd := NewPrewrite(req)
	resp, err := RunCommand(&prewriteCmd, store, latches)
	assert.Nil(t, err)
	prewriteResp, ok := resp.(*kvrpcpb.PrewriteResponse)
	assert.True(t, ok)
	assert.Equal(t, 0, len(prewriteResp.GetErrors()))
	assert.Nil(t, prewriteResp.GetRegionError())
}

func mustPrewritePutErr(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte,
	value []byte, primaryKey []byte, startTS uint64, lockTTL uint64) {
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Op_Put,
		Key:   key,
		Value: value,
	}
	req := &kvrpcpb.PrewriteRequest{
		Context:      newInitRegionKVCtx(store.Client()),
		Mutations:    []*kvrpcpb.Mutation{mut},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      lockTTL,
	}
	prewriteCmd := NewPrewrite(req)
	resp, err := RunCommand(&prewriteCmd, store, latches)
	assert.Nil(t, err)
	prewriteResp, ok := resp.(*kvrpcpb.PrewriteResponse)
	assert.True(t, ok)
	assert.Equal(t, 1, len(prewriteResp.GetErrors()))
	assert.Nil(t, prewriteResp.GetRegionError())
}

func mustCommit(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte, startTS uint64, commitTS uint64) {
	req := &kvrpcpb.CommitRequest{
		Context:       newInitRegionKVCtx(store.Client()),
		StartVersion:  startTS,
		Keys:          [][]byte{key},
		CommitVersion: commitTS,
	}
	commitCmd := NewCommit(req)
	resp, err := RunCommand(&commitCmd, store, latches)
	assert.Nil(t, err)
	commitResp, ok := resp.(*kvrpcpb.CommitResponse)
	assert.True(t, ok)
	assert.Nil(t, commitResp.GetRegionError())
	assert.Nil(t, commitResp.GetError())
}

func mustCommitErr(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte, startTS uint64, commitTS uint64) {
	req := &kvrpcpb.CommitRequest{
		Context:       newInitRegionKVCtx(store.Client()),
		StartVersion:  startTS,
		Keys:          [][]byte{key},
		CommitVersion: commitTS,
	}
	commitCmd := NewCommit(req)
	resp, err := RunCommand(&commitCmd, store, latches)
	assert.Nil(t, err)
	commitResp, ok := resp.(*kvrpcpb.CommitResponse)
	assert.True(t, ok)
	assert.NotNil(t, commitResp.GetError())
}

func mustGetKV(t *testing.T, store storage.Storage, latches *latches2.Latches, key []byte, readTS uint64, expectedVal []byte) {
	req := &kvrpcpb.GetRequest{
		Context: newInitRegionKVCtx(store.Client()),
		Key:     key,
		Version: readTS,
	}
	getCmd := NewGet(req)
	resp, err := RunCommand(&getCmd, store, latches)
	assert.Nil(t, err)
	getResp, ok := resp.(*kvrpcpb.GetResponse)
	assert.True(t, ok)
	if len(getResp.Value) > 0 {
		assert.True(t, bytes.Equal(expectedVal, getResp.Value))
	} else {
		assert.Equal(t, len(expectedVal), len(getResp.Value))
	}
}

func checkTxnStatusResp(t *testing.T, store storage.Storage, latches *latches2.Latches, primaryLockKey []byte,
	lockStartTS uint64, currentTS uint64) *kvrpcpb.CheckTxnStatusResponse {
	req := &kvrpcpb.CheckTxnStatusRequest{
		Context:    newInitRegionKVCtx(store.Client()),
		PrimaryKey: primaryLockKey,
		LockTs:     lockStartTS,
		CurrentTs:  currentTS,
	}
	checkTxnStatCmd := NewCheckTxnStatus(req)
	resp, err := RunCommand(&checkTxnStatCmd, store, latches)
	assert.Nil(t, err)
	checkTxnStatResp, ok := resp.(*kvrpcpb.CheckTxnStatusResponse)
	assert.True(t, ok)
	return checkTxnStatResp
}

func mustCheckTxnStatusTTLExpireRollback(t *testing.T, store storage.Storage, latches *latches2.Latches,
	primaryLockKey []byte, lockStartTs uint64, currentTS uint64) {
	resp := checkTxnStatusResp(t, store, latches, primaryLockKey, lockStartTs, currentTS)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	assert.Equal(t, uint64(0), resp.LockTtl)
	assert.Equal(t, kvrpcpb.Action_TTLExpireRollback, resp.Action)
}

func mustCheckTxnStatusLockNotExistRollback(t *testing.T, store storage.Storage, latches *latches2.Latches,
	primaryLockKey []byte, lockStartTs uint64, currentTS uint64) {
	resp := checkTxnStatusResp(t, store, latches, primaryLockKey, lockStartTs, currentTS)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	assert.Equal(t, uint64(0), resp.LockTtl)
	assert.Equal(t, kvrpcpb.Action_LockNotExistRollback, resp.Action)
}

func mustCheckTxnStatusAlreadyRollback(t *testing.T, store storage.Storage, latches *latches2.Latches,
	primaryLockKey []byte, lockStartTs uint64, currentTS uint64) {
	resp := checkTxnStatusResp(t, store, latches, primaryLockKey, lockStartTs, currentTS)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	assert.Equal(t, uint64(0), resp.LockTtl)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
}

func mustCheckTxnStatusCommitted(t *testing.T, store storage.Storage, latches *latches2.Latches,
	primaryLockKey []byte, lockStartTs uint64, currentTS uint64, expectedCommitTS uint64) {
	resp := checkTxnStatusResp(t, store, latches, primaryLockKey, lockStartTs, currentTS)
	assert.Equal(t, expectedCommitTS, resp.CommitVersion)
	assert.Equal(t, uint64(0), resp.LockTtl)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
}

func mustCheckTxnStatusLocked(t *testing.T, store storage.Storage, latches *latches2.Latches,
	primaryLockKey []byte, lockStartTs uint64, currentTS uint64, expectedTTL uint64) {
	resp := checkTxnStatusResp(t, store, latches, primaryLockKey, lockStartTs, currentTS)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	assert.Equal(t, expectedTTL, resp.LockTtl)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
}

func mustRollback(t *testing.T, store storage.Storage, latches *latches2.Latches,
	key []byte, startTS uint64) {
	req := &kvrpcpb.BatchRollbackRequest{
		Context:      newInitRegionKVCtx(store.Client()),
		StartVersion: startTS,
		Keys:         [][]byte{key},
	}
	rollbackCmd := NewRollback(req)
	resp, err := RunCommand(&rollbackCmd, store, latches)
	assert.Nil(t, err)
	rollbackResp, ok := resp.(*kvrpcpb.BatchRollbackResponse)
	assert.True(t, ok)
	assert.Nil(t, rollbackResp.GetRegionError())
	assert.Nil(t, rollbackResp.GetError())
}

func mustRollbackErr(t *testing.T, store storage.Storage, latches *latches2.Latches,
	key []byte, startTS uint64) {
	req := &kvrpcpb.BatchRollbackRequest{
		Context:      newInitRegionKVCtx(store.Client()),
		StartVersion: startTS,
		Keys:         [][]byte{key},
	}
	rollbackCmd := NewRollback(req)
	resp, err := RunCommand(&rollbackCmd, store, latches)
	assert.Nil(t, err)
	rollbackResp, ok := resp.(*kvrpcpb.BatchRollbackResponse)
	assert.True(t, ok)
	assert.Nil(t, rollbackResp.GetRegionError())
	assert.NotNil(t, rollbackResp.GetError())
}

func mustResolveLock(t *testing.T, store storage.Storage, latches *latches2.Latches,
	startTS uint64, commitTS uint64) {
	req := &kvrpcpb.ResolveLockRequest{
		Context:       newInitRegionKVCtx(store.Client()),
		StartVersion:  startTS,
		CommitVersion: commitTS,
	}
	resolveLockCmd := NewResolveLock(req)
	resp, err := RunCommand(&resolveLockCmd, store, latches)
	assert.Nil(t, err)
	resolveResp, ok := resp.(*kvrpcpb.ResolveLockResponse)
	assert.True(t, ok)
	assert.Nil(t, resolveResp.GetRegionError())
	assert.Nil(t, resolveResp.GetError())
}

func TestBasicTestStoreLab2P1(t *testing.T) {
	store := newTestRaftStore()
	stopTestStore(store)
}

func TestBasicReadWriteLab2P1(t *testing.T) {
	store := newTestRaftStore()
	defer stopTestStore(store)
	latches := latches2.NewLatches()

	k1 := []byte("tk1")
	v1 := []byte("v1")
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustCommit(t, store, latches, k1, genPhyTS(1), genPhyTS(2))

	// Use new ts and old ts to read the committed kv.
	mustGetKV(t, store, latches, k1, genPhyTS(2), v1)
	mustGetKV(t, store, latches, k1, genPhyTS(1), nil)
}

func TestBasicCheckTxnStatusLab2P2(t *testing.T) {
	store := newTestRaftStore()
	defer stopTestStore(store)
	latches := latches2.NewLatches()

	k1 := []byte("tk1")
	v1 := []byte("v1")
	// Test the ttl expire rollback. The following commit will fail.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustCheckTxnStatusTTLExpireRollback(t, store, latches, k1, genPhyTS(1), genPhyTS(12))
	mustCommitErr(t, store, latches, k1, genPhyTS(1), genPhyTS(5))

	// Test the lock not exist rollback. The following prewrite will fail.
	mustCheckTxnStatusLockNotExistRollback(t, store, latches, k1, genPhyTS(11), genPhyTS(22))
	mustPrewritePutErr(t, store, latches, k1, v1, k1, genPhyTS(11), 10)

	// Test the lock not expire path.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(21), 7)
	mustCheckTxnStatusLocked(t, store, latches, k1, genPhyTS(21), genPhyTS(22), 7)

	// Test the transaction is already committed.
	mustCommit(t, store, latches, k1, genPhyTS(21), genPhyTS(25))
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(21), genPhyTS(30), genPhyTS(25))
}

func TestBasicRollbackLab2P2(t *testing.T) {
	store := newTestRaftStore()
	defer stopTestStore(store)
	latches := latches2.NewLatches()

	k1 := []byte("tk1")
	v1 := []byte("v1")
	k2 := []byte("tk2")
	v2 := []byte("v2")
	// The primary lock is rolled back by the check txn status call, the secondary lock is rolled back.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustPrewritePut(t, store, latches, k2, v2, k1, genPhyTS(1), 10)
	mustCheckTxnStatusTTLExpireRollback(t, store, latches, k1, genPhyTS(1), genPhyTS(12))
	mustCommitErr(t, store, latches, k1, genPhyTS(1), genPhyTS(5))
	mustRollback(t, store, latches, k1, genPhyTS(1))
	mustRollback(t, store, latches, k2, genPhyTS(1))
	mustCommitErr(t, store, latches, k2, genPhyTS(1), genPhyTS(5))

	// The key is already committed, the rollback should fail.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(11), 10)
	mustCommit(t, store, latches, k1, genPhyTS(11), genPhyTS(12))
	mustGetKV(t, store, latches, k1, genPhyTS(12), v1)
	mustRollbackErr(t, store, latches, k1, genPhyTS(11))

	// There's no prewrite lock and write records, write rollback anyway, future prewrite
	// with same start_ts should fail.
	k3 := []byte("tk3")
	v3 := []byte("v3")
	mustRollback(t, store, latches, k3, genPhyTS(20))
	mustPrewritePutErr(t, store, latches, k3, v3, k3, genPhyTS(20), 10)
}

func TestBasicResolveLab2P3(t *testing.T) {
	store := newTestRaftStore()
	defer stopTestStore(store)
	latches := latches2.NewLatches()

	k1 := []byte("tk1")
	v1 := []byte("v1")
	k2 := []byte("tk2")
	v2 := []byte("v2")
	v11 := []byte("v11")
	v22 := []byte("v22")
	// The key is locked, some other transaction ties to resolve this lock.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustCommit(t, store, latches, k1, genPhyTS(1), genPhyTS(5))
	mustPrewritePut(t, store, latches, k1, v2, k1, genPhyTS(11), 8)

	// The key is still locked, keep wait. Then it's expired.
	mustCheckTxnStatusLocked(t, store, latches, k1, genPhyTS(11), genPhyTS(12), 8)
	mustResolveLock(t, store, latches, genPhyTS(11), genPhyTS(0))
	mustCommitErr(t, store, latches, k1, genPhyTS(11), genPhyTS(12))

	// The key is still locked, keep wait. Then it's committed.
	mustPrewritePut(t, store, latches, k1, v11, k1, genPhyTS(21), 9)
	mustPrewritePut(t, store, latches, k2, v22, k1, genPhyTS(21), 9)
	mustCommit(t, store, latches, k1, genPhyTS(21), genPhyTS(25))
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(21), genPhyTS(31), genPhyTS(25))
	mustResolveLock(t, store, latches, genPhyTS(21), genPhyTS(25))
	mustGetKV(t, store, latches, k2, genPhyTS(32), v22)
	mustGetKV(t, store, latches, k1, genPhyTS(32), v11)
	mustCommit(t, store, latches, k1, genPhyTS(21), genPhyTS(25))
	mustCommit(t, store, latches, k2, genPhyTS(21), genPhyTS(25))
}

func TestBasicIdempotentLab2P3(t *testing.T) {
	store := newTestRaftStore()
	defer stopTestStore(store)
	latches := latches2.NewLatches()

	k1 := []byte("tk1")
	k2 := []byte("tk2")
	v1 := []byte("v1")
	v2 := []byte("v2")
	v3 := []byte("v3")

	// Same requests multiple times to simulate rpc retry.
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustPrewritePut(t, store, latches, k1, v1, k1, genPhyTS(1), 10)
	mustCheckTxnStatusLocked(t, store, latches, k1, genPhyTS(1), genPhyTS(2), 10)
	mustCheckTxnStatusLocked(t, store, latches, k1, genPhyTS(1), genPhyTS(2), 10)
	mustCommit(t, store, latches, k1, genPhyTS(1), genPhyTS(5))
	mustCommit(t, store, latches, k1, genPhyTS(1), genPhyTS(5))
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(1), genPhyTS(2), genPhyTS(5))
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(1), genPhyTS(2), genPhyTS(5))
	mustGetKV(t, store, latches, k1, genPhyTS(6), v1)
	mustGetKV(t, store, latches, k1, genPhyTS(6), v1)
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(1), genPhyTS(2), genPhyTS(5))
	mustCommit(t, store, latches, k1, genPhyTS(1), genPhyTS(5))
	mustPrewritePutErr(t, store, latches, k1, v1, k1, genPhyTS(1), 10)

	mustPrewriteDel(t, store, latches, k1, k1, genPhyTS(11), 10)
	mustPrewriteDel(t, store, latches, k1, k1, genPhyTS(11), 10)
	mustCommit(t, store, latches, k1, genPhyTS(11), genPhyTS(15))
	mustCommit(t, store, latches, k1, genPhyTS(11), genPhyTS(15))
	mustGetKV(t, store, latches, k1, genPhyTS(16), nil)
	mustGetKV(t, store, latches, k1, genPhyTS(16), nil)

	mustPrewritePut(t, store, latches, k1, v2, k1, genPhyTS(21), 10)
	mustPrewritePut(t, store, latches, k1, v2, k1, genPhyTS(21), 10)
	mustRollback(t, store, latches, k1, genPhyTS(21))
	mustRollback(t, store, latches, k1, genPhyTS(21))
	mustCheckTxnStatusAlreadyRollback(t, store, latches, k1, genPhyTS(21), genPhyTS(22))
	mustCheckTxnStatusAlreadyRollback(t, store, latches, k1, genPhyTS(21), genPhyTS(22))
	mustGetKV(t, store, latches, k1, genPhyTS(23), nil)

	mustPrewritePut(t, store, latches, k1, v3, k1, genPhyTS(31), 10)
	mustPrewritePut(t, store, latches, k2, v3, k1, genPhyTS(31), 10)
	mustCheckTxnStatusTTLExpireRollback(t, store, latches, k1, genPhyTS(31), genPhyTS(42))
	mustCheckTxnStatusAlreadyRollback(t, store, latches, k1, genPhyTS(31), genPhyTS(43))
	mustResolveLock(t, store, latches, genPhyTS(31), genPhyTS(0))
	mustResolveLock(t, store, latches, genPhyTS(31), genPhyTS(0))
	mustCommitErr(t, store, latches, k1, genPhyTS(31), genPhyTS(35))
	mustCommitErr(t, store, latches, k1, genPhyTS(31), genPhyTS(35))
	mustCommitErr(t, store, latches, k2, genPhyTS(31), genPhyTS(38))
	mustCommitErr(t, store, latches, k2, genPhyTS(31), genPhyTS(38))
	mustGetKV(t, store, latches, k1, genPhyTS(40), nil)
	mustGetKV(t, store, latches, k2, genPhyTS(40), nil)

	mustPrewritePut(t, store, latches, k1, v3, k1, genPhyTS(41), 10)
	mustPrewritePut(t, store, latches, k2, v3, k1, genPhyTS(41), 10)
	mustCommit(t, store, latches, k1, genPhyTS(41), genPhyTS(45))
	mustCheckTxnStatusCommitted(t, store, latches, k1, genPhyTS(41), genPhyTS(50), genPhyTS(45))
	mustResolveLock(t, store, latches, genPhyTS(41), genPhyTS(45))
	mustCheckTxnStatusCommitted(t, store, latches, k2, genPhyTS(41), genPhyTS(50), genPhyTS(45))
	mustGetKV(t, store, latches, k1, genPhyTS(50), v3)
	mustGetKV(t, store, latches, k2, genPhyTS(50), v3)
}
