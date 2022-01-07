package test_raftstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const TIMEOUT = 10 * time.Second

type Simulator interface {
	RunStore(raftConf *config.Config, engine *engine_util.Engines, ctx context.Context) error
	StopStore(storeID uint64)
	AddFilter(filter Filter)
	ClearFilters()
	GetStoreIds() []uint64
	CallCommandOnStore(storeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, error)
}

type Cluster struct {
	schedulerClient *MockSchedulerClient
	count           int
	engines         map[uint64]*engine_util.Engines
	snapPaths       map[uint64]string
	dirs            []string
	simulator       Simulator
	cfg             *config.Config
}

func NewCluster(count int, schedulerClient *MockSchedulerClient, simulator Simulator, cfg *config.Config) *Cluster {
	return &Cluster{
		count:           count,
		schedulerClient: schedulerClient,
		engines:         make(map[uint64]*engine_util.Engines),
		snapPaths:       make(map[uint64]string),
		simulator:       simulator,
		cfg:             cfg,
	}
}

func (c *Cluster) Start() {
	ctx := context.TODO()
	clusterID := c.schedulerClient.GetClusterID(ctx)

	for storeID := uint64(1); storeID <= uint64(c.count); storeID++ {
		dbPath, err := ioutil.TempDir("", "test-raftstore")
		if err != nil {
			log.Fatal(fmt.Sprintf("creating storage path unexpected error=%v", err))
		}
		c.cfg.DBPath = dbPath
		kvPath := filepath.Join(dbPath, "kv")
		raftPath := filepath.Join(dbPath, "raft")
		snapPath := filepath.Join(dbPath, "snap")
		c.snapPaths[storeID] = snapPath
		c.dirs = append(c.dirs, []string{kvPath, raftPath, snapPath}...)

		err = os.MkdirAll(kvPath, os.ModePerm)
		if err != nil {
			log.Fatal(fmt.Sprintf("creating storage path unexpected error=%v", err))
		}
		err = os.MkdirAll(raftPath, os.ModePerm)
		if err != nil {
			log.Fatal(fmt.Sprintf("creating storage path unexpected error=%v", err))
		}
		err = os.MkdirAll(snapPath, os.ModePerm)
		if err != nil {
			log.Fatal(fmt.Sprintf("creating storage path unexpected error=%v", err))
		}

		raftDB := engine_util.CreateDB("raft", c.cfg)
		kvDB := engine_util.CreateDB("kv", c.cfg)
		engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
		c.engines[storeID] = engine
	}

	regionEpoch := &metapb.RegionEpoch{
		Version: raftstore.InitEpochVer,
		ConfVer: raftstore.InitEpochConfVer,
	}
	firstRegion := &metapb.Region{
		Id:          1,
		StartKey:    []byte{},
		EndKey:      []byte{},
		RegionEpoch: regionEpoch,
	}

	for storeID, engine := range c.engines {
		peer := NewPeer(storeID, storeID)
		firstRegion.Peers = append(firstRegion.Peers, peer)
		err := raftstore.BootstrapStore(engine, clusterID, storeID)
		if err != nil {
			log.Fatal(fmt.Sprintf("bootstrap store=%v unexpected error=%v", storeID, err))
		}
	}

	for _, engine := range c.engines {
		err := raftstore.PrepareBootstrapCluster(engine, firstRegion)
		if err != nil {
			log.Fatal(fmt.Sprintf("prepare bootstrap cluster unexpected error=%v", err))
		}
	}

	store := &metapb.Store{
		Id:      1,
		Address: "",
	}
	resp, err := c.schedulerClient.Bootstrap(context.TODO(), store)
	if err != nil {
		log.Fatal(fmt.Sprintf("bootstrap cluster using scheduler client unexpected error=%v", err))
	}
	if resp.Header != nil && resp.Header.Error != nil {
		log.Fatal(fmt.Sprintf("bootstrap cluster using scheduler client unexpected error=%v", resp.Header.Error.String()))
	}

	for storeID, engine := range c.engines {
		store := &metapb.Store{
			Id:      storeID,
			Address: "",
		}
		err := c.schedulerClient.PutStore(context.TODO(), store)
		if err != nil {
			log.Fatal(fmt.Sprintf("put store=%v using scheduler client unexpected error=%v", storeID, err))
		}
		err = raftstore.ClearPrepareBootstrapState(engine)
		if err != nil {
			log.Fatal(fmt.Sprintf("clear bootstrap cluster unexpected error=%v", err))
		}
	}

	for storeID := range c.engines {
		c.StartServer(storeID)
	}
}

func (c *Cluster) Shutdown() {
	for _, storeID := range c.simulator.GetStoreIds() {
		c.simulator.StopStore(storeID)
	}
	for _, engine := range c.engines {
		err := engine.Close()
		if err != nil {
			log.Fatal("shutdown engine error", zap.Error(err))
		}
	}
	for _, dir := range c.dirs {
		os.RemoveAll(dir)
	}
}

func (c *Cluster) AddFilter(filter Filter) {
	c.simulator.AddFilter(filter)
}

func (c *Cluster) ClearFilters() {
	c.simulator.ClearFilters()
}

func (c *Cluster) StopServer(storeID uint64) {
	c.simulator.StopStore(storeID)
}

func (c *Cluster) StartServer(storeID uint64) {
	engine := c.engines[storeID]
	err := c.simulator.RunStore(c.cfg, engine, context.TODO())
	if err != nil {
		log.Fatal(fmt.Sprintf("start server unexpected error=%v", err))
	}
}

func (c *Cluster) AllocPeer(storeID uint64) *metapb.Peer {
	id, err := c.schedulerClient.AllocID(context.TODO())
	if err != nil {
		log.Fatal(fmt.Sprintf("alloc peer unexpected error=%v", err))
	}
	return NewPeer(storeID, id)
}

func (c *Cluster) Request(key []byte, reqs []*raft_cmdpb.Request, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, error) {
	startTime := time.Now()
	for i := 0; i < 10 || time.Now().Sub(startTime) < timeout; i++ {
		region := c.GetRegion(key)
		regionID := region.GetId()
		req := NewRequest(regionID, region.RegionEpoch, reqs)
		resp, txn, err := c.CallCommandOnLeader(&req, timeout)
		if err != nil {
			SleepMS(500)
			continue
		}
		if resp == nil {
			// it should be timeouted innerly
			SleepMS(500)
			continue
		}
		if resp.Header.Error != nil {
			SleepMS(500)
			continue
		}
		return resp, txn, nil
	}
	return nil, nil, fmt.Errorf("request timed out duration=%v", timeout)
}

func (c *Cluster) CallCommand(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, error) {
	storeID := request.Header.Peer.StoreId
	return c.simulator.CallCommandOnStore(storeID, request, timeout)
}

func (c *Cluster) CallCommandOnLeader(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, error) {
	startTime := time.Now()
	regionID := request.Header.RegionId
	leader := c.LeaderOfRegion(regionID)
	for {
		if time.Now().Sub(startTime) > timeout {
			return nil, nil, fmt.Errorf("request has timed out duration=%v", timeout)
		}
		if leader == nil {
			log.Fatal(fmt.Sprintf("can't get leader of region %d", regionID))
		}
		request.Header.Peer = leader
		resp, txn, err := c.CallCommand(request, 1*time.Second)
		if err != nil {
			return nil, nil, err
		}
		if resp == nil {
			log.Debug(fmt.Sprintf("can't call command %s on leader %d of region %d", request.String(), leader.GetId(), regionID))
			newLeader := c.LeaderOfRegion(regionID)
			if leader == newLeader {
				region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
				if err != nil {
					return nil, nil, err
				}
				peers := region.GetPeers()
				leader = peers[rand.Int()%len(peers)]
				log.Debug(fmt.Sprintf("leader info maybe wrong, use random leader %d of region %d", leader.GetId(), regionID))
			} else {
				leader = newLeader
				log.Debug(fmt.Sprintf("use new leader %d of region %d", leader.GetId(), regionID))
			}
			continue
		}
		if resp.Header.Error != nil {
			err := resp.Header.Error
			if err.GetStaleCommand() != nil || err.GetEpochNotMatch() != nil || err.GetNotLeader() != nil {
				log.Debug(fmt.Sprintf("encouter retryable err %+v", resp))
				if err.GetNotLeader() != nil && err.GetNotLeader().Leader != nil {
					leader = err.GetNotLeader().Leader
				} else {
					leader = c.LeaderOfRegion(regionID)
				}
				continue
			}
		}
		return resp, txn, nil
	}
}

func (c *Cluster) LeaderOfRegion(regionID uint64) *metapb.Peer {
	for i := 0; i < 500; i++ {
		_, leader, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err == nil && leader != nil {
			return leader
		}
		SleepMS(10)
	}
	return nil
}

func (c *Cluster) GetRegion(key []byte) *metapb.Region {
	for i := 0; i < 100; i++ {
		region, _, _ := c.schedulerClient.GetRegion(context.TODO(), key)
		if region != nil {
			return region
		}
		// We may meet range gap after split, so here we will
		// retry to get the region again.
		SleepMS(20)
	}
	log.Fatal(fmt.Sprintf("find no region for %s", hex.EncodeToString(key)))
	return nil
}

func (c *Cluster) GetRandomRegion() *metapb.Region {
	return c.schedulerClient.getRandomRegion()
}

func (c *Cluster) GetStoreIdsOfRegion(regionID uint64) []uint64 {
	region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
	if err != nil {
		log.Fatal(fmt.Sprintf(" get region by id=%v unexpected error=%v", region, err))
	}
	peers := region.GetPeers()
	storeIds := make([]uint64, len(peers))
	for i, peer := range peers {
		storeIds[i] = peer.GetStoreId()
	}
	return storeIds
}

func (c *Cluster) MustPut(key, value []byte) {
	c.MustPutCF(engine_util.CfDefault, key, value)
}

func (c *Cluster) MustPutCF(cf string, key, value []byte) error {
	req := NewPutCfCmd(cf, key, value)
	resp, _, err := c.Request(key, []*raft_cmdpb.Request{req}, TIMEOUT)
	if err != nil {
		log.Fatal(fmt.Sprintf("request failed err=%v", err))
		return err
	}
	if resp.Header.Error != nil {
		log.Fatal(fmt.Sprintf("error exists in response err=%v", resp.Header.Error.String()))
		return fmt.Errorf("err=%v", resp.Header.Error.String())
	}
	if len(resp.Responses) != 1 {
		log.Fatal(fmt.Sprintf("unexpected len(resp.Responses)=%v, should be 1", len(resp.Responses)))
		return fmt.Errorf("unexpected len(resp.Responses)=%v, should be 1", len(resp.Responses))
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Put {
		log.Fatal(fmt.Sprintf("unexpected response=%v, should be CmdType_Put", resp.Responses[0].String()))
		return fmt.Errorf("unexpected response=%v, should be CmdType_Put", resp.Responses[0].String())
	}
	return nil
}

func (c *Cluster) MustGet(key []byte, value []byte) {
	v, err := c.Get(key)
	if err != nil {
		log.Fatal(fmt.Sprintf("get error=%v", err))
	}
	if !bytes.Equal(v, value) {
		log.Fatal(fmt.Sprintf("expected value %s, but got %s", value, v))
	}
}

func (c *Cluster) Get(key []byte) ([]byte, error) {
	return c.GetCF(engine_util.CfDefault, key)
}

func (c *Cluster) GetCF(cf string, key []byte) ([]byte, error) {
	req := NewGetCfCmd(cf, key)
	resp, _, err := c.Request(key, []*raft_cmdpb.Request{req}, TIMEOUT)
	if resp.Header.Error != nil {
		return nil, err
	}
	if len(resp.Responses) != 1 {
		return nil, fmt.Errorf("unexpected len(resp.Responses)=%v, should be 1", len(resp.Responses))
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Get {
		return nil, fmt.Errorf("unexpected response=%v, should be CmdType_Get", resp.Responses[0].String())
	}
	return resp.Responses[0].Get.Value, nil
}

func (c *Cluster) MustDelete(key []byte) {
	c.MustDeleteCF(engine_util.CfDefault, key)
}

func (c *Cluster) MustDeleteCF(cf string, key []byte) {
	req := NewDeleteCfCmd(cf, key)
	resp, _, err := c.Request(key, []*raft_cmdpb.Request{req}, TIMEOUT)
	if err != nil {
		log.Fatal(fmt.Sprintf("request failed err=%v", err))
	}
	if resp.Header.Error != nil {
		log.Fatal(fmt.Sprintf("MustDeleteCF error in response error=%v", err))
	}
	if len(resp.Responses) != 1 {
		log.Fatal(fmt.Sprintf("unexpected len(resp.Responses)=%v, should be 1", len(resp.Responses)))
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Delete {
		log.Fatal(fmt.Sprintf("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Delete"))
	}
}

func (c *Cluster) Scan(start, end []byte) ([][]byte, error) {
	req := NewSnapCmd()
	values := make([][]byte, 0)
	key := start
	for (len(end) != 0 && bytes.Compare(key, end) < 0) || (len(key) == 0 && len(end) == 0) {
		resp, txn, err := c.Request(key, []*raft_cmdpb.Request{req}, TIMEOUT)
		if err != nil {
			return nil, err
		}
		if resp.Header.Error != nil {
			return nil, fmt.Errorf("%v", resp.Header.Error.String())
		}
		if len(resp.Responses) != 1 {
			return nil, fmt.Errorf("len(resp.Responses) != 1")
		}
		if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Snap {
			return nil, fmt.Errorf("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Snap")
		}
		region := resp.Responses[0].GetSnap().Region
		iter := raft_storage.NewRegionReader(txn, *region).IterCF(engine_util.CfDefault)
		for iter.Seek(key); iter.Valid(); iter.Next() {
			if engine_util.ExceedEndKey(iter.Item().Key(), end) {
				break
			}
			value, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		iter.Close()

		key = region.EndKey
		if len(key) == 0 {
			break
		}
	}

	return values, nil
}

func (c *Cluster) TransferLeader(regionID uint64, leader *metapb.Peer) {
	region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
	if err != nil {
		log.Fatal(fmt.Sprintf("trasfer leader get region by id=%v has failed err=%v", regionID, err))
	}
	epoch := region.RegionEpoch
	transferLeader := NewAdminRequest(regionID, epoch, NewTransferLeaderCmd(leader))
	resp, _, err := c.CallCommandOnLeader(transferLeader, TIMEOUT)
	if err != nil {
		log.Fatal(fmt.Sprintf("trasfer leader call has failed err=%v", err))
	}
	if resp.AdminResponse.CmdType != raft_cmdpb.AdminCmdType_TransferLeader {
		log.Fatal("resp.AdminResponse.CmdType != raft_cmdpb.AdminCmdType_TransferLeader")
	}
}

func (c *Cluster) MustTransferLeader(regionID uint64, leader *metapb.Peer) {
	timer := time.Now()
	for {
		currentLeader := c.LeaderOfRegion(regionID)
		if currentLeader.Id == leader.Id &&
			currentLeader.StoreId == leader.StoreId {
			return
		}
		if time.Since(timer) > TIMEOUT {
			log.Fatal(fmt.Sprintf("failed to transfer leader to [%d] %s", regionID, leader.String()))
		}
		c.TransferLeader(regionID, leader)
	}
}

func (c *Cluster) MustAddPeer(regionID uint64, peer *metapb.Peer) {
	c.schedulerClient.AddPeer(regionID, peer)
	c.MustHavePeer(regionID, peer)
}

func (c *Cluster) MustRemovePeer(regionID uint64, peer *metapb.Peer) {
	c.schedulerClient.RemovePeer(regionID, peer)
	c.MustNonePeer(regionID, peer)
}

func (c *Cluster) MustHavePeer(regionID uint64, peer *metapb.Peer) {
	for i := 0; i < 200; i++ {
		region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err != nil {
			log.Fatal(fmt.Sprintf("MustHavePeer err=%v", err))
		}
		if region != nil {
			if p := FindPeer(region, peer.GetStoreId()); p != nil {
				if p.GetId() == peer.GetId() {
					return
				}
			}
		}
		SleepMS(100)
	}
	log.Fatal(fmt.Sprintf("no peer: %v", peer))
}

func (c *Cluster) MustNonePeer(regionID uint64, peer *metapb.Peer) {
	for i := 0; i < 200; i++ {
		region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err != nil {
			log.Fatal(fmt.Sprintf("MustNonePeer get region by id=%v err=%v", regionID, err))
		}
		if region != nil {
			if p := FindPeer(region, peer.GetStoreId()); p != nil {
				if p.GetId() != peer.GetId() {
					return
				}
			} else {
				return
			}
		}
		SleepMS(100)
	}
	log.Fatal(fmt.Sprintf("have peer: %v", peer))
}
