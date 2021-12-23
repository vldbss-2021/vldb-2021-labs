package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	applyCh chan []message.Msg
	ctx     *GlobalContext
}

func newPeerMsgHandler(peer *peer, applyCh chan []message.Msg, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer:    peer,
		applyCh: applyCh,
		ctx:     ctx,
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Error(fmt.Sprintf("%s handle raft message error %v", d.Tag, err))
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeApplyRes:
		res := msg.Data.(*MsgApplyRes)
		d.onApplyResult(res)
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Info(fmt.Sprintf("%s on split with %v", d.Tag, split.SplitKey))
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Info(fmt.Sprintf("%s snap file %s has been compacted, delete", d.Tag, key))
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Info(fmt.Sprintf("%s snap file %s has been expired, delete", d.Tag, key))
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Info(fmt.Sprintf("%s snap file %s has been applied, delete", d.Tag, key))
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	msgs := make([]message.Msg, 0)
	if p := d.TakeApplyProposals(); p != nil {
		msg := message.Msg{Type: message.MsgTypeApplyProposal, Data: p, RegionID: p.RegionId}
		msgs = append(msgs, msg)
	}
	applySnapResult, msgs := d.peer.HandleRaftReady(msgs, d.ctx.schedulerTaskSender, d.ctx.trans)
	if applySnapResult != nil {
		prevRegion := applySnapResult.PrevRegion
		region := applySnapResult.Region

		log.Info(fmt.Sprintf("%s snapshot for region %s is applied", d.Tag, region))
		meta := d.ctx.storeMeta
		meta.Lock()
		defer meta.Unlock()
		initialized := len(prevRegion.Peers) > 0
		if initialized {
			log.Info(fmt.Sprintf("%s region changed from %s -> %s after applying snapshot", d.Tag, prevRegion, region))
			meta.regionRanges.Delete(&regionItem{region: prevRegion})
		}
		if oldRegion := meta.regionRanges.ReplaceOrInsert(&regionItem{region: region}); oldRegion != nil {
			panic(fmt.Sprintf("%s unexpected old region %+v, region %+v", d.Tag, oldRegion, region))
		}
		meta.regions[region.Id] = region
	}
	d.applyCh <- msgs
}

func (d *peerMsgHandler) onRaftBaseTick() {
	// When having pending snapshot, if election timeout is met, it can't pass
	// the pending conf change check because first index has been updated to
	// a value that is larger than last index.
	if d.HasPendingSnapshot() {
		// need to check if snapshot is applied.
		d.ticker.schedule(PeerTickRaft)
		return
	}
	// TODO: make Tick returns bool to indicate if there is ready.
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) onApplyResult(res *MsgApplyRes) {

	log.Debug(fmt.Sprintf("%s async apply finished %v", d.Tag, res))
	// handle executing committed log results
	for _, result := range res.execResults {
		switch x := result.(type) {
		case *execResultChangePeer:
			d.onReadyChangePeer(x)
		case *execResultCompactLog:
			d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
		case *execResultSplitRegion:
			d.onReadySplitRegion(x.derived, x.regions)
		}
	}
	res.execResults = nil
	if d.stopped {
		return
	}

	diff := d.SizeDiffHint + res.sizeDiffHint
	if diff > 0 {
		d.SizeDiffHint = diff
	} else {
		d.SizeDiffHint = 0
	}
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debug(fmt.Sprintf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId()))
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debug(fmt.Sprintf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId()))
	if to.GetStoreId() != d.storeID() {
		log.Warn(fmt.Sprintf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID()))
		return false
	}
	if msg.RegionEpoch == nil {
		log.Error(fmt.Sprintf("[region %d] missing epoch in raft message, ignore it", regionID))
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Info(fmt.Sprintf("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId()))
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Info(fmt.Sprintf("%s is stale as received a larger peer %s, destroying", d.Tag, target))
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Info(fmt.Sprintf("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch))
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Error(fmt.Sprintf("[region %d] send message failed %v", regionID, err))
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Info(fmt.Sprintf("%s receive stale gc msg, ignore", d.Tag))
		return
	}
	log.Info(fmt.Sprintf("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer))
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Info(fmt.Sprintf("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID))
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Info(fmt.Sprintf("%s stale delegate detected, skip", d.Tag))
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Info(fmt.Sprintf("%s region overlapped %s %s", d.Tag, existRegion, snapRegion))
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Info(fmt.Sprintf("%s starts destroy", d.Tag))
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) onReadyChangePeer(cp *execResultChangePeer) {
	changeType := cp.confChange.ChangeType
	d.RaftGroup.ApplyConfChange(*cp.confChange)
	if cp.confChange.NodeId == 0 {
		// Apply failed, skip.
		return
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	meta.setRegion(cp.region, d.peer)
	meta.Unlock()
	peerID := cp.peer.Id
	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		// Add this peer to cache and heartbeats.
		now := time.Now()
		if d.IsLeader() {
			d.PeersStartPendingTime[peerID] = now
		}
		d.insertPeerCache(cp.peer)
	case eraftpb.ConfChangeType_RemoveNode:
		// Remove this peer from cache.
		if d.IsLeader() {
			delete(d.PeersStartPendingTime, peerID)
		}
		d.removePeerCache(peerID)
	}

	// In pattern matching above, if the peer is the leader,
	// it will push the change peer into `peers_start_pending_time`
	// without checking if it is duplicated. We move `heartbeat_pd` here
	// to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
	// adding the redundant peer.
	if d.IsLeader() {
		// Notify scheduler immediately.
		log.Info(fmt.Sprintf("%s notify scheduler with change peer region %s", d.Tag, d.Region()))
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	myPeerID := d.PeerId()

	// We only care remove itself now.
	if changeType == eraftpb.ConfChangeType_RemoveNode && cp.peer.StoreId == d.storeID() {
		if myPeerID == peerID {
			d.destroyPeer()
		} else {
			panic(fmt.Sprintf("%s trying to remove unknown peer %s", d.Tag, cp.peer))
		}
	}
}

func (d *peerMsgHandler) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	regionID := derived.Id
	meta.setRegion(derived, d.peer)
	d.SizeDiffHint = 0
	isLeader := d.IsLeader()
	if isLeader {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		// Notify scheduler immediately to let it update the region meta.
		log.Info(fmt.Sprintf("%s notify scheduler with split count %d", d.Tag, len(regions)))
	}

	if meta.regionRanges.Delete(&regionItem{region: regions[0]}) == nil {
		panic(d.Tag + " original region should exist")
	}
	// It's not correct anymore, so set it to None to let split checker update it.
	d.ApproximateSize = nil

	for _, newRegion := range regions {
		newRegionID := newRegion.Id
		notExist := meta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		if notExist != nil {
			panic(fmt.Sprintf("%v %v newregion:%v, region:%v", d.Tag, notExist.(*regionItem).region, newRegion, regions[0]))
		}
		if newRegionID == regionID {
			continue
		}

		// Insert new regions and validation
		log.Info(fmt.Sprintf("[region %d] inserts new region %s", regionID, newRegion))
		if r, ok := meta.regions[newRegionID]; ok {
			// Suppose a new node is added by conf change and the snapshot comes slowly.
			// Then, the region splits and the first vote message comes to the new node
			// before the old snapshot, which will create an uninitialized peer on the
			// store. After that, the old snapshot comes, followed with the last split
			// proposal. After it's applied, the uninitialized peer will be met.
			// We can remove this uninitialized peer directly.
			if len(r.Peers) > 0 {
				panic(fmt.Sprintf("[region %d] duplicated region %s for split region %s",
					newRegionID, r, newRegion))
			}
			d.ctx.router.close(newRegionID)
		}

		newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
		}
		metaPeer := newPeer.Meta

		for _, p := range newRegion.GetPeers() {
			newPeer.insertPeerCache(p)
		}

		// New peer derive write flow from parent region,
		// this will be used by balance write flow.
		campaigned := newPeer.MaybeCampaign(isLeader)

		if isLeader {
			// The new peer is likely to become leader, send a heartbeat immediately to reduce
			// client query miss.
			newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}

		meta.regions[newRegionID] = newRegion
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeStart, newRegionID, nil))
		if !campaigned {
			for i, msg := range meta.pendingVotes {
				if util.PeerEqual(msg.ToPeer, metaPeer) {
					meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
					_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeRaftMessage, newRegionID, msg))
					break
				}
			}
		}
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	panic("not implemented yet")
	// YOUR CODE HERE (lab1).
	// Hint1: do `preProposeRaftCommand` check for the command, if the check fails, need to execute the
	// callback function and return the error results. `ErrResp` is useful to generate error response.

	// Hint2: Check if peer is stopped already, if so notify the callback that the region is removed, check
	// the `destroy` function for related utilities. `NotifyReqRegionRemoved` is useful to generate error response.

	// Hint3: Bind the possible response with term then do the real requests propose using the `Propose` function.
	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed. There are some useful information in the `ctx` of the `peerMsgHandler`.
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatal(fmt.Sprintf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx))
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error("split key empty error", zap.Error(err))
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Info(fmt.Sprintf("%s not leader, skip", d.Tag))
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Info(fmt.Sprintf("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch))
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
