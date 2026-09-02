package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
	"github.com/Sephy314/Cachey/pkg/client"
)

// clusterNode bundles one node's store (FSM), raft node, transport, WAL, and
// the ClusterStore adapter exposed to clients.
type clusterNode struct {
	id    string
	dir   string
	store *store.CacheyStore
	node  *raft.Node
	tr    *raft.TCPTransport
	wal   *wal.WAL
	cs    *ClusterStore
}

type persistentCluster struct {
	mu                sync.Mutex
	nodes             map[string]*clusterNode
	addrs             map[string]string
	ids               []string
	snapshotThreshold uint64
}

// newPersistentCluster starts a raft cluster over TCP with WAL persistence.
// dirs maps node id to its WAL directory (reuse a dir to test restart).
func newPersistentCluster(t *testing.T, ids []string, dirs map[string]string) *persistentCluster {
	return newPersistentClusterThreshold(t, ids, dirs, 0)
}

// newPersistentClusterThreshold is newPersistentCluster with a raft log
// compaction threshold (entries after the snapshot base that trigger a
// snapshot).
func newPersistentClusterThreshold(t *testing.T, ids []string, dirs map[string]string, threshold uint64) *persistentCluster {
	t.Helper()
	pc := &persistentCluster{ids: ids, snapshotThreshold: threshold}
	trs := make(map[string]*raft.TCPTransport)
	pc.addrs = make(map[string]string)
	pc.nodes = make(map[string]*clusterNode)
	for _, id := range ids {
		trs[id] = raft.NewTCPTransport(nil)
	}
	for _, id := range ids {
		addr, err := trs[id].Listen("127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		pc.addrs[id] = addr
	}
	for _, id := range ids {
		pc.nodes[id] = newPersistentNode(t, id, dirs[id], trs[id], pc.addrs, otherIDs(id, pc.addrs), threshold)
	}
	for _, id := range ids {
		pc.nodes[id].node.Run()
	}
	return pc
}

func newPersistentNode(t *testing.T, id, dir string, tr *raft.TCPTransport, addrs map[string]string, peers []string, snapshotThreshold uint64) *clusterNode {
	t.Helper()
	st := store.NewCacheyStore()
	cfg := raft.Config{
		ID:                id,
		Peers:             peers,
		HeartbeatInterval: 100 * time.Millisecond,
		// Election timeout randomized to 600-1200ms: ~6-12x the heartbeat, so
		// a follower does not time out on a single delayed heartbeat under CI
		// CPU contention. The old 300ms with an 80ms heartbeat (~4x) made an
		// even-size remainder (kill/remove leaves a unanimity quorum) churn:
		// one delayed heartbeat deposed the leader and the survivors ping-ponged
		// terms long enough to stall writes/reads.
		ElectionTimeout:   600 * time.Millisecond,
		SnapshotThreshold: snapshotThreshold,
	}
	n, err := raft.NewNode(cfg, tr, NewRaftApply(st))
	if err != nil {
		t.Fatalf("NewNode(%s): %v", id, err)
	}
	n.SetSnapshotCallbacks(
		func() ([]byte, error) {
			entries, err := st.Snapshot()
			if err != nil {
				return nil, err
			}
			return json.Marshal(entries)
		},
		func(data []byte) error {
			var entries []wal.SnapshotEntry
			if err := json.Unmarshal(data, &entries); err != nil {
				return err
			}
			return st.ApplySnapshot(entries)
		},
	)
	n.SetSnapshotStore(raft.NewFileSnapshotStore(dir))
	// Recovery order: restore a persisted snapshot (FSM + log base) before
	// replaying the WAL so records at or before the snapshot are skipped.
	if snap, ok, err := raft.NewFileSnapshotStore(dir).Load(); err == nil && ok {
		if err := n.RestoreSnapshot(snap); err != nil {
			t.Fatalf("restore snapshot (%s): %v", id, err)
		}
	}
	wcfg := wal.DefaultConfig(dir)
	wcfg.DisableRotation = true
	w, err := wal.Open(wcfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   n.ApplyRecoveredRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		t.Fatalf("wal.Open(%s): %v", dir, err)
	}
	n.SetLogStore(raft.NewWALLogStore(w))
	tr.SetNode(n)
	tr.SetPeers(peerAddrsOf(id, addrs))
	cs := NewClusterStore(n, st)
	cs.SetLeaderResolver(func(leaderID string) string { return addrs[leaderID] })
	return &clusterNode{id: id, dir: dir, store: st, node: n, tr: tr, wal: w, cs: cs}
}

func otherIDs(id string, addrs map[string]string) []string {
	var out []string
	for k := range addrs {
		if k != id {
			out = append(out, k)
		}
	}
	return out
}

func peerAddrsOf(id string, addrs map[string]string) map[string]string {
	m := make(map[string]string)
	for k, v := range addrs {
		if k != id {
			m[k] = v
		}
	}
	return m
}

func (pc *persistentCluster) stopAll() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, cn := range pc.nodes {
		cn.node.Stop()
		cn.tr.Close()
		cn.wal.Close()
	}
}

// restart replaces a node with a fresh instance recovered from the same WAL
// dir (new transport, new listener), updating peers' address maps.
func (pc *persistentCluster) restart(t *testing.T, id string) {
	t.Helper()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	old := pc.nodes[id]
	old.node.Stop()
	old.tr.Close()
	old.wal.Close()

	tr := raft.NewTCPTransport(nil)
	addr, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("re-listen %s: %v", id, err)
	}
	pc.addrs[id] = addr
	for peer, cn := range pc.nodes {
		if peer != id {
			cn.tr.SetPeers(peerAddrsOf(peer, pc.addrs))
		}
	}
	cn := newPersistentNode(t, id, old.dir, tr, pc.addrs, otherIDs(id, pc.addrs), pc.snapshotThreshold)
	cn.node.Run()
	pc.nodes[id] = cn
}

// leaderID returns the current single leader id, or "" if none is elected or
// the view is ambiguous. Unlike waitLeader it never fails the test.
func (pc *persistentCluster) leaderID() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.leaderIDLocked("")
}

// leaderIDExcept returns the single leader among the nodes other than the
// excluded id (used while a stale leader is partitioned but still running), or
// "" if none / ambiguous.
func (pc *persistentCluster) leaderIDExcept(excluded string) string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.leaderIDLocked(excluded)
}

func (pc *persistentCluster) leaderIDLocked(excluded string) string {
	var leader string
	leaders := 0
	for id, cn := range pc.nodes {
		if id == excluded {
			continue
		}
		if cn.node.IsLeader() {
			leader = id
			leaders++
		}
	}
	if leaders != 1 {
		return ""
	}
	return leader
}

// waitLeader returns the cluster's single leader id.
func (pc *persistentCluster) waitLeader(t *testing.T) string {
	t.Helper()
	var leader string
	waitFor(t, "a leader to be elected", 30*time.Second, func() bool {
		leaders := 0
		for id, cn := range pc.nodes {
			if cn.node.IsLeader() {
				leader = id
				leaders++
			}
		}
		return leaders == 1
	})
	return leader
}

// write puts a key on the leader and waits for commit+apply. It re-finds the
// leader and retries on a transient "not leader" (the node we picked can be
// deposed between waitLeader and the write) or a commit that stalls briefly
// while an even-size remainder settles an election — exactly how a real client
// behaves via leader redirect.
func (pc *persistentCluster) write(t *testing.T, key, val string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		leader := pc.waitLeader(t)
		err := pc.nodes[leader].cs.Put(key, val)
		if err == nil {
			return
		}
		if !(errors.Is(err, raft.ErrNotLeader) || errors.Is(err, context.DeadlineExceeded)) || time.Now().After(deadline) {
			t.Fatalf("Put(%s=%s): %v", key, val, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// hasKey reports whether every live node's FSM has key=val.
func (pc *persistentCluster) hasKey(key, val string) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, cn := range pc.nodes {
		v, err := cn.store.Get(key)
		if err != nil || *v != val {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPersistentClusterReplicationAndFailover(t *testing.T) {
	// 5-node base: killing one leaves 4 survivors needing only a 3-vote
	// majority, so failover does not depend on a fragile unanimity quorum.
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir(), "d": t.TempDir(), "e": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c", "d", "e"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	pc.write(t, "k2", "v2")
	waitFor(t, "all nodes replicate both writes", 15*time.Second, func() bool {
		return pc.hasKey("k1", "v1") && pc.hasKey("k2", "v2")
	})

	// Kill the leader; the remaining four elect a new one (majority of 5 = 3).
	old := pc.waitLeader(t)
	pc.mu.Lock()
	pc.nodes[old].node.Stop()
	pc.nodes[old].tr.Close()
	pc.nodes[old].wal.Close()
	delete(pc.nodes, old)
	pc.mu.Unlock()

	newLeader := pc.waitLeader(t)
	if newLeader == old {
		t.Fatal("leader did not change after failover")
	}
	pc.write(t, "k3", "v3")
	waitFor(t, "live nodes replicate the post-failover write", 15*time.Second, func() bool {
		return pc.hasKey("k1", "v1") && pc.hasKey("k2", "v2") && pc.hasKey("k3", "v3")
	})
}

func TestPersistentClusterRestart(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir(), "d": t.TempDir(), "e": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c", "d", "e"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	pc.write(t, "k2", "v2")
	waitFor(t, "all nodes replicate before restart", 15*time.Second, func() bool {
		return pc.hasKey("k1", "v1") && pc.hasKey("k2", "v2")
	})

	// Crash a follower and restart it from the same WAL dir.
	var victim string
	for id, cn := range pc.nodes {
		if !cn.node.IsLeader() {
			victim = id
			break
		}
	}
	if victim == "" {
		t.Fatal("no follower to restart")
	}
	pc.mu.Lock()
	pc.nodes[victim].node.Stop()
	pc.nodes[victim].wal.Close()
	pc.mu.Unlock()
	pc.restart(t, victim)

	// It must recover its log from the WAL and catch up to committed state.
	waitFor(t, "restarted node catches up to committed state", 15*time.Second, func() bool {
		v, err := pc.nodes[victim].store.Get("k1")
		return err == nil && *v == "v1"
	})
}

func TestClusterLinearizableReadAndRedirect(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	// Start a client-facing server per node; redirects point at the leader's
	// client address.
	clientAddrs := map[string]string{}
	servers := map[string]*Server{}
	for id, cn := range pc.nodes {
		srv := NewServer("127.0.0.1:0", NewCacheyHandler(cn.cs))
		if err := srv.Start(); err != nil {
			t.Fatalf("start client server %s: %v", id, err)
		}
		clientAddrs[id] = srv.Addr()
		servers[id] = srv
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()
	for _, cn := range pc.nodes {
		cn.cs.SetLeaderResolver(func(leaderID string) string { return clientAddrs[leaderID] })
	}

	pc.write(t, "k1", "v1")
	leader := pc.waitLeader(t)

	// Linearizable read on the leader via the client protocol.
	lc, err := client.NewClient(clientAddrs[leader])
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	resp, err := lc.SendCommand(protocol.Command{Type: protocol.GET, Key: "k1"})
	if err != nil || resp == nil || *resp == "" {
		t.Fatalf("leader GET k1: resp=%v err=%v", resp, err)
	}

	// A write to a follower is rejected with a redirect to the leader's
	// client address.
	var follower string
	for id := range pc.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	fc, err := client.NewClient(clientAddrs[follower])
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	_, err = fc.SendCommand(protocol.Command{Type: protocol.GET, Key: "k1"})
	addr, ok := client.RedirectLeader(err)
	if !ok {
		t.Fatalf("follower GET: expected a redirect error, got %v", err)
	}
	if addr != clientAddrs[leader] {
		t.Fatalf("redirect leader = %q, want %q", addr, clientAddrs[leader])
	}

	// The follower's ClusterStore surfaces the same redirect on reads/writes.
	if _, err := pc.nodes[follower].cs.Get("k1"); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower cs.Get: want raft.ErrNotLeader, got %v", err)
	}
	if err := pc.nodes[follower].cs.Put("k2", "v2"); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower cs.Put: want raft.ErrNotLeader, got %v", err)
	}
}

func TestClusterAddServer(t *testing.T) {
	// Start from a stable 3-node cluster, then add a fresh 4th node.
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	leader := pc.waitLeader(t)

	// Fresh joiner node d: empty log, listening, unknown configuration.
	tr := raft.NewTCPTransport(nil)
	dAddr, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc.addrs["d"] = dAddr
	d := newPersistentNode(t, "d", t.TempDir(), tr, pc.addrs, nil, pc.snapshotThreshold)
	d.node.Run()
	defer func() {
		d.node.Stop()
		d.tr.Close()
		d.wal.Close()
	}()
	if voters := d.node.Voters(); len(voters) != 1 || voters[0] != "d" {
		t.Fatalf("joiner starts with voters %v, want only itself", voters)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.AddServer(ctx, "d", dAddr); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	// d catches up: learns the 4-voter config and the committed data.
	waitFor(t, "joiner learns config and data", 15*time.Second, func() bool {
		if len(d.node.Voters()) != 4 {
			return false
		}
		got, err := d.store.Get("k1")
		return err == nil && *got == "v1"
	})

	// New writes replicate to the new member.
	pc.mu.Lock()
	pc.nodes["d"] = d
	pc.mu.Unlock()
	pc.write(t, "k2", "v2")
	waitFor(t, "new member replicates new writes", 15*time.Second, func() bool {
		got, err := d.store.Get("k2")
		return err == nil && *got == "v2"
	})

	// d is a real voter: killing the leader leaves three voters (a majority of
	// 4) that include the new member, able to elect a new leader.
	pc.mu.Lock()
	pc.nodes[leader].node.Stop()
	pc.nodes[leader].tr.Close()
	pc.nodes[leader].wal.Close()
	delete(pc.nodes, leader)
	pc.mu.Unlock()
	waitFor(t, "majority including the new member elects a leader", 15*time.Second, func() bool {
		leaders := 0
		for _, cn := range pc.nodes {
			if cn.node.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	})
}

func TestClusterRemoveServer(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	leader := pc.waitLeader(t)
	// Remove a follower deterministically (self-removal is covered separately).
	var victim string
	for id := range pc.nodes {
		if id != leader {
			victim = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.RemoveServer(ctx, victim); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	// victim is removed from the configuration on the remaining nodes.
	waitFor(t, "victim removed from the configuration", 15*time.Second, func() bool {
		for id, cn := range pc.nodes {
			if id == victim {
				continue
			}
			for _, v := range cn.node.Voters() {
				if v == victim {
					return false
				}
			}
		}
		return true
	})

	// Stop the victim; the remaining majority of 2 keeps serving.
	pc.mu.Lock()
	pc.nodes[victim].node.Stop()
	pc.nodes[victim].tr.Close()
	pc.nodes[victim].wal.Close()
	delete(pc.nodes, victim)
	pc.mu.Unlock()

	pc.write(t, "k2", "v2")
	waitFor(t, "remaining voters replicate after removal", 15*time.Second, func() bool {
		return pc.hasKey("k2", "v2")
	})
}

// TestClusterLeaderRemovesItself verifies that a leader removing itself still
// propagates the committed configuration before stepping down.
func TestClusterLeaderRemovesItself(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	leader := pc.waitLeader(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.RemoveServer(ctx, leader); err != nil {
		t.Fatalf("leader self-removal: %v", err)
	}

	// The remaining two members learn the new configuration (leader removed).
	waitFor(t, "remaining members apply the new configuration", 15*time.Second, func() bool {
		for id, cn := range pc.nodes {
			if id == leader {
				continue
			}
			for _, v := range cn.node.Voters() {
				if v == leader {
					return false
				}
			}
		}
		return true
	})

	// They keep serving with a 2-node majority.
	pc.mu.Lock()
	pc.nodes[leader].node.Stop()
	pc.nodes[leader].tr.Close()
	pc.nodes[leader].wal.Close()
	delete(pc.nodes, leader)
	pc.mu.Unlock()
	pc.write(t, "k2", "v2")
	waitFor(t, "remaining members replicate after leader removal", 15*time.Second, func() bool {
		return pc.hasKey("k2", "v2")
	})
}

// TestClusterLogCompaction drives enough writes to trigger snapshot +
// compaction, then verifies the log is truncated and data survives.
func TestClusterLogCompaction(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentClusterThreshold(t, []string{"a", "b", "c"}, dirs, 5)
	defer pc.stopAll()

	leader := pc.waitLeader(t)
	const n = 60
	for i := 0; i < n; i++ {
		pc.write(t, fmt.Sprintf("k%d", i), "v")
	}

	// The leader compacted its log (base advanced past the first writes).
	waitFor(t, "leader log to be compacted", 15*time.Second, func() bool {
		return pc.nodes[leader].node.LogBase() > 0
	})
	if base := pc.nodes[leader].node.LogBase(); base < n/2 {
		t.Fatalf("leader log base = %d, want it past the snapshot point", base)
	}

	// Every key survives on every node.
	waitFor(t, "all keys present after compaction", 15*time.Second, func() bool {
		for i := 0; i < n; i++ {
			if !pc.hasKey(fmt.Sprintf("k%d", i), "v") {
				return false
			}
		}
		return true
	})
}

// TestClusterInstallSnapshotCatchUp verifies a brand-new member that joins
// after the leader has compacted catches up via InstallSnapshot.
func TestClusterInstallSnapshotCatchUp(t *testing.T) {
	// Use a 3-node cluster for the write + compaction phase (a majority of 3
	// is stable under load), then join a fresh 4th node that must be caught up
	// via InstallSnapshot after the leader has compacted.
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentClusterThreshold(t, []string{"a", "b", "c"}, dirs, 5)
	defer pc.stopAll()

	leader := pc.waitLeader(t)
	const n = 60
	for i := 0; i < n; i++ {
		pc.write(t, fmt.Sprintf("k%d", i), "v")
	}
	waitFor(t, "leader log to be compacted", 15*time.Second, func() bool {
		return pc.nodes[leader].node.LogBase() > 0
	})

	// A fresh joiner with an empty log must be caught up via InstallSnapshot.
	tr := raft.NewTCPTransport(nil)
	cAddr, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc.addrs["d"] = cAddr
	c := newPersistentNode(t, "d", t.TempDir(), tr, pc.addrs, nil, pc.snapshotThreshold)
	c.node.Run()
	defer func() {
		c.node.Stop()
		c.tr.Close()
		c.wal.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.AddServer(ctx, "d", cAddr); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	waitFor(t, "joiner catches up via snapshot and entries", 15*time.Second, func() bool {
		if len(c.node.Voters()) != 4 {
			return false
		}
		for i := 0; i < n; i++ {
			got, err := c.store.Get(fmt.Sprintf("k%d", i))
			if err != nil || *got != "v" {
				return false
			}
		}
		return true
	})
}

// TestClusterRestartFromSnapshot verifies a restarted node recovers state from
// a persisted snapshot plus the WAL tail.
func TestClusterRestartFromSnapshot(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentClusterThreshold(t, []string{"a", "b", "c"}, dirs, 5)
	defer pc.stopAll()

	leader := pc.waitLeader(t)
	const n = 60
	for i := 0; i < n; i++ {
		pc.write(t, fmt.Sprintf("k%d", i), "v")
	}
	waitFor(t, "leader log to be compacted", 15*time.Second, func() bool {
		return pc.nodes[leader].node.LogBase() > 0
	})

	// Crash and restart a follower from the same dir; it recovers the snapshot
	// + WAL tail and rejoins.
	var victim string
	for id, cn := range pc.nodes {
		if !cn.node.IsLeader() {
			victim = id
			break
		}
	}
	pc.mu.Lock()
	pc.nodes[victim].node.Stop()
	pc.nodes[victim].wal.Close()
	pc.mu.Unlock()
	pc.restart(t, victim)

	waitFor(t, "restarted node recovers from snapshot", 15*time.Second, func() bool {
		for i := 0; i < n; i++ {
			got, err := pc.nodes[victim].store.Get(fmt.Sprintf("k%d", i))
			if err != nil || *got != "v" {
				return false
			}
		}
		return true
	})
}
