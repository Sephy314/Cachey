package server

import (
	"context"
	"errors"
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
	mu    sync.Mutex
	nodes map[string]*clusterNode
	addrs map[string]string
	ids   []string
}

// newPersistentCluster starts a raft cluster over TCP with WAL persistence.
// dirs maps node id to its WAL directory (reuse a dir to test restart).
func newPersistentCluster(t *testing.T, ids []string, dirs map[string]string) *persistentCluster {
	t.Helper()
	pc := &persistentCluster{ids: ids}
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
		pc.nodes[id] = newPersistentNode(t, id, dirs[id], trs[id], pc.addrs, otherIDs(id, pc.addrs))
	}
	for _, id := range ids {
		pc.nodes[id].node.Run()
	}
	return pc
}

func newPersistentNode(t *testing.T, id, dir string, tr *raft.TCPTransport, addrs map[string]string, peers []string) *clusterNode {
	t.Helper()
	st := store.NewCacheyStore()
	cfg := raft.Config{
		ID:                id,
		Peers:             peers,
		HeartbeatInterval: 50 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
	}
	n, err := raft.NewNode(cfg, tr, NewRaftApply(st))
	if err != nil {
		t.Fatalf("NewNode(%s): %v", id, err)
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
	cn := newPersistentNode(t, id, old.dir, tr, pc.addrs, otherIDs(id, pc.addrs))
	cn.node.Run()
	pc.nodes[id] = cn
}

// waitLeader returns the cluster's single leader id.
func (pc *persistentCluster) waitLeader(t *testing.T) string {
	t.Helper()
	var leader string
	waitFor(t, "a leader to be elected", 115*time.Second, func() bool {
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

// write puts a key on the leader and waits for commit+apply.
func (pc *persistentCluster) write(t *testing.T, key, val string) {
	t.Helper()
	leader := pc.waitLeader(t)
	if err := pc.nodes[leader].cs.Put(key, val); err != nil {
		t.Fatalf("Put(%s=%s): %v", key, val, err)
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
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	pc.write(t, "k2", "v2")
	waitFor(t, "all nodes replicate both writes", 15*time.Second, func() bool {
		return pc.hasKey("k1", "v1") && pc.hasKey("k2", "v2")
	})

	// Kill the leader; the remaining two elect a new one.
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
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
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
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	leader := pc.waitLeader(t)

	// Fresh joiner node c: empty log, listening, unknown configuration.
	tr := raft.NewTCPTransport(nil)
	cAddr, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc.addrs["c"] = cAddr
	c := newPersistentNode(t, "c", t.TempDir(), tr, pc.addrs, nil)
	c.node.Run()
	defer func() {
		c.node.Stop()
		c.tr.Close()
		c.wal.Close()
	}()
	if voters := c.node.Voters(); len(voters) != 1 || voters[0] != "c" {
		t.Fatalf("joiner starts with voters %v, want only itself", voters)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.AddServer(ctx, "c", cAddr); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	// c catches up: learns the 3-voter config and the committed data.
	waitFor(t, "joiner learns config and data", 15*time.Second, func() bool {
		if len(c.node.Voters()) != 3 {
			return false
		}
		got, err := c.store.Get("k1")
		return err == nil && *got == "v1"
	})

	// New writes replicate to the new member.
	pc.mu.Lock()
	pc.nodes["c"] = c
	pc.mu.Unlock()
	pc.write(t, "k2", "v2")
	waitFor(t, "new member replicates new writes", 15*time.Second, func() bool {
		got, err := c.store.Get("k2")
		return err == nil && *got == "v2"
	})

	// c is a real voter: killing the leader leaves {b,c}, a majority of 2 that
	// includes the new member, able to elect a new leader.
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pc.nodes[leader].node.RemoveServer(ctx, "b"); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	// b is removed from the configuration on the remaining nodes.
	waitFor(t, "b removed from the configuration", 15*time.Second, func() bool {
		for id, cn := range pc.nodes {
			if id == "b" {
				continue
			}
			for _, v := range cn.node.Voters() {
				if v == "b" {
					return false
				}
			}
		}
		return true
	})

	// Stop b; the remaining majority of 2 keeps serving.
	pc.mu.Lock()
	pc.nodes["b"].node.Stop()
	pc.nodes["b"].tr.Close()
	pc.nodes["b"].wal.Close()
	delete(pc.nodes, "b")
	pc.mu.Unlock()

	pc.write(t, "k2", "v2")
	waitFor(t, "remaining voters replicate after removal", 15*time.Second, func() bool {
		return pc.hasKey("k2", "v2")
	})
}
