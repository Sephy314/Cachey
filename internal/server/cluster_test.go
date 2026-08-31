package server

import (
	"sync"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
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
		pc.nodes[id] = newPersistentNode(t, id, dirs[id], trs[id], pc.addrs)
	}
	for _, id := range ids {
		pc.nodes[id].node.Run()
	}
	return pc
}

func newPersistentNode(t *testing.T, id, dir string, tr *raft.TCPTransport, addrs map[string]string) *clusterNode {
	t.Helper()
	st := store.NewCacheyStore()
	cfg := raft.Config{
		ID:                id,
		Peers:             otherIDs(id, addrs),
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   100 * time.Millisecond,
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
	return &clusterNode{id: id, dir: dir, store: st, node: n, tr: tr, wal: w, cs: NewClusterStore(n, st)}
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
	cn := newPersistentNode(t, id, old.dir, tr, pc.addrs)
	cn.node.Run()
	pc.nodes[id] = cn
}

// waitLeader returns the cluster's single leader id.
func (pc *persistentCluster) waitLeader(t *testing.T) string {
	t.Helper()
	var leader string
	waitFor(t, "a leader to be elected", 5*time.Second, func() bool {
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
	waitFor(t, "all nodes replicate both writes", 5*time.Second, func() bool {
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
	waitFor(t, "live nodes replicate the post-failover write", 5*time.Second, func() bool {
		return pc.hasKey("k1", "v1") && pc.hasKey("k2", "v2") && pc.hasKey("k3", "v3")
	})
}

func TestPersistentClusterRestart(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	pc := newPersistentCluster(t, []string{"a", "b", "c"}, dirs)
	defer pc.stopAll()

	pc.write(t, "k1", "v1")
	pc.write(t, "k2", "v2")
	waitFor(t, "all nodes replicate before restart", 5*time.Second, func() bool {
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
	waitFor(t, "restarted node catches up to committed state", 5*time.Second, func() bool {
		v, err := pc.nodes[victim].store.Get("k1")
		return err == nil && *v == "v1"
	})
}
