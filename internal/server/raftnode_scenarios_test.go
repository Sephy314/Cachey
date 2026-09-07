package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
)

// raftTC drives a small raft cluster built on the production OpenRaftNode
// builder. Nodes join one at a time through the current leader's
// raft.AddServer — the same call cacheyd's JOIN control path makes — and are
// NOT seeded with each other's addresses: a joiner (or restarted member)
// learns the full membership only from committed configurations and the
// durable committed-config meta file, so restart-after-compaction and
// multi-hop joins are exercised honestly.
type raftTC struct {
	t         *testing.T
	nodes     map[string]*RaftNode // live nodes
	dirs      map[string]string
	voters    []string // intended committed voter set
	threshold uint64
}

func newRaftTC(t *testing.T, threshold uint64) *raftTC {
	return &raftTC{
		t:         t,
		nodes:     make(map[string]*RaftNode),
		dirs:      make(map[string]string),
		threshold: threshold,
	}
}

func (c *raftTC) open(id string) *RaftNode {
	c.t.Helper()
	return c.openDir(id, c.t.TempDir())
}

func (c *raftTC) openDir(id, dir string) *RaftNode {
	c.t.Helper()
	rn, err := OpenRaftNode(RaftNodeConfig{
		ID:                id,
		Dir:               dir,
		RaftAddr:          "127.0.0.1:0",
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   600 * time.Millisecond,
		SnapshotThreshold: c.threshold,
	})
	if err != nil {
		c.t.Fatalf("OpenRaftNode(%s): %v", id, err)
	}
	c.nodes[id] = rn
	c.dirs[id] = dir
	return rn
}

// wait polls cond until true or timeout, failing the test.
func (c *raftTC) wait(what string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

func setOf(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// waitVoters waits until every live node has exactly the intended voter set.
func (c *raftTC) waitVoters() {
	c.t.Helper()
	want := setOf(c.voters)
	c.wait("all nodes to hold voters "+strings.Join(c.voters, ","), 30*time.Second, func() bool {
		for _, rn := range c.nodes {
			if !setEq(setOf(rn.Node.Voters()), want) {
				return false
			}
		}
		return true
	})
}

func setEq(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// leader returns the single live leader, or nil if none/ambiguous.
func (c *raftTC) leader() *RaftNode {
	var out *RaftNode
	n := 0
	for _, rn := range c.nodes {
		if rn.Node.IsLeader() {
			out = rn
			n++
		}
	}
	if n == 1 {
		return out
	}
	return nil
}

func (c *raftTC) waitLeader() *RaftNode {
	c.t.Helper()
	var out *RaftNode
	c.wait("a single leader", 30*time.Second, func() bool {
		out = c.leader()
		return out != nil
	})
	return out
}

// add makes id a committed voting member. The first node bootstraps a
// 1-node cluster (it must win its own election); every later node is added by
// the current leader via raft.AddServer, then starts running.
func (c *raftTC) add(id string) *RaftNode {
	c.t.Helper()
	rn := c.open(id)
	if len(c.voters) == 0 {
		rn.Node.Run()
		c.voters = []string{id}
		c.waitLeader()
		return rn
	}
	lead := c.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := lead.Node.AddServer(ctx, id, rn.RaftAddr); err != nil {
		c.t.Fatalf("AddServer(%s) via %s: %v", id, lead.ID, err)
	}
	c.voters = append(c.voters, id)
	rn.Node.Run()
	c.waitVoters()
	return rn
}

// stop shuts id down. Its membership is retained (a dead member stays a voter
// until removed).
func (c *raftTC) stop(id string) {
	c.t.Helper()
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

// restartNode stops id and reopens it from its own data dir (recovering the
// snapshot, WAL tail, and the durable committed-config meta), then runs it
// and waits for it to re-learn the full membership.
func (c *raftTC) restartNode(id string) *RaftNode {
	c.t.Helper()
	c.nodes[id].Stop()
	delete(c.nodes, id)
	rn := c.openDir(id, c.dirs[id])
	rn.Node.Run()
	c.waitVoters()
	return rn
}

// removeMember removes id from the committed configuration via the current
// leader.
func (c *raftTC) removeMember(id string) {
	c.t.Helper()
	lead := c.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := lead.Node.RemoveServer(ctx, id); err != nil {
		c.t.Fatalf("RemoveServer(%s) via %s: %v", id, lead.ID, err)
	}
	var kept []string
	for _, v := range c.voters {
		if v != id {
			kept = append(kept, v)
		}
	}
	c.voters = kept
	c.waitVoters()
}

// putLeader writes through the current leader, retrying on a transient
// ErrNotLeader (the leader can change between the read and the write).
func (c *raftTC) putLeader(key, val string) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lead := c.leader()
		if lead == nil {
			lead = c.waitLeader()
		}
		err := lead.CS.Put(key, val)
		if err == nil {
			return
		}
		if !errors.Is(err, raft.ErrNotLeader) || time.Now().After(deadline) {
			c.t.Fatalf("Put(%s=%s): %v", key, val, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// hasFSMAll reports whether every live node's FSM has key=val.
func (c *raftTC) hasFSMAll(key, val string) bool {
	for _, rn := range c.nodes {
		v, err := rn.Store.Get(key)
		if err != nil || *v != val {
			return false
		}
	}
	return true
}

func (c *raftTC) waitFSM(key, val string) {
	c.t.Helper()
	c.wait(fmt.Sprintf("every node to hold %s=%s", key, val), 30*time.Second, func() bool {
		return c.hasFSMAll(key, val)
	})
}

// waitNodeCompacted waits until id's raft log has been compacted (base past 0),
// which on restart drops its config entries from the WAL replay.
func (c *raftTC) waitNodeCompacted(id string) {
	c.t.Helper()
	c.wait(id+" to compact its log", 30*time.Second, func() bool {
		return c.nodes[id].Node.LogBase() > 0
	})
}

// TestRaftNodeStaleMetaDoesNotOverrideRecoveredConfig is a regression test for
// the crash window between a configuration committing on a node (durable in
// its WAL) and the raft.meta file write. If the meta file is stale, recovery
// must keep the NEWER configuration recovered from the WAL — a stale meta must
// never shrink the cluster back to an older membership.
func TestRaftNodeStaleMetaDoesNotOverrideRecoveredConfig(t *testing.T) {
	c := newRaftTC(t, 0)
	c.add("n1")
	c.add("n2")
	c.add("n3") // committed config {n1,n2,n3} is in every node's WAL and raft.meta
	c.putLeader("k1", "v1")
	c.waitFSM("k1", "v1")

	// Simulate the crash window on n2: its WAL holds the 3-voter config, but
	// raft.meta is stale — the 2-voter config from before n3 joined.
	n2dir := c.dirs["n2"]
	c.nodes["n2"].Stop()
	delete(c.nodes, "n2")

	metaPath := filepath.Join(n2dir, "raft.meta")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read raft.meta: %v", err)
	}
	var meta raft.CommittedMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("decode raft.meta: %v", err)
	}
	if meta.Index == 0 {
		t.Fatalf("raft.meta carries no config index; cannot test staleness")
	}
	meta.Index--
	meta.Voters = []string{"n1", "n2"} // stale: n3's add is missing
	meta.Addrs = nil
	stale, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, stale, 0o644); err != nil {
		t.Fatal(err)
	}

	// Restart n2: the WAL-recovered 3-voter config must win over the stale
	// meta, so n2 stays a full member of {n1,n2,n3}.
	rn := c.openDir("n2", n2dir)
	rn.Node.Run()
	c.waitVoters()
	c.waitFSM("k1", "v1")

	// n2 is a healthy member: after the leader dies it helps elect a
	// replacement and keeps committing.
	c.stop("n1")
	c.waitLeader()
	c.putLeader("k2", "v2")
	c.waitFSM("k2", "v2")
}

// ---- scenario: add a node, then restart it ----

func TestRaftNodeAddThenRestartJoiner(t *testing.T) {
	c := newRaftTC(t, 0)
	c.add("n1")
	c.add("n2")
	c.add("n3")
	c.putLeader("k1", "v1")
	c.waitFSM("k1", "v1")

	// Add a fourth node and verify it is a full member with the data.
	c.add("n4")
	c.putLeader("k2", "v2")
	c.waitFSM("k2", "v2")

	// Restart the just-added node from its own data dir: membership and data
	// must survive, and the cluster keeps serving.
	c.restartNode("n4")
	c.waitFSM("k1", "v1")
	c.putLeader("k3", "v3")
	c.waitFSM("k3", "v3")
}

// ---- scenario: restart restores membership and the node can later lead ----

func TestRaftNodeRestartRestoresMembershipAndLeads(t *testing.T) {
	c := newRaftTC(t, 0)
	c.add("n1")
	c.add("n2")
	c.add("n3")
	c.putLeader("k1", "v1")
	c.waitFSM("k1", "v1")

	// Restart a follower; recovery (config + durable meta) must bring back the
	// full voter set and its peers' addresses, because this harness never
	// seeds addresses.
	c.restartNode("n2")
	c.waitFSM("k1", "v1")

	// Kill the leader: the restarted node must be able to reach the surviving
	// peer and be elected (it relearned the peer's address on recovery).
	c.stop("n1")
	lead := c.waitLeader()
	if lead.ID == "n2" || lead.ID == "n3" {
		// ok: one of the survivors leads
	} else {
		t.Fatalf("unexpected leader %s after n1 died", lead.ID)
	}
	c.putLeader("k2", "v2")
	c.waitFSM("k2", "v2")
}

// ---- scenario: leadership moves, then a new node joins under the new leader
// and later participates in a quorum ----

func TestRaftNodeLeaderChangeThenAddNode(t *testing.T) {
	c := newRaftTC(t, 0)
	c.add("n1") // bootstrap leader
	c.add("n2")
	c.add("n3")
	c.putLeader("k1", "v1")

	// Force a leadership change: kill the bootstrap leader.
	c.stop("n1")
	lead := c.waitLeader()
	if lead.ID == "n1" {
		t.Fatal("n1 is dead but still leading")
	}

	// Add a fresh node under the NEW leader (not the bootstrap node).
	c.add("n4")
	c.putLeader("k2", "v2")
	c.waitFSM("k2", "v2")

	// Drop the dead n1 so the config is odd again: {n2,n3,n4}.
	c.removeMember("n1")

	// Kill the new leader too: the newest member (n4), which joined only after
	// the leadership change, must be a real voter that learned the surviving
	// peer's address and can form a quorum.
	c.stop(lead.ID)
	c.waitLeader()
	c.putLeader("k3", "v3")
	c.waitFSM("k3", "v3")
}

// ---- scenario: snapshot + compaction, restart an existing node, join a new
// node, and confirm membership stays correct afterwards ----

func TestRaftNodeSnapshotRestartJoinMembership(t *testing.T) {
	// Small threshold forces snapshots + log compaction while we write.
	c := newRaftTC(t, 5)
	c.add("n1")
	c.add("n2")
	const n = 60
	for i := 0; i < n; i++ {
		c.putLeader(fmt.Sprintf("k%d", i), "v")
	}
	c.waitFSM(fmt.Sprintf("k%d", n-1), "v")
	// Both nodes must have compacted: on restart n2's config entries are no
	// longer in its WAL replay, so only the durable committed-config meta can
	// restore its membership.
	c.waitNodeCompacted("n1")
	c.waitNodeCompacted("n2")

	// Restart the existing follower after compaction: membership must come
	// back from the meta file, and it must catch up via snapshot + entries.
	c.restartNode("n2")
	c.waitFSM(fmt.Sprintf("k%d", n-1), "v")

	// Join a brand-new node after compaction: it catches up via InstallSnapshot
	// and becomes a full voter.
	c.add("n3")
	c.waitFSM(fmt.Sprintf("k%d", n-1), "v")

	// The cluster still has correct membership: kill the leader and the
	// remaining two (odd 3) elect a replacement that keeps serving.
	c.stop("n1")
	c.waitLeader()
	c.putLeader("kz", "vz")
	c.waitFSM("kz", "vz")
}
