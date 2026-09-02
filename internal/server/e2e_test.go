package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/pkg/client"
)

// e2eCluster is a persistent raft cluster with a client-facing server on every
// node, so tests exercise the full path: client protocol → handler →
// ClusterStore → raft → replicated FSM.
type e2eCluster struct {
	pc         *persistentCluster
	servers    map[string]*Server
	clientAddr map[string]string
}

func newE2ECluster(t *testing.T, ids []string, dirs map[string]string, threshold uint64) *e2eCluster {
	t.Helper()
	pc := newPersistentClusterThreshold(t, ids, dirs, threshold)
	ec := &e2eCluster{
		pc:         pc,
		servers:    make(map[string]*Server),
		clientAddr: make(map[string]string),
	}
	for id, cn := range pc.nodes {
		srv := NewServer("127.0.0.1:0", NewCacheyHandler(cn.cs))
		if err := srv.Start(); err != nil {
			t.Fatalf("start client server %s: %v", id, err)
		}
		ec.servers[id] = srv
		ec.clientAddr[id] = srv.Addr()
	}
	for _, cn := range pc.nodes {
		cn.cs.SetLeaderResolver(func(leaderID string) string { return ec.clientAddr[leaderID] })
	}
	return ec
}

func (ec *e2eCluster) stop() {
	for _, s := range ec.servers {
		s.Stop()
	}
	ec.pc.stopAll()
}

func (ec *e2eCluster) send(id string, cmd protocol.Command) (*string, error) {
	c, err := client.NewClient(ec.clientAddr[id])
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.SendCommand(cmd)
}

// putTo writes to a specific node's client (used to target a stale leader).
func (ec *e2eCluster) putTo(t *testing.T, id, key, val string) error {
	t.Helper()
	_, err := ec.send(id, protocol.Command{Type: protocol.PUT, Key: key, Val: val})
	return err
}

// put writes through the current leader.
func (ec *e2eCluster) put(t *testing.T, key, val string) {
	t.Helper()
	if err := ec.putTo(t, ec.pc.waitLeader(t), key, val); err != nil {
		t.Fatalf("PUT %s=%s via leader: %v", key, val, err)
	}
}

// getFrom reads from a specific node's client.
func (ec *e2eCluster) getFrom(t *testing.T, id, key string) string {
	t.Helper()
	resp, err := ec.send(id, protocol.Command{Type: protocol.GET, Key: key})
	if err != nil {
		t.Fatalf("GET %s via %s: %v", key, id, err)
	}
	var cmd protocol.Command
	if err := json.Unmarshal([]byte(*resp), &cmd); err != nil {
		t.Fatalf("GET %s: bad response %q: %v", key, *resp, err)
	}
	return cmd.Val
}

// get reads through the current leader.
func (ec *e2eCluster) get(t *testing.T, key string) string {
	t.Helper()
	return ec.getFrom(t, ec.pc.waitLeader(t), key)
}

// kill stops a node (raft + transport + WAL + client server), removes it from
// the live set, and returns its data dir so it can later be recovered via
// rejoin.
func (ec *e2eCluster) kill(t *testing.T, id string) string {
	t.Helper()
	ec.pc.mu.Lock()
	defer ec.pc.mu.Unlock()
	cn := ec.pc.nodes[id]
	if cn == nil {
		t.Fatalf("kill %s: not a member", id)
	}
	cn.node.Stop()
	cn.tr.Close()
	cn.wal.Close()
	delete(ec.pc.nodes, id)
	if s := ec.servers[id]; s != nil {
		s.Stop()
		delete(ec.servers, id)
		delete(ec.clientAddr, id)
	}
	return cn.dir
}

// rejoin recovers a previously killed node from its data dir and brings its
// client server back up.
func (ec *e2eCluster) rejoin(t *testing.T, id, dir string) {
	t.Helper()
	ec.pc.mu.Lock()
	tr := raft.NewTCPTransport(nil)
	addr, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("re-listen %s: %v", id, err)
	}
	ec.pc.addrs[id] = addr
	for peer, cn := range ec.pc.nodes {
		cn.tr.SetPeers(peerAddrsOf(peer, ec.pc.addrs))
	}
	cn := newPersistentNode(t, id, dir, tr, ec.pc.addrs, otherIDs(id, ec.pc.addrs), ec.pc.snapshotThreshold)
	cn.node.Run()
	ec.pc.nodes[id] = cn
	ec.pc.mu.Unlock()

	srv := NewServer("127.0.0.1:0", NewCacheyHandler(cn.cs))
	if err := srv.Start(); err != nil {
		t.Fatalf("client server for rejoined %s: %v", id, err)
	}
	ec.servers[id] = srv
	ec.clientAddr[id] = srv.Addr()
	cn.cs.SetLeaderResolver(func(leaderID string) string { return ec.clientAddr[leaderID] })
}

// partition isolates the transport messages between isolated and every other
// live node in both directions.
func (ec *e2eCluster) partition(t *testing.T, isolated string, on bool) {
	t.Helper()
	ec.pc.mu.Lock()
	defer ec.pc.mu.Unlock()
	for _, cn := range ec.pc.nodes {
		if !on {
			cn.tr.SetFaultInjector(nil)
			continue
		}
		iso := isolated
		cn.tr.SetFaultInjector(func(from, to string) bool {
			return (from == iso && to != iso) || (from != iso && to == iso)
		})
	}
}

// ---- scenario 1: Put replicates to every node, then Get returns it ----

func TestE2EPutReplicateAndGet(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	ec.put(t, "k1", "v1")
	// The write is distributed to every node's FSM.
	waitFor(t, "k1 replicated to all nodes", 15*time.Second, func() bool {
		return ec.pc.hasKey("k1", "v1")
	})
	// And readable through the leader.
	if got := ec.get(t, "k1"); got != "v1" {
		t.Fatalf("GET k1 = %q, want v1", got)
	}
}

// ---- scenario 2: leader death → replacement, cluster keeps serving ----

func TestE2ELeaderDeathReplacement(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	ec.put(t, "k1", "v1")
	old := ec.pc.waitLeader(t)

	ec.kill(t, old)
	waitFor(t, "a replacement leader is elected", 15*time.Second, func() bool {
		newL := ec.pc.leaderID()
		return newL != "" && newL != old
	})

	// The cluster keeps serving both old and new data through the new leader.
	ec.put(t, "k2", "v2")
	if got := ec.get(t, "k1"); got != "v1" {
		t.Fatalf("GET k1 after failover = %q, want v1", got)
	}
	if got := ec.get(t, "k2"); got != "v2" {
		t.Fatalf("GET k2 after failover = %q, want v2", got)
	}
}

// ---- scenario 3: node death → restart → log recovery → Get ----

func TestE2ENodeRestartLogRecovery(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	ec.put(t, "k1", "v1")
	waitFor(t, "k1 distributed", 15*time.Second, func() bool { return ec.pc.hasKey("k1", "v1") })

	// Kill the leader and commit more on the new leader while it is down.
	dead := ec.pc.waitLeader(t)
	dir := ec.kill(t, dead)
	ec.put(t, "k2", "v2")
	ec.put(t, "k3", "v3")

	// Bring the dead node back from its durable log.
	ec.rejoin(t, dead, dir)
	waitFor(t, "restarted node recovers its log and catches up", 15*time.Second, func() bool {
		got, err := ec.pc.nodes[dead].store.Get("k3")
		return err == nil && *got == "v3"
	})
	if got := ec.get(t, "k3"); got != "v3" {
		t.Fatalf("GET k3 = %q, want v3", got)
	}
}

// ---- scenario 4: a node with a stale log loses the election ----

func TestE2EStaleNodeLosesElection(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	leader := ec.pc.waitLeader(t)
	// Isolate one follower so it misses the committed entries below.
	var stale string
	for id := range ec.pc.nodes {
		if id != leader {
			stale = id
			break
		}
	}
	ec.partition(t, stale, true)

	for i := 0; i < 5; i++ {
		ec.put(t, fmt.Sprintf("k%d", i), "v")
	}
	// The isolated node is behind: it does not have the committed keys.
	if got, err := ec.pc.nodes[stale].store.Get("k0"); err == nil {
		t.Fatalf("isolated node unexpectedly has k0=%q", *got)
	}
	// While isolated it keeps campaigning at rising terms (no quorum). Wait
	// until it has campaigned at least once above the quorum's term.
	waitFor(t, "isolated node's term to advance above the quorum", 40*time.Second, func() bool {
		return ec.pc.nodes[stale].node.Term() > ec.pc.nodes[leader].node.Term()
	})

	// Heal the partition: the stale node campaigns but is rejected (its log is
	// not up-to-date); a node holding the committed log is elected instead.
	ec.partition(t, stale, false)
	waitFor(t, "a non-stale node with the committed log leads", 40*time.Second, func() bool {
		l := ec.pc.leaderID()
		if l == "" || l == stale {
			return false
		}
		// The leader must actually hold the committed entries.
		for i := 0; i < 5; i++ {
			got, err := ec.pc.nodes[l].store.Get(fmt.Sprintf("k%d", i))
			if err != nil || *got != "v" {
				return false
			}
		}
		return true
	})
	// The stale node eventually rejoins as a follower and catches up.
	waitFor(t, "stale node catches up after losing", 40*time.Second, func() bool {
		for i := 0; i < 5; i++ {
			got, err := ec.pc.nodes[stale].store.Get(fmt.Sprintf("k%d", i))
			if err != nil || *got != "v" {
				return false
			}
		}
		return true
	})
}

// ---- scenario 5: leader isolated → majority elects, no split brain; heal ----

func TestE2ENetworkPartition(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	ec.put(t, "k1", "v1")
	old := ec.pc.waitLeader(t)

	// Isolate the leader from the rest of the cluster. The old leader keeps
	// running and still believes it is the leader, so the cluster view is
	// ambiguous until it hears the higher term — we scope to the majority.
	ec.partition(t, old, true)

	// The majority (excluding the isolated leader) elects a new leader.
	var newLeader string
	waitFor(t, "majority elects a new leader after leader partition", 15*time.Second, func() bool {
		newLeader = ec.pc.leaderIDExcept(old)
		return newLeader != ""
	})
	if newLeader == "" || newLeader == old {
		t.Fatalf("majority did not elect a new leader")
	}

	// The majority side keeps serving old and new data.
	if err := ec.putTo(t, newLeader, "k2", "v2"); err != nil {
		t.Fatalf("PUT k2 via majority leader %s: %v", newLeader, err)
	}
	if got := ec.getFrom(t, newLeader, "k1"); got != "v1" {
		t.Fatalf("GET k1 via majority leader = %q, want v1", got)
	}

	// The isolated old leader cannot commit: it lacks a quorum, so its write
	// must fail rather than succeed (no split brain).
	errCh := make(chan error, 1)
	go func() { errCh <- ec.putTo(t, old, "kStale", "v") }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("isolated leader committed kStale — split brain")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("isolated leader's PUT neither succeeded nor failed promptly")
	}

	// Heal: the old leader learns the higher term, steps down as a follower,
	// and catches up to the majority's writes.
	ec.partition(t, old, false)
	waitFor(t, "old leader rejoins as follower and catches up", 20*time.Second, func() bool {
		if ec.pc.nodes[old].node.IsLeader() {
			return false
		}
		got, err := ec.pc.nodes[old].store.Get("k2")
		return err == nil && *got == "v2"
	})
	// The stale write the isolated leader attempted never became committed.
	if got, err := ec.pc.nodes[old].store.Get("kStale"); err == nil {
		t.Fatalf("stale write from isolated leader leaked: kStale=%q", *got)
	}
}
