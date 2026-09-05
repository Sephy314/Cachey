package pbft

import (
	"context"
	"testing"
	"time"
)

// startTCPCluster boots a 4-replica PBFT cluster over real TCP transports with
// the key handshake (no out-of-band key wiring), returning replicas, fsms and
// transports. view-0 primary is the lexicographically first id.
func startTCPCluster(t *testing.T, ids []string) (map[string]*Replica, map[string]*fsm, map[string]*TCPTransport) {
	t.Helper()
	peersOf := func(id string) []string {
		var out []string
		for _, other := range ids {
			if other != id {
				out = append(out, other)
			}
		}
		return out
	}
	nodes := make(map[string]*Replica)
	fsms := make(map[string]*fsm)
	transports := make(map[string]*TCPTransport)
	for _, id := range ids {
		tr := NewTCPTransport(nil)
		f := &fsm{}
		r, err := NewReplica(Config{ID: id, Peers: peersOf(id)}, tr, f.apply)
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		tr.SetNode(r)
		if _, err := tr.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("Listen(%s): %v", id, err)
		}
		nodes[id] = r
		fsms[id] = f
		transports[id] = tr
	}
	// Register every peer address on every transport.
	addrs := make(map[string]string, len(ids))
	for _, id := range ids {
		addrs[id] = transports[id].Addr()
	}
	for _, id := range ids {
		transports[id].SetPeers(addrs)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			transports[id].Close()
		}
	})
	return nodes, fsms, transports
}

// TestTCPClusterReplication verifies the engine reaches consensus over real TCP
// with the dynamic key handshake: writes submitted to the view-0 primary are
// replicated and executed on all four replicas in order, and a non-primary
// rejects submissions.
func TestTCPClusterReplication(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startTCPCluster(t, ids)
	primary := nodes["r0"] // view-0 primary

	if _, err := primary.Submit([]byte("a")); err != nil {
		t.Fatalf("Submit(a): %v", err)
	}
	seq, err := primary.Submit([]byte("b"))
	if err != nil {
		t.Fatalf("Submit(b): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := primary.WaitApplied(ctx, seq); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq, err)
	}

	want := []string{"1:a", "2:b"}
	for _, id := range ids {
		waitFor(t, id+" to apply over TCP", 5*time.Second, func() bool {
			return equal(fsms[id].snapshot(), want)
		})
	}
	// A backup rejects submissions over the same transport wiring.
	if _, err := nodes["r1"].Submit([]byte("x")); err != ErrNotPrimary {
		t.Fatalf("Submit on backup = %v, want ErrNotPrimary", err)
	}
}
