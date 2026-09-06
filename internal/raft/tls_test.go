package raft

import (
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls/testca"
)

// TestTCPClusterTLS runs a real 3-node Raft cluster with mutual TLS on every
// transport: leader election and replication must work when every node dials
// peers whose certificates (DNS SAN = node id) it pins, and each listener
// admits only known node identities.
func TestTCPClusterTLS(t *testing.T) {
	ids := []string{"a", "b", "c"}
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := make(map[string][]byte, len(ids))
	keys := make(map[string][]byte, len(ids))
	for _, id := range ids {
		certs[id], keys[id], err = ca.Issue(id)
		if err != nil {
			t.Fatalf("issue %s: %v", id, err)
		}
	}

	nodes, fsms, transports := startTCPClusterWith(t, ids, clusterOpts{}, func(id string, tr *TCPTransport) {
		tr.EnableTLS(ca.CertPEM(), certs[id], keys[id])
	})
	defer stopTCPCluster(nodes, transports)

	leader := waitLeader(t, nodes)
	propose(t, nodes[leader], "k1=v1")
	waitFor(t, "all nodes apply the entry over mTLS", 5*time.Second, func() bool {
		for _, f := range fsms {
			if !equalStrings(f.snapshot(), []string{"k1=v1"}) {
				return false
			}
		}
		return true
	})
}
