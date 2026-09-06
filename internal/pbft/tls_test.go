package pbft

import (
	"context"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
)

// TestTCPClusterTLS runs a real 4-replica PBFT cluster with mutual TLS on
// every transport: the Hello key exchange must work over TLS (where the
// exchange is trusted to the mTLS-authenticated peer, not TOFU) and consensus
// must still be reached.
func TestTCPClusterTLS(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	ca, err := mtls.NewCA()
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

	nodes, fsms, _ := startTCPClusterWith(t, ids, func(id string, tr *TCPTransport) {
		tr.EnableTLS(ca.CertPEM(), certs[id], keys[id])
	})
	primary := nodes["r0"] // view-0 primary

	seq, err := primary.Submit([]byte("a"))
	if err != nil {
		t.Fatalf("Submit(a): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := primary.WaitApplied(ctx, seq); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq, err)
	}

	want := []string{"1:a"}
	for _, id := range ids {
		waitFor(t, id+" to apply the request over mTLS", 10*time.Second, func() bool {
			return equal(fsms[id].snapshot(), want)
		})
	}
}
