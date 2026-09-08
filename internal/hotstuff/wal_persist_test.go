package hotstuff

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/wal"
)

// HS-M4 crash-restart tests (mirroring pbft/wal_persist_test.go): a replica's
// accepted blocks, QCs, executed watermark and vote height are durably
// written, and a restart from the same WAL directory rebuilds the tree and
// watermark so a command executed before the crash is NOT re-executed
// (idempotent recovery) while the replica can keep making progress.

// openWALReplica boots a WAL-backed replica at dir with recovery wired in.
// Peers is nil for a single-node cluster (f=0). apply records executed
// commands; pass the SAME recorder across a restart to catch re-execution.
func openWALReplica(t *testing.T, dir, id, leader string, peers []string, apply func(Block)) (*Replica, *wal.WAL, *recorderTransport) {
	t.Helper()
	cfg := wal.DefaultConfig(dir)
	cfg.DisableRotation = true // engine-level log; compaction is a post-M5 concern
	tr := &recorderTransport{}
	r, err := NewReplica(Config{ID: id, Leader: leader, Peers: peers}, tr, apply)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(cfg, wal.Hooks{ApplyRecord: r.ApplyRecoveredRecord})
	if err != nil {
		t.Fatal(err)
	}
	r.SetLogStore(NewWALLogStore(w))
	r.FinishRecovery()
	return r, w, tr
}

// commitCmd drives a single-node leader (f=0) to propose cmd and flush two
// empty blocks, so the 3-chain above cmd commits it.
func commitCmd(t *testing.T, r *Replica, cmd string) {
	t.Helper()
	for i := 0; i < 3; i++ {
		var c []byte
		if i == 0 {
			c = []byte(cmd)
		}
		if _, err := r.Propose(c); err != nil {
			t.Fatalf("propose(%q, flush %d): %v", cmd, i, err)
		}
	}
}

// TestWALPersistenceRestart commits commands on a WAL-backed single node,
// restarts it from the same directory, and verifies the executed watermark
// prevents re-execution while the replica keeps committing (HS-M4).
func TestWALPersistenceRestart(t *testing.T) {
	dir := t.TempDir()
	applied := &orderLog{}

	// Life 1: commit "a" and "b".
	r, w, _ := openWALReplica(t, dir, "solo", "", nil, func(b Block) {
		if len(b.Cmd) > 0 {
			applied.add(string(b.Cmd))
		}
	})
	commitCmd(t, r, "a")
	commitCmd(t, r, "b")
	if got := fmt.Sprint(applied.snapshot()); got != "[a b]" {
		t.Fatalf("before crash applied %v, want [a b]", got)
	}
	w.Close()

	// Life 2: recover from the WAL. The watermark must prevent re-running "a"/"b".
	r2, w2, _ := openWALReplica(t, dir, "solo", "", nil, func(b Block) {
		if len(b.Cmd) > 0 {
			applied.add(string(b.Cmd))
		}
	})
	defer w2.Close()
	if got := fmt.Sprint(applied.snapshot()); got != "[a b]" {
		t.Fatalf("recovery re-executed commands: applied %v, want [a b]", got)
	}

	// The recovered tree/watermark let it keep committing.
	commitCmd(t, r2, "c")
	if got := fmt.Sprint(applied.snapshot()); got != "[a b c]" {
		t.Fatalf("after restart applied %v, want [a b c]", got)
	}
}

// TestWALRecoveryRebuildsTree: blocks accepted but not yet committed before a
// crash survive in the tree, and a restarted node resumes from the highest
// recovered QC to commit them.
func TestWALRecoveryRebuildsTree(t *testing.T) {
	dir := t.TempDir()

	r, w, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	idA, err := r.Propose([]byte("a")) // B1
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Propose([]byte("b")); err != nil { // B2; nothing committed yet
		t.Fatal(err)
	}
	w.Close()

	// Restart with no new commands: "a" must NOT have been executed, and
	// WaitCommitted for it must still block (it is known but uncommitted).
	r2, w2, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	defer w2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r2.WaitCommitted(ctx, idA); err != context.DeadlineExceeded {
		t.Fatalf("recovered-but-uncommitted block must not resolve as committed, got %v", err)
	}

	// Two more blocks complete the 3-chain above B1; "a" commits on the restarted
	// node — proving the accepted B1/B2 and their QCs were rebuilt.
	commitCmd(t, r2, "flush")
	ctx2, cancel2 := context.WithTimeout(context.Background(), testTimeout)
	defer cancel2()
	if err := r2.WaitCommitted(ctx2, idA); err != nil {
		t.Fatalf("recovered block did not commit after restart: %v", err)
	}
}

// TestWALVoteHeightSurvivesRestart: a follower that voted up to height h must
// not vote again at or below h after a crash+restart (the vote-once /
// QC-uniqueness invariant is durable). It resumes voting above the guard.
func TestWALVoteHeightSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Life 1: a follower (phantom leader L0 + peers P1,P2) votes for heights 1..3.
	r, w, _ := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	wirePhantom(r, testLeaderID, testPeer1, testPeer2)
	ids := mainChain(t, r, []string{"a", "b", "c"}) // votes at heights 1,2,3
	_ = ids
	r.mu.Lock()
	voted := r.vHeight
	r.mu.Unlock()
	if voted != 3 {
		t.Fatalf("follower voted up to height %d, want 3", voted)
	}
	w.Close()

	// Life 2: the restored vote height guards against re-voting.
	r2, w2, tr2 := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	defer w2.Close()
	wirePhantom(r2, testLeaderID, testPeer1, testPeer2)
	r2.mu.Lock()
	restored := r2.vHeight
	r2.mu.Unlock()
	if restored != 3 {
		t.Fatalf("restored vote height = %d, want 3", restored)
	}

	// Re-feeding proposals at heights <= 3 (same content -> same ids, already in
	// the tree) is deduplicated and earns no new vote; a strictly higher block
	// does, proving the restored guard did not freeze voting forever.
	p4 := prop(4, ids[3], "d", quorumQC(ids[3], 3))
	r2.HandleProposal(p4)
	if got := tr2.voteCount(p4.Block.ID); got != 1 {
		t.Fatalf("recovered follower must vote for a height-4 block, got %d votes", got)
	}
	r2.mu.Lock()
	after := r2.vHeight
	r2.mu.Unlock()
	if after != 4 {
		t.Fatalf("vote height should advance to 4, got %d", after)
	}
}

// TestWALRecoveryIgnoresForeignOps verifies the recovery hook is safe on a WAL
// shared with other subsystems: non-HotStuff records (and unknown kinds) are
// ignored, not replayed as blocks.
func TestWALRecoveryIgnoresForeignOps(t *testing.T) {
	dir := t.TempDir()
	r, w, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	defer w.Close()
	if err := r.ApplyRecoveredRecord(wal.Record{Op: wal.OpPut, Key: "k", Val: "v"}); err != nil {
		t.Fatalf("foreign record should be ignored, got error: %v", err)
	}
	if err := r.ApplyRecoveredRecord(wal.Record{Op: wal.OpHotStuff, Data: []byte(`{"kind":"nonsense"}`)}); err != nil {
		t.Fatalf("unknown kind should be ignored, got error: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.blocks) != 1 { // genesis only
		t.Fatalf("foreign records rebuilt %d blocks, want 1", len(r.blocks))
	}
	if len(r.recoverQCs) != 0 {
		t.Fatalf("foreign records queued %d QCs, want 0", len(r.recoverQCs))
	}
}
