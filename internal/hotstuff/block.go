// Package hotstuff implements a Chained HotStuff BFT replica (Yin et al.,
// "HotStuff: BFT Consensus with Linearity and Responsiveness", PODC'19),
// following the paper's §5 (chained) chain rules and §6 event-driven core.
//
// This milestone (HS-M1) is deliberately the smallest safe slice: the block
// tree, quorum certificates and the 2-chain lock / 3-chain commit rule, driven
// by a single designated leader, entirely in memory and WITHOUT signatures
// (votes are just member ids). No view-change, no network transport, no WAL,
// no Ed25519 — those layers attach in later milestones so the consensus
// core is validated in isolation first.
package hotstuff

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// genesisID is the id of the single root block every replica starts from.
const genesisID = "genesis"

// Block is one node in the block tree. On the committed chain heights grow by
// one per proposal; Parent links a block to its tree parent; Justify is the QC
// this block carries, which certifies the block it extends (equal to Parent on
// the clean, gap-free chain of HS-M1). Cmd is the single client command the
// block proposes (nil for an empty block used to keep the chain advancing).
// View is the leader term (view number) in which the block was proposed — the
// only proposer for view v is leaderOf(v), so a receiver can recognise a
// stale leader (old view) and catch up to a newer one (higher view).
//
// Block identity is content-derived: two proposals with the same content are
// the same block, which is what lets replicas recognize equivocation (a
// leader proposing conflicting blocks at one height yields different ids).
type Block struct {
	View    uint64
	Height  uint64
	Parent  string // parent block id ("" only for genesis)
	ID      string // content digest; must match blockID(View, Height, Parent, Cmd)
	Cmd     []byte // nil for empty/genesis blocks
	Justify *QC    // QC certifying an ancestor (== Parent in HS-M1)
}

// blockID returns the deterministic identity of a block. It is a plain content
// hash — identity, not authentication; signatures arrive in HS-M3.
func blockID(view, h uint64, parent string, cmd []byte) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s:", view, h, parent)))
	sum = sha256.Sum256(append(sum[:], cmd...))
	return hex.EncodeToString(sum[:])
}

// QC is a quorum certificate: proof that 2f+1 distinct cluster members voted
// for the block with NodeID at Height. Since HS-M3 every member's partial vote
// is an Ed25519 signature over the vote tuple, stored here so a QC is
// self-contained and verifiable by any replica that knows the members' public
// keys. Votes maps voter id -> that voter's signature.
type QC struct {
	NodeID string
	Height uint64
	Votes  map[string][]byte // member id -> Ed25519 vote signature
}

func newQC(nodeID string, height uint64) *QC {
	return &QC{NodeID: nodeID, Height: height, Votes: make(map[string][]byte)}
}

// genesisBlock returns the root block and its pre-certified QC. Every member
// "votes" for genesis at bootstrap (the paper's hard-coded self-QC) so a
// leader may chain the first real block onto genesis immediately. The genesis
// QC is the trusted root and is special-cased as valid by qcValid, so its
// stored "votes" carry no signatures.
func genesisBlock(members []string) (*Block, *QC) {
	q := newQC(genesisID, 0)
	for _, m := range members {
		q.Votes[m] = nil
	}
	g := &Block{Height: 0, Parent: "", ID: genesisID, Justify: q}
	return g, q
}

// oneChainedBy reports whether child is a DIRECT child of parent that certifies
// parent (paper Appendix B notation: parent (⇐∧←) child): child.Parent is
// parent and child.Justify certifies parent. Lock and commit rules are
// anchored on chains of these direct one-chain steps.
func oneChainedBy(parent, child *Block) bool {
	return child != nil && child.Parent != "" &&
		child.Parent == parent.ID &&
		child.Justify != nil && child.Justify.NodeID == parent.ID
}
