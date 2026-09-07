package hotstuff

import "context"

// Proposal is a leader's broadcast of a new block — one of the only two
// message kinds in Chained HotStuff (the other is Vote). Replicas validate it
// against the safeNode predicate and, if they accept it, answer with a Vote.
// Since HS-M3 the proposal carries the proposer's Ed25519 signature over the
// whole block, so receivers can authenticate the leader and detect tampering.
type Proposal struct {
	Block Block  `json:"block"`
	From  string `json:"from"` // proposing leader id
	Sig   []byte `json:"sig,omitempty"`
}

// Vote is one replica's acceptance of a block: a claim "I accept the block
// NodeID at Height". Since HS-M3 Voter signs the vote, so a QC is a set of
// 2f+1 individually verifiable partial signatures. Votes are addressed to the
// leader that must form the next QC.
type Vote struct {
	Height uint64 `json:"height"`
	NodeID string `json:"node_id"`
	Voter  string `json:"voter"`
	Sig    []byte `json:"sig,omitempty"`
}

// ViewChange is sent by a replica that suspects its current leader (a timeout
// with no QC progress in its view). It asks to move to View and reports the
// highest QC the replica holds, so the new view's leader can pick a safe
// branch (the highest reported QC) to resume proposing from. It is unicast to
// leaderOf(View) (HS-M2; mirrors pbft's StartViewChange determinism) and is
// signed by From (HS-M3).
type ViewChange struct {
	View   uint64 `json:"view"`
	HighQC *QC    `json:"high_qc"`
	From   string `json:"from"`
	Sig    []byte `json:"sig,omitempty"`
}

// Fetch asks a peer to send the block with BlockID — used by a replica that
// fell behind (partitioned, or joining a view whose base it has not seen) to
// pull missing ancestors from the peer that holds them. To names the peer the
// request is addressed to (informational in the in-memory transport; the TCP
// transport will address by connection).
type Fetch struct {
	BlockID string
	To      string
	From    string
}

// BlockMsg carries a requested block to a replica that asked for it, signed by
// From (HS-M3) so the receiver can tell it came from the peer it trusts.
type BlockMsg struct {
	Block Block  `json:"block"`
	From  string `json:"from"`
	Sig   []byte `json:"sig,omitempty"`
}

// Transport delivers hotstuff messages between replicas. It is deliberately a
// tiny seam independent of the consensus core: a deterministic in-memory
// transport drives the unit tests, and a TCP transport (a later milestone)
// plugs in without touching consensus logic. Implementations must be safe for
// concurrent use.
type Transport interface {
	SendProposal(ctx context.Context, peer string, p *Proposal) error
	SendVote(ctx context.Context, peer string, v *Vote) error
	SendViewChange(ctx context.Context, peer string, vc *ViewChange) error
	SendFetch(ctx context.Context, peer string, f *Fetch) error
	SendBlock(ctx context.Context, peer string, b *BlockMsg) error
}
