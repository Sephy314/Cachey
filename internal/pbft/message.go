package pbft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Request is a client request. Client+Timestamp together identify one unique
// request, so replicas can recognize duplicates and detect a primary that
// reuses a request under a different sequence number (PBFT §4.1). Command is
// the opaque mutation applied to the state machine when the request executes.
// In M1 the engine synthesizes Client/Timestamp per Submit; M5 will carry real
// client identities.
type Request struct {
	Client    string `json:"client"`
	Timestamp uint64 `json:"timestamp"`
	Command   []byte `json:"command,omitempty"`
}

// PrePrepare is the primary's ordering proposal (PBFT §4.1 step 1): it binds
// request Req to sequence number Seq in view View and ships the request itself
// so a backup can verify that Digest really is the request's digest.
type PrePrepare struct {
	View   uint64  `json:"view"`
	Seq    uint64  `json:"seq"`
	Digest string  `json:"digest"`
	Req    Request `json:"req"`
	Sender string  `json:"sender"`
	Sig    []byte  `json:"sig,omitempty"`
}

// Prepare certifies that the sender accepted the primary's proposal
// (PBFT §4.1 step 2). A replica is "prepared" once it holds the pre-prepare
// plus 2f matching prepares from other replicas.
type Prepare struct {
	View   uint64 `json:"view"`
	Seq    uint64 `json:"seq"`
	Digest string `json:"digest"`
	Sender string `json:"sender"`
	Sig    []byte `json:"sig,omitempty"`
}

// Commit certifies that the sender reached a prepared certificate for the
// proposal (PBFT §4.1 step 3). A replica holds a commit certificate — and may
// execute the request — once it has sent its own commit and collected 2f
// matching commits from other replicas (2f+1 matching commits in total).
type Commit struct {
	View   uint64 `json:"view"`
	Seq    uint64 `json:"seq"`
	Digest string `json:"digest"`
	Sender string `json:"sender"`
	Sig    []byte `json:"sig,omitempty"`
}

// PreparedCert is a verifiable prepared certificate for one request: the
// signed PRE-PREPARE that ordered it plus the signed PREPAREs the sender
// collected. A new primary validates the signatures and the 2f quorum before
// treating a request as genuinely prepared, so a Byzantine replica cannot
// conjure a prepared certificate for a request nobody actually prepared.
type PreparedCert struct {
	PrePrepare *PrePrepare `json:"pre_prepare"`
	Prepares   []*Prepare  `json:"prepares"`
}

// ViewEntry is one request a view-change sender still knows about (above its
// executed watermark). Cert is non-nil only when the sender holds a verifiable
// prepared certificate for the request in the view being left; entries without
// a certificate were merely accepted (their pre-prepare seen) but never
// prepared. The new primary replays certified requests ahead of merely-known
// ones, so a request some correct replica prepared can never be displaced by a
// request others merely observed or that a Byzantine claims.
type ViewEntry struct {
	Seq    uint64        `json:"seq"`
	Digest string        `json:"digest"`
	Req    Request       `json:"req"`
	Cert   *PreparedCert `json:"cert,omitempty"`
}

// ViewChange announces a replica's suspicion of the current primary and
// carries the state needed to reconstruct the next view (PBFT §4.4). View is
// the view being entered (current + 1); S is the sender's highest executed
// sequence number; Entries holds every request the sender knows above S.
type ViewChange struct {
	View    uint64      `json:"view"`
	S       uint64      `json:"s"`
	Entries []ViewEntry `json:"entries,omitempty"`
	Sender  string      `json:"sender"`
	Sig     []byte      `json:"sig,omitempty"`
}

// NewView is sent by the new primary once it holds 2f+1 view-change messages
// for the next view: it carries the collected view-changes (V) and the
// pre-prepares (O) that replay every request between the cluster's last
// executed sequence and the highest known one, so all replicas converge.
type NewView struct {
	View   uint64        `json:"view"`
	V      []*ViewChange `json:"v"`
	O      []PrePrepare  `json:"o"`
	Sender string        `json:"sender"`
	Sig    []byte        `json:"sig,omitempty"`
}

// digestOf returns the canonical digest of a request: sha256 over its JSON
// encoding. Marshaling a struct is deterministic, so every replica computes
// the same digest for the same request.
func digestOf(req Request) string {
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
