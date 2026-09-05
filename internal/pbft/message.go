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
}

// Prepare certifies that the sender accepted the primary's proposal
// (PBFT §4.1 step 2). A replica is "prepared" once it holds the pre-prepare
// plus 2f matching prepares from other replicas.
type Prepare struct {
	View   uint64 `json:"view"`
	Seq    uint64 `json:"seq"`
	Digest string `json:"digest"`
	Sender string `json:"sender"`
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
}

// digestOf returns the canonical digest of a request: sha256 over its JSON
// encoding. Marshaling a struct is deterministic, so every replica computes
// the same digest for the same request.
func digestOf(req Request) string {
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
