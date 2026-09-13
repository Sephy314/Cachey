package hotstuff

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/Sephy314/Cachey/internal/wal"
)

// This file implements the Phase 2.1 membership model: an epoch is a
// configuration generation, and every protocol decision in an epoch — who may
// vote, whose signature counts, what quorum is needed, who leads a view — is
// derived from that epoch's ValidatorSet, never from the replica's current
// membership.
//
// Epochs are chain-derived, not proposer-chosen: a block's epoch is the number
// of membership-transition blocks committed within its ancestry. A transition
// block T (epoch N) commits under the OLD set's consensus rules (T, B1, B2 are
// all epoch N, voted by set(N)); only once T is 3-chain committed does epoch
// N+1 activate, and the first new-epoch block B3 (epoch N+1, voted by
// set(N+1)) may justify itself with the old QC(B2). This is what makes the
// "new validator cannot vote before activation" and "removed validator cannot
// vote after removal" invariants hold by construction.

// ValidatorSet is the consensus configuration of one epoch: the sorted
// validator ids, their identity public keys, and the derived fault bound and
// quorum. Historical sets are preserved (n.sets), so a QC from any past epoch
// remains verifiable with the set that created it.
type ValidatorSet struct {
	Epoch      uint64
	Validators []string // sorted (deterministic ordering for leader selection)
	PublicKeys map[string]ed25519.PublicKey
	F          int    // floor((N-1)/3)
	Quorum     int    // 2F+1
	Hash       string // sha256 over sorted "id:pub" entries (set identifier)
}

// newValidatorSet builds a set from sorted ids and their keys, deriving the
// fault bound and quorum. N need not be exactly 3f+1 (N=5,6 are allowed); the
// quorum is always 2f+1 with f=floor((N-1)/3).
func newValidatorSet(epoch uint64, ids []string, keys map[string]ed25519.PublicKey) *ValidatorSet {
	f := (len(ids) - 1) / 3
	return &ValidatorSet{
		Epoch:      epoch,
		Validators: ids,
		PublicKeys: keys,
		F:          f,
		Quorum:     2*f + 1,
		Hash:       setHash(ids, keys),
	}
}

func setHash(ids []string, keys map[string]ed25519.PublicKey) string {
	h := sha256.New()
	for _, id := range ids {
		h.Write([]byte(id))
		h.Write([]byte{0})
		h.Write(keys[id])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// has reports whether id is a validator in this set.
func (s *ValidatorSet) has(id string) bool {
	i := sort.SearchStrings(s.Validators, id)
	return i < len(s.Validators) && s.Validators[i] == id
}

// apply derives the next epoch's ValidatorSet from this one plus a committed
// membership change: adds are appended with their public keys, removes are
// dropped. It returns nil for a malformed change (adding an existing member,
// removing a non-member, a bad key, an empty result) — such a transition can
// never activate.
func (s *ValidatorSet) apply(mc *MembershipChange) *ValidatorSet {
	members := make(map[string]bool, len(s.Validators)+len(mc.Add))
	for _, id := range s.Validators {
		members[id] = true
	}
	for _, id := range mc.Remove {
		if !members[id] {
			return nil
		}
		delete(members, id)
	}
	keys := make(map[string]ed25519.PublicKey, len(s.PublicKeys)+len(mc.Add))
	for id, pub := range s.PublicKeys {
		keys[id] = pub
	}
	for _, a := range mc.Add {
		if members[a.ID] || len(a.Pub) != ed25519.PublicKeySize {
			return nil
		}
		members[a.ID] = true
		keys[a.ID] = a.Pub
	}
	if len(members) == 0 {
		return nil
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return newValidatorSet(s.Epoch+1, ids, keys)
}

// MembershipChange is the protocol command carried by a membership-transition
// block. It is part of consensus history (the block's Cmd) but is never
// applied to the application FSM.
type MembershipChange struct {
	Add    []ValidatorEntry `json:"add,omitempty"`
	Remove []string         `json:"remove,omitempty"`
}

// ValidatorEntry is one validator to add: its id and identity public key.
type ValidatorEntry struct {
	ID  string `json:"id"`
	Pub []byte `json:"pub"`
}

// membershipCmd builds the block Cmd payload for a membership change.
func membershipCmd(mc *MembershipChange) ([]byte, error) {
	data, err := json.Marshal(mc)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wal.Record{Op: wal.OpMembership, Data: data})
}

// isMembershipBlock reports whether b carries a membership-change command.
func isMembershipBlock(b *Block) bool {
	if b == nil || len(b.Cmd) == 0 {
		return false
	}
	var rec wal.Record
	if err := json.Unmarshal(b.Cmd, &rec); err != nil {
		return false
	}
	return rec.Op == wal.OpMembership
}

// membershipOf parses b's membership-change command (nil if b is not a
// membership block or the payload is malformed).
func membershipOf(b *Block) *MembershipChange {
	if b == nil || len(b.Cmd) == 0 {
		return nil
	}
	var rec wal.Record
	if err := json.Unmarshal(b.Cmd, &rec); err != nil {
		return nil
	}
	if rec.Op != wal.OpMembership {
		return nil
	}
	var mc MembershipChange
	if err := json.Unmarshal(rec.Data, &mc); err != nil {
		return nil
	}
	return &mc
}

// epochOfLocked returns the epoch of block b, derived from b's ancestry: the
// number of membership-transition blocks committed within it. Must hold n.mu;
// all of b's ancestors must be in the tree.
func (n *Replica) epochOfLocked(b *Block) uint64 {
	epoch, _ := n.epochAndSetOfLocked(b)
	return epoch
}

// epochAndSetOfLocked walks b's ancestry, counting committed membership
// transitions and deriving the ValidatorSet for b's epoch. The walk starts
// from the GC base — the lowest retained block, whose epoch is stored in the
// block itself (its own ancestry was deleted by GC, so the base epoch cannot
// be re-derived from the tree). Transitions at or below the base are already
// reflected in the base epoch and in n.sets; only the ones above it are
// counted and applied. A transition block T is committed in b's ancestry when
// the 3-chain above it is present (T ← B1 ← B2 ← B3, each certifying its
// parent) — the same condition under which the engine commits T. Returns the
// derived epoch and set; set is nil when a transition's command is malformed
// (the block must then be rejected — a new epoch with an underivable
// configuration is not a valid claim). Must hold n.mu.
func (n *Replica) epochAndSetOfLocked(b *Block) (uint64, *ValidatorSet) {
	base := n.blocks[n.gcBase]
	if base == nil {
		return 0, n.sets[0] // defensive: no GC base (should not happen)
	}
	baseEpoch := base.Epoch
	baseSet := n.sets[baseEpoch]
	if b.Height <= base.Height {
		return baseEpoch, baseSet // at or below the GC base: already derived
	}
	var transitions []*Block
	var c1, c2, c3 *Block // the three blocks below the current one (descendants)
	for cur := b; cur != nil && cur.ID != n.gcBase; cur = n.blocks[cur.Parent] {
		if isMembershipBlock(cur) && c3 != nil &&
			oneChainedBy(cur, c1) && oneChainedBy(c1, c2) && oneChainedBy(c2, c3) {
			transitions = append(transitions, cur)
		}
		c3, c2, c1 = c2, c1, cur
	}
	set := baseSet
	for i := len(transitions) - 1; i >= 0; i-- { // oldest first
		mc := membershipOf(transitions[i])
		if mc == nil || set == nil {
			return baseEpoch + uint64(len(transitions)), nil
		}
		set = set.apply(mc)
		if set == nil {
			return baseEpoch + uint64(len(transitions)), nil
		}
	}
	return baseEpoch + uint64(len(transitions)), set
}

// leaderOfSet returns the leader of view in set: a deterministic round-robin
// over the set's sorted validators starting at the view-0 leader (cfg.Leader
// when it is a member, else the first validator). Every replica derives the
// same schedule from the same set — no map iteration, no arrival order.
func (n *Replica) leaderOfSet(set *ValidatorSet, view uint64) string {
	if set == nil || len(set.Validators) == 0 {
		return ""
	}
	idx := 0
	if i := sort.SearchStrings(set.Validators, n.leader0); i < len(set.Validators) && set.Validators[i] == n.leader0 {
		idx = i
	}
	return set.Validators[(idx+int(view))%len(set.Validators)]
}

// leaderOfEpoch returns the leader of view in the given epoch's validator set.
func (n *Replica) leaderOfEpoch(epoch, view uint64) string {
	return n.leaderOfSet(n.sets[epoch], view)
}

// verifyInSet reports whether sig is a valid signature by sender's key in set.
func (n *Replica) verifyInSet(set *ValidatorSet, sender string, sig []byte, m any) bool {
	if set == nil {
		return false
	}
	pub, ok := set.PublicKeys[sender]
	if !ok || len(pub) == 0 {
		return false
	}
	return verifyPayload(pub, sig, m)
}

// knownMemberLocked reports whether id is a validator in any known epoch's
// set. Used for transport-level checks (fetch/block replies) where the peer's
// consensus epoch is not the point — the block itself is validated separately.
// Must hold n.mu.
func (n *Replica) knownMemberLocked(id string) bool {
	for _, set := range n.sets {
		if set.has(id) {
			return true
		}
	}
	return false
}

// activateTransitionLocked activates the validator set of the epoch following
// b, where b is a committed membership-transition block: the new set is
// derived from the current epoch's set plus b's command and becomes the
// replica's current epoch. Must hold n.mu.
func (n *Replica) activateTransitionLocked(b *Block) {
	if !isMembershipBlock(b) {
		return
	}
	mc := membershipOf(b)
	old := n.sets[b.Epoch]
	if mc == nil || old == nil {
		return // defensive: malformed command or unknown base epoch
	}
	ns := old.apply(mc)
	if ns == nil {
		return // defensive: malformed transition — never activates
	}
	n.sets[ns.Epoch] = ns
	n.epoch = ns.Epoch
}
