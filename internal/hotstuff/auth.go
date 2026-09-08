package hotstuff

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
)

// This file implements message authentication (HS-M3), mirroring PBFT's M3:
// every replica generates an Ed25519 identity keypair at construction, and
// every protocol message it sends (proposal, vote, view change, block reply)
// carries a signature over the message's canonical JSON (with the signature
// field itself excluded), so a receiver can verify that a message really came
// from its claimed sender and was not tampered with.
//
// Quorum certificates carry the 2f+1 partial vote signatures themselves, so a
// QC is self-contained: any replica that knows the members' public keys can
// verify it (no threshold-signature scheme needed — signatures are stored
// individually, HS-later optimization).
//
// Key distribution is a trusted bootstrap step (SetPeerKey). A key is PINNED
// once and never silently replaced: a peer's claimed public key can only be
// registered if none is pinned yet (or the same key is offered again), so a
// later impostor claiming the same member id cannot take over. In-memory tests
// wire deterministic phantom keys; a persistent node reloads its own identity
// and its peers' pins from disk before any traffic (see server.OpenHotStuffNode
// and the HS-M3/HM5 notes). The production upgrade path is pinning keys in the
// cluster config or running the transport over mTLS.

// canonicalSignable returns a deterministic JSON encoding of m with the "sig"
// field removed, so a signature covers everything that matters without
// including itself. encoding/json sorts map keys, so re-marshaling the map is
// byte-for-byte stable across replicas.
func canonicalSignable(m any) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, err
	}
	delete(obj, "sig")
	return json.Marshal(obj)
}

// signPayload signs m's canonical JSON (minus its "sig" field) with priv.
func signPayload(priv ed25519.PrivateKey, m any) []byte {
	b, err := canonicalSignable(m)
	if err != nil {
		return nil
	}
	return ed25519.Sign(priv, b)
}

// verifyPayload reports whether sig is a valid signature by pub over m's
// canonical JSON (minus its "sig" field).
func verifyPayload(pub ed25519.PublicKey, sig []byte, m any) bool {
	b, err := canonicalSignable(m)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, b, sig)
}

// newKeyPair generates a fresh Ed25519 identity keypair.
func newKeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// PublicKey returns this replica's identity public key, for peers to register
// (mirrors pbft.Replica.PublicKey).
func (n *Replica) PublicKey() ed25519.PublicKey {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.pub
}

// SetPeerKey pins the identity public key of peer. Pinning is one-way: once a
// key is pinned for a member, a DIFFERENT key for that member is refused
// (returns false) — whoever pinned first cannot be impersonated by a later
// claim. Re-pinning the SAME key (a reconnect) is a no-op success. Unknown
// members may still register (the membership check lives in the transport /
// caller).
func (n *Replica) SetPeerKey(peer string, pub ed25519.PublicKey) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(pub) == 0 {
		return false
	}
	if old, ok := n.peerKeys[peer]; ok && len(old) > 0 && !bytes.Equal(old, pub) {
		return false // a different key is already pinned for this member
	}
	n.peerKeys[peer] = pub
	return true
}

// PeerKey returns the pinned identity public key of peer, if any.
func (n *Replica) PeerKey(peer string) (ed25519.PublicKey, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	pub, ok := n.peerKeys[peer]
	return pub, ok
}

// PeerKeys returns a snapshot of every pinned peer public key (for a persistent
// node to write its pins to disk so a restart re-pins before any traffic).
func (n *Replica) PeerKeys() map[string]ed25519.PublicKey {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]ed25519.PublicKey, len(n.peerKeys))
	for id, pub := range n.peerKeys {
		out[id] = pub
	}
	return out
}

// sign returns this replica's signature over m (see signPayload).
func (n *Replica) sign(m any) []byte {
	return signPayload(n.priv, m)
}

// verify reports whether sig is a valid signature by the recorded key of
// sender over m. Unknown senders fail (they were never given a key).
func (n *Replica) verify(sender string, sig []byte, m any) bool {
	pub, ok := n.peerKeys[sender]
	if !ok || len(pub) == 0 {
		return false
	}
	return verifyPayload(pub, sig, m)
}

// qcValid reports whether qc is a genuine quorum certificate: either the
// trusted genesis root (whose QC is hard-coded, per the paper) or at least
// 2f+1 distinct members whose stored vote signatures all verify. Must hold
// n.mu (reads peerKeys).
func (n *Replica) qcValid(qc *QC) bool {
	if qc == nil {
		return false
	}
	if qc.NodeID == genesisID {
		// Only the actual genesis root — the QC over genesis at height 0 — is
		// trusted. A forged "genesis QC" claiming a higher height (or any votes
		// that would certify something else) must not pass: an attacker could
		// otherwise bootstrap a block with an absurd justification height and
		// corrupt the replica's height bookkeeping.
		return qc.Height == 0
	}
	if len(qc.Votes) < 2*n.f+1 {
		return false
	}
	for voter, sig := range qc.Votes {
		if !n.members[voter] {
			return false // a non-validator can never contribute to a quorum
		}
		pub, ok := n.peerKeys[voter]
		if !ok || len(pub) == 0 {
			return false
		}
		// The vote each voter signed was Vote{Height, NodeID, Voter}; rebuild
		// it from the QC so the signature verifies against the same bytes.
		if !verifyPayload(pub, sig, Vote{Height: qc.Height, NodeID: qc.NodeID, Voter: voter}) {
			return false
		}
	}
	return true
}
