package hotstuff

import (
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
// Key distribution is a trusted bootstrap step (SetPeerKey), mirroring PBFT's
// dynamic key exchange whose first hop is trusted. In the in-memory milestone
// the test harness wires every peer's public key before any message flows; a
// TCP transport (a later milestone) will carry the exchange on connect. The
// upgrade path is pinning keys in the cluster config or running the transport
// over mTLS.

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

// SetPeerKey registers the identity public key of a peer (the trusted key
// bootstrap / dynamic key exchange of HS-M3). A replica rejects any signed
// message whose sender's key is not registered (mirrors pbft.Replica.SetPeerKey).
func (n *Replica) SetPeerKey(peer string, pub ed25519.PublicKey) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerKeys[peer] = pub
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
		return true // hard-coded genesis QC is the trusted root
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
