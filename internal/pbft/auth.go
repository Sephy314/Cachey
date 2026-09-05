package pbft

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
)

// This file implements message authentication (M3). Every replica generates an
// Ed25519 identity keypair at construction and propagates its public key to
// peers; every PBFT wire message carries a signature over the message's
// canonical JSON (with the signature field itself excluded), so a replica can
// verify that a message really came from its claimed Sender.
//
// The key distribution is dynamic but its very first hop is trusted (TOFU):
// a replica accepts a peer's public key over the wire the first time it sees
// it. A man-in-the-middle active at that first exchange could substitute keys,
// which is out of scope for the milestone (the upgrade path is pinning keys in
// the cluster config or running the transport over TLS).

// signPayload signs m's canonical JSON (minus its "sig" field) with priv.
func signPayload(priv ed25519.PrivateKey, m any) []byte {
	b, err := canonicalSignable(m)
	if err != nil {
		return nil
	}
	return ed25519.Sign(priv, b)
}

// verifyPayload reports whether sig is a valid signature by pub over m's
// canonical JSON.
func verifyPayload(pub ed25519.PublicKey, sig []byte, m any) bool {
	b, err := canonicalSignable(m)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, b, sig)
}

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

// newKeyPair generates a fresh Ed25519 identity keypair.
func newKeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// digestBytes hashes arbitrary bytes (used to bind multi-message payloads
// like a NewView's replayed pre-prepares into one signature).
func digestBytes(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}
