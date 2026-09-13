package server

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls/testca"
)

// Identity & key-lifecycle suite for the durable HotStuff node: the Ed25519
// consensus identity is generated once, persisted, and never rotated — a
// changed public key would invalidate the signatures inside every past QC. The
// transport identity (mTLS certificate) is separate: same node, different key.

// TestHSIdentityCreatedAndPersisted: a first boot mints an Ed25519 keypair and
// writes it to hsidentity.json with owner-only permissions.
func TestHSIdentityCreatedAndPersisted(t *testing.T) {
	dir := t.TempDir()
	priv, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	if len(priv) != ed25519.PrivateKeySize {
		t.Fatalf("private key has %d bytes, want %d", len(priv), ed25519.PrivateKeySize)
	}
	path := filepath.Join(dir, hsIdentityFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("identity file not written: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("identity file mode = %o, want 600", perm)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f hsIdentityFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("identity file is not valid JSON: %v", err)
	}
	if f.ID != "n0" {
		t.Fatalf("identity file records id %q, want n0", f.ID)
	}
	if f.Priv != hex.EncodeToString(priv) {
		t.Fatal("persisted key differs from the returned key")
	}
}

// TestHSIdentityStableAcrossRestart: reopening the same directory yields the
// SAME public key — the property that keeps past QC signatures verifiable.
func TestHSIdentityStableAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	first, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	second, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	if !first.Public().(ed25519.PublicKey).Equal(second.Public().(ed25519.PublicKey)) {
		t.Fatal("public key changed across a restart")
	}

	// A full node restart keeps it too, and the identity file is not rewritten.
	before, err := os.ReadFile(filepath.Join(dir, hsIdentityFileName))
	if err != nil {
		t.Fatal(err)
	}
	n, err := OpenHotStuffNode(HotStuffNodeConfig{
		ID: "n0", Dir: dir, HSAddr: "127.0.0.1:0",
		ValidatorKeys: map[string]ed25519.PublicKey{"n0": first.Public().(ed25519.PublicKey)},
	})
	if err != nil {
		t.Fatalf("OpenHotStuffNode: %v", err)
	}
	defer n.Close()
	if !n.Node.PublicKey().Equal(first.Public().(ed25519.PublicKey)) {
		t.Fatal("node public key changed after reopening the data dir")
	}
	after, err := os.ReadFile(filepath.Join(dir, hsIdentityFileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("an existing identity file was rewritten on restart")
	}
}

// TestHSIdentityRejectsForeignDataDir: a data dir holding a DIFFERENT node's
// identity is refused, so pointing two nodes at one directory cannot silently
// share (or steal) an identity.
func TestHSIdentityRejectsForeignDataDir(t *testing.T) {
	dir := t.TempDir()
	if _, err := loadHSIdentity(dir, "n0"); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHSIdentity(dir, "n1"); err == nil {
		t.Fatal("a data dir holding another node's identity was accepted")
	}
}

// TestHSIdentityRejectsCorruptFile: a truncated, non-JSON, or wrong-length
// identity file fails loudly rather than silently minting a new key (which
// would rotate the node's public key and invalidate past QCs).
func TestHSIdentityRejectsCorruptFile(t *testing.T) {
	cases := map[string]string{
		"not-json":    "{not json",
		"short-key":   `{"id":"n0","priv":"deadbeef"}`,
		"not-hex":     `{"id":"n0","priv":"zzzz"}`,
		"empty-value": `{"id":"n0","priv":""}`,
	}
	for name, content := range cases {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, hsIdentityFileName), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadHSIdentity(dir, "n0"); err == nil {
			t.Fatalf("%s: corrupt identity file was accepted", name)
		}
	}
}

// TestHSIdentityDoesNotRegenerateInUsedDir: if the identity file disappears
// from a directory that already holds consensus state, key generation must be
// refused — a silent rotation is worse than a failed start.
func TestHSIdentityDoesNotRegenerateInUsedDir(t *testing.T) {
	dir := t.TempDir()
	if _, err := loadHSIdentity(dir, "n0"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wal.ndjson"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, hsIdentityFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHSIdentity(dir, "n0"); err == nil {
		t.Fatal("a missing identity file in a used data dir was silently regenerated")
	}
	// An empty directory still boots normally (a genuinely fresh node).
	empty := t.TempDir()
	if _, err := loadHSIdentity(empty, "n0"); err != nil {
		t.Fatalf("a fresh data dir must still generate an identity: %v", err)
	}
}

// TestHSIdentityUnreadableFileFails: an unreadable identity file is an error,
// never a new key. (Skipped when running as root, which bypasses file modes.)
func TestHSIdentityUnreadableFileFails(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("file permissions are not enforced for this user")
	}
	dir := t.TempDir()
	if _, err := loadHSIdentity(dir, "n0"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, hsIdentityFileName)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o600)
	if _, err := loadHSIdentity(dir, "n0"); err == nil {
		t.Fatal("an unreadable identity file was treated as absent")
	}
}

// TestHSValidatorKeysHoldPublicKeysOnly: the validator configuration carries
// public keys, and OpenHotStuffNode refuses a configuration whose entry for
// this node is not the persistent identity's public half — so a private key
// can never be smuggled in through the configuration map, and a mismatch (a
// rotated or copied key) is caught before the listener opens.
func TestHSValidatorKeysHoldPublicKeysOnly(t *testing.T) {
	dir := t.TempDir()
	priv, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	// A 32-byte value that is not this node's public key (e.g. someone else's
	// key, or a private-key half pasted in by mistake).
	wrong := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(wrong, priv[:ed25519.PublicKeySize])
	_, err = OpenHotStuffNode(HotStuffNodeConfig{
		ID: "n0", Dir: dir, HSAddr: "127.0.0.1:0",
		ValidatorKeys: map[string]ed25519.PublicKey{"n0": wrong},
	})
	if err == nil {
		t.Fatal("a validator key that is not this node's public key was accepted")
	}

	// The peer entry must be a well-formed public key as well.
	short := ed25519.PublicKey([]byte{1, 2, 3})
	_, err = OpenHotStuffNode(HotStuffNodeConfig{
		ID: "n0", Dir: dir, HSAddr: "127.0.0.1:0", Peers: []string{"n1"},
		ValidatorKeys: map[string]ed25519.PublicKey{
			"n0": priv.Public().(ed25519.PublicKey),
			"n1": short,
		},
	})
	if err == nil {
		t.Fatal("a malformed peer public key was accepted")
	}
}

// TestHotStuffPersistentClusterOverMTLS: a durable node cluster with mutual
// TLS on the peer transport converges, and each node's transport identity is
// its own mTLS certificate (separate from its Ed25519 consensus key).
func TestHotStuffPersistentClusterOverMTLS(t *testing.T) {
	base := t.TempDir()
	ids := []string{"p0", "p1", "p2", "p3"}
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	validatorKeys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		priv, err := loadHSIdentity(dir, id)
		if err != nil {
			t.Fatal(err)
		}
		validatorKeys[id] = priv.Public().(ed25519.PublicKey)
	}
	nodes := make(map[string]*HotStuffNode, len(ids))
	for _, id := range ids {
		cert, key, err := ca.Issue(id)
		if err != nil {
			t.Fatal(err)
		}
		n, err := OpenHotStuffNode(HotStuffNodeConfig{
			ID: id, Dir: filepath.Join(base, id), HSAddr: "127.0.0.1:0",
			Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: validatorKeys,
			TLSCA: ca.CertPEM(), TLSCert: cert, TLSKey: key,
		})
		if err != nil {
			t.Fatalf("OpenHotStuffNode(%s): %v", id, err)
		}
		nodes[id] = n
	}
	defer closePersistentCluster(nodes)
	addrs := make(map[string]string, len(ids))
	for id, n := range nodes {
		addrs[id] = n.HSAddr
	}
	for _, n := range nodes {
		n.Tr.SetPeers(addrs)
		n.CS.SetLeaderResolver(func(leader string) string { return "client://" + leader })
	}
	for _, n := range nodes {
		n.Tr.ConnectPeers(time.Now().Add(10 * time.Second))
	}
	if err := nodes[ids[0]].CS.Put("k", "v"); err != nil {
		t.Fatalf("Put over mTLS: %v", err)
	}
	allFSMHas(t, nodes, "k", "v")
}

// TestOpenHotStuffNodeRejectsPartialTLSConfig: the three TLS inputs are wired
// as a unit; a half-configured node (say, a key without a CA) must fail at
// startup rather than silently falling back to plaintext, which would look
// secure while not being so.
func TestOpenHotStuffNodeRejectsPartialTLSConfig(t *testing.T) {
	dir := t.TempDir()
	priv, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"n0": priv.Public().(ed25519.PublicKey)}
	for name, cfg := range map[string]HotStuffNodeConfig{
		"ca-only":   {TLSCA: []byte("x")},
		"cert-only": {TLSCert: []byte("x")},
		"key-only":  {TLSKey: []byte("x")},
	} {
		cfg.ID, cfg.Dir, cfg.HSAddr, cfg.ValidatorKeys = "n0", dir, "127.0.0.1:0", keys
		if _, err := OpenHotStuffNode(cfg); err == nil {
			t.Fatalf("%s: a partial TLS configuration was accepted", name)
		}
	}
}
