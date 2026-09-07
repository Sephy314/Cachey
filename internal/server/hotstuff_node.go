package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Sephy314/Cachey/internal/hotstuff"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// This file implements the durable, fully-wired HotStuff node
// (HotStuffClusterStore + persistent engine + store FSM) — the HotStuff
// counterpart of RaftNode. One WAL in the node's data directory carries BOTH
// the store FSM's mutations (OpPut/OpDelete/OpTTL, appended when the engine
// applies a committed command) and the engine's consensus records
// (OpHotStuff: accepted blocks/QCs/watermark/vote height). Recovery replays
// the WAL into a fresh store and engine in order — the store rebuilds its
// data from its own records, the engine rebuilds its tree and marks the
// executed prefix applied without re-running applyFn — so a restart restores
// the data exactly once (no loss, no re-execution).
//
// The node's identity is also durable: its Ed25519 private key and its peers'
// pinned public keys live on disk, so a restarted node keeps the same public
// key (signatures in past QCs still verify) and re-pins its peers before any
// traffic (an impostor's Hello can never overwrite a pinned key).

// HotStuffNode is a fully wired, persistent HotStuff node exposing its
// replicated store (HotStuffClusterStore) for client traffic.
type HotStuffNode struct {
	ID     string
	Dir    string
	Store  *store.CacheyStore
	Node   *hotstuff.Replica
	Tr     *hotstuff.TCPTransport
	WAL    *wal.WAL
	CS     *HotStuffClusterStore
	HSAddr string // bound hotstuff RPC listen address (host:port)
}

// HotStuffNodeConfig configures OpenHotStuffNode.
type HotStuffNodeConfig struct {
	ID     string   // this node's id (unique in the cluster)
	Dir    string   // data directory (identity, peer pins, shared WAL)
	HSAddr string   // hotstuff RPC listen address, e.g. "127.0.0.1:9201"
	Peers  []string // peer node ids (fixed membership; no dynamic join)
	Leader string   // view-0 leader id (defaults to ID)
	// ValidatorKeys is the fixed validator identity configuration. It must
	// contain cfg.ID and every entry in Peers; Hello only proves possession of
	// these configured keys and never establishes trust on first connection.
	ValidatorKeys map[string]ed25519.PublicKey
}

// OpenHotStuffNode opens (or recovers) a persistent HotStuff node and returns
// it with its transport already listening on cfg.HSAddr.
func OpenHotStuffNode(cfg HotStuffNodeConfig) (*HotStuffNode, error) {
	if cfg.ID == "" || cfg.Dir == "" || cfg.HSAddr == "" {
		return nil, fmt.Errorf("hotstuff node: id, dir and HSAddr are required")
	}
	if cfg.Leader == "" {
		cfg.Leader = cfg.ID
	}
	// 1. Durable identity: load (or create) this node's Ed25519 keypair and any
	//    previously pinned peer keys, BEFORE any network traffic.
	priv, err := loadHSIdentity(cfg.Dir, cfg.ID)
	if err != nil {
		return nil, err
	}
	if err := validateHSValidatorKeys(cfg, priv); err != nil {
		return nil, err
	}
	st := store.NewCacheyStore()
	tr := hotstuff.NewTCPTransport(nil)
	// The shared WAL is opened below; the apply hook only runs live (after a
	// commit), by which point it is set. It appends each executed mutation to
	// the shared WAL as its own store record so a restart rebuilds the FSM from
	// those records (the engine never re-runs apply below its watermark).
	var sharedWAL *wal.WAL
	node, err := hotstuff.NewReplica(hotstuff.Config{
		ID: cfg.ID, Peers: cfg.Peers, Leader: cfg.Leader, PrivateKey: priv,
	}, tr, newHSStoreApply(&sharedWAL, st))
	if err != nil {
		return nil, err
	}
	if err := repinHSPeers(cfg.Dir, node); err != nil {
		return nil, err
	}
	tr.SetNode(node)
	if err := tr.SetValidatorKeys(cfg.ValidatorKeys); err != nil {
		return nil, err
	}

	// 2. Shared WAL recovery: store snapshot first, then every record — store
	//    mutations into the FSM, engine records into the engine.
	wcfg := wal.DefaultConfig(cfg.Dir)
	wcfg.DisableRotation = true // no engine snapshot yet; rotation would truncate engine records
	w, err := wal.Open(wcfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord: func(rec wal.Record) error {
			if rec.Op == wal.OpHotStuff {
				return node.ApplyRecoveredRecord(rec)
			}
			return st.ApplyRecord(rec)
		},
		Snapshot: st.Snapshot,
	})
	if err != nil {
		return nil, err
	}
	sharedWAL = w
	node.SetLogStore(hotstuff.NewWALLogStore(w))
	node.FinishRecovery()

	// Do not make the node reachable until both its FSM and consensus state are
	// fully recovered and the durable apply hook is wired. Receiving a proposal
	// before this point could otherwise commit against partial state.
	bound, err := tr.Listen(cfg.HSAddr)
	if err != nil {
		w.Close()
		return nil, err
	}
	tr.RegisterPeer(cfg.ID, bound) // advertise our own RPC address
	return &HotStuffNode{
		ID:     cfg.ID,
		Dir:    cfg.Dir,
		Store:  st,
		Node:   node,
		Tr:     tr,
		WAL:    w,
		CS:     NewHotStuffClusterStore(node, st),
		HSAddr: bound,
	}, nil
}

func validateHSValidatorKeys(cfg HotStuffNodeConfig, priv ed25519.PrivateKey) error {
	if len(cfg.ValidatorKeys) != len(cfg.Peers)+1 {
		return fmt.Errorf("hotstuff node %s: validator key configuration must contain every member", cfg.ID)
	}
	self, ok := cfg.ValidatorKeys[cfg.ID]
	if !ok || !bytes.Equal(self, priv.Public().(ed25519.PublicKey)) {
		return fmt.Errorf("hotstuff node %s: configured key does not match persistent identity", cfg.ID)
	}
	for _, peer := range cfg.Peers {
		if pub, ok := cfg.ValidatorKeys[peer]; !ok || len(pub) != ed25519.PublicKeySize {
			return fmt.Errorf("hotstuff node %s: missing validator key for peer %q", cfg.ID, peer)
		}
	}
	return nil
}

// Close shuts the node's transport down. The WAL is flushed by its owner
// (call w.Close() explicitly before reopening the dir — tests and the server
// do this; see wal_persist_test / raftnode usage).
func (n *HotStuffNode) Close() { n.Tr.Close() }

// newHSStoreApply builds the durable apply hook for a persistent node. Unlike
// the in-memory NewHotStuffApply, it durably appends each executed mutation to
// the shared WAL as its own store record (OpPut/OpDelete/OpTTL) before applying
// it to memory. A restart then rebuilds the FSM by replaying those records —
// the engine marks its watermark applied and never re-runs apply, so the data
// survives exactly once (HS-M5).
func newHSStoreApply(wp **wal.WAL, fsm *store.CacheyStore) func(hotstuff.Block) {
	return func(b hotstuff.Block) {
		if len(b.Cmd) == 0 {
			return // an empty flush block
		}
		w := *wp // set once the shared WAL is open; apply only runs live
		if w == nil {
			log.Printf("hotstuff apply: no shared WAL wired")
			return
		}
		var rec wal.Record
		if err := json.Unmarshal(b.Cmd, &rec); err != nil {
			log.Printf("hotstuff apply: bad command: %v", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), hsProposeTimeout)
		defer cancel()
		if err := w.Append(ctx, rec); err != nil {
			log.Printf("hotstuff apply: durable write: %v", err)
			return
		}
		if err := fsm.ApplyRecord(rec); err != nil {
			log.Printf("hotstuff apply: %v", err)
		}
	}
}

// ---- durable identity & peer pins ----

const (
	hsIdentityFileName = "hsidentity.json"
	hsPeersFileName    = "hspeers.json"
)

// hsIdentityFile is the persisted node identity: its id and Ed25519 private
// key (hex). A stable key across restarts is what keeps signatures in past QCs
// verifiable (HS-M3).
type hsIdentityFile struct {
	ID   string `json:"id"`
	Priv string `json:"priv"` // hex ed25519 private key
}

// loadHSIdentity loads the node's private key from dir, generating and
// persisting a fresh one on first boot. It refuses a file that belongs to a
// different node id (misconfigured data dir).
func loadHSIdentity(dir, id string) (ed25519.PrivateKey, error) {
	path := filepath.Join(dir, hsIdentityFileName)
	b, err := os.ReadFile(path)
	if err == nil {
		var f hsIdentityFile
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("hotstuff node %s: corrupt identity file: %w", id, err)
		}
		if f.ID != id {
			return nil, fmt.Errorf("hotstuff node %s: data dir holds identity for %q", id, f.ID)
		}
		priv, err := hex.DecodeString(f.Priv)
		if err != nil || len(priv) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("hotstuff node %s: corrupt private key", id)
		}
		return ed25519.PrivateKey(priv), nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(hsIdentityFile{ID: id, Priv: hex.EncodeToString(priv)})
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}

// repinHSPeers re-pins the peer public keys persisted from a previous run so a
// restarted node does not re-TOFU peers it already trusts.
func repinHSPeers(dir string, node *hotstuff.Replica) error {
	b, err := os.ReadFile(filepath.Join(dir, hsPeersFileName))
	if os.IsNotExist(err) {
		return nil // first boot: no pins yet, ConnectPeers establishes them
	}
	if err != nil {
		return err
	}
	var pins map[string]string
	if err := json.Unmarshal(b, &pins); err != nil {
		return err
	}
	for id, pubHex := range pins {
		pub, err := hex.DecodeString(pubHex)
		if err != nil {
			return err
		}
		node.SetPeerKey(id, ed25519.PublicKey(pub))
	}
	return nil
}

// SavePeerPins persists the node's currently pinned peer public keys. Call
// after the transport's ConnectPeers key exchange (and after any membership
// change), so a restart re-pins the same keys before any traffic.
func (n *HotStuffNode) SavePeerPins() error {
	pins := make(map[string]string, 8)
	for id, pub := range n.Node.PeerKeys() {
		pins[id] = hex.EncodeToString(pub)
	}
	out, err := json.Marshal(pins)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(n.Dir, hsPeersFileName), out, 0o600)
}
