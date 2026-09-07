package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/server"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// nameList collects a repeatable string flag (e.g. --allow-client).
type nameList []string

func (n *nameList) String() string { return fmt.Sprint([]string(*n)) }
func (n *nameList) Set(v string) error {
	*n = append(*n, v)
	return nil
}

func main() {
	fs := flag.NewFlagSet("cacheyd", flag.ExitOnError)
	tlsCA := fs.String("tls-ca", "", "path to the PEM CA that signs server and client certificates (mTLS)")
	tlsCert := fs.String("tls-cert", "", "path to this server's PEM certificate (mTLS)")
	tlsKey := fs.String("tls-key", "", "path to this server's PEM private key (mTLS)")
	var allowClients nameList
	fs.Var(&allowClients, "allow-client", "client identity (the certificate's DNS SAN) permitted to connect; repeatable (mTLS)")
	insecure := fs.Bool("insecure-plaintext", false, "serve WITHOUT TLS — development only, never in production")

	consensus := fs.String("consensus", "", "cluster consensus engine: \"\" (standalone), \"raft\", or \"pbft\" (not implemented yet)")
	nodeID := fs.String("node-id", "", "this node's unique id in the cluster (raft cluster)")
	clientAddr := fs.String("client-addr", "", "client-facing NDJSON listen address, e.g. 127.0.0.1:8081 (raft cluster)")
	raftAddr := fs.String("raft-addr", "", "raft RPC listen address, e.g. 127.0.0.1:9101 (raft cluster)")
	dataDir := fs.String("data-dir", "", "data directory for the raft log and snapshots (raft cluster)")
	bootstrap := fs.Bool("bootstrap", false, "start a brand-new raft cluster as its first node (raft cluster)")
	join := fs.String("join", "", "client address of an existing member to join or re-announce to (raft cluster)")

	fs.Usage = func() { usage(fs) }
	fs.Parse(os.Args[1:])

	opts := mTLSOptions(*insecure, *tlsCA, *tlsCert, *tlsKey, allowClients)

	switch *consensus {
	case "":
		args := fs.Args()
		if len(args) < 1 || len(args) > 2 {
			fs.Usage()
			os.Exit(1)
		}
		if *nodeID != "" || *clientAddr != "" || *raftAddr != "" || *dataDir != "" || *bootstrap || *join != "" {
			fmt.Fprintln(os.Stderr, "cacheyd: cluster flags require -consensus raft")
			os.Exit(1)
		}
		dir := "data"
		if len(args) == 2 {
			dir = args[1]
		}
		runStandalone(args[0], dir, opts)
	case "raft":
		if len(fs.Args()) != 0 {
			fmt.Fprintln(os.Stderr, "cacheyd: -consensus raft takes no positional address; use -client-addr")
			os.Exit(1)
		}
		cf := &clusterFlags{
			nodeID:     *nodeID,
			clientAddr: *clientAddr,
			raftAddr:   *raftAddr,
			dataDir:    *dataDir,
			bootstrap:  *bootstrap,
			join:       *join,
		}
		if err := runRaftCluster(cf, opts); err != nil {
			fmt.Fprintln(os.Stderr, "cacheyd:", err)
			os.Exit(1)
		}
	case "pbft":
		fmt.Fprintln(os.Stderr, "cacheyd: -consensus pbft is not implemented yet; use -consensus raft")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "cacheyd: unknown -consensus %q (want \"\", raft or pbft)\n", *consensus)
		os.Exit(1)
	}
}

// usage prints the command's help text.
func usage(fs *flag.FlagSet) {
	fmt.Fprintf(fs.Output(), `Cachey server — a distributed key-value cache.

Usage:
  cacheyd <address> [data-dir] [flags]                        standalone single node
  cacheyd -consensus raft -node-id N -client-addr A \
          -raft-addr R -data-dir D (-bootstrap | -join ADDR)  replicated raft cluster

The first cluster node is started with -bootstrap; each later node joins by
pointing -join at any existing member's client address (ADDR above). Client
connections are mTLS by default; pass -insecure-plaintext for local
development without TLS.

Flags:
`)
	fs.PrintDefaults()
}

// runStandalone serves one independent cache node over addr with its WAL in
// dir (the historical cacheyd behavior).
func runStandalone(addr, dir string, opts []server.Option) {
	st := store.NewCacheyStore()

	cfg := wal.DefaultConfig(dir)
	w, err := wal.Open(cfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   st.ApplyRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		println("Error opening WAL:", err.Error())
		os.Exit(1)
	}
	defer w.Close()
	st.SetWAL(w)

	st.StartActiveExpiration(1 * time.Second)
	hdl := server.NewCacheyHandler(st)
	srv := server.NewServer(addr, hdl, opts...)

	if err := srv.Start(); err != nil {
		println("Error starting server:", err.Error())
		os.Exit(1)
	}

	mode := "mTLS"
	if len(opts) == 0 {
		mode = "plaintext (--insecure-plaintext)"
	}
	fmt.Printf("Server started on %s (%s) with WAL at %s\n", addr, mode, dir)
	select {}
}

// mTLSOptions resolves the TLS flags into server options. TLS is the default
// posture: a bare invocation with no TLS flags errors out rather than silently
// serving plaintext; serving without TLS requires the explicitly named
// --insecure-plaintext development flag, so plaintext can never be switched on
// by accident in production.
func mTLSOptions(insecure bool, caPath, certPath, keyPath string, allowClients []string) []server.Option {
	if insecure {
		if caPath != "" || certPath != "" || keyPath != "" || len(allowClients) > 0 {
			fmt.Fprintln(os.Stderr, "cacheyd: --insecure-plaintext cannot be combined with mTLS flags")
			os.Exit(1)
		}
		return nil
	}
	if caPath == "" || certPath == "" || keyPath == "" {
		fmt.Fprintln(os.Stderr, "cacheyd: mTLS requires --tls-ca, --tls-cert and --tls-key; pass --insecure-plaintext only for local development without TLS")
		os.Exit(1)
	}
	if len(allowClients) == 0 {
		fmt.Fprintln(os.Stderr, "cacheyd: mTLS requires at least one --allow-client; pass --insecure-plaintext only for local development without TLS")
		os.Exit(1)
	}
	allowed := make(map[string]bool, len(allowClients))
	for _, name := range allowClients {
		if !mtls.ValidName(name) {
			fmt.Fprintf(os.Stderr, "cacheyd: --allow-client %q is not a valid identity name\n", name)
			os.Exit(1)
		}
		allowed[name] = true
	}
	cfg, err := mtls.Server(
		mustReadFile(caPath),
		mustReadFile(certPath),
		mustReadFile(keyPath),
		func(identity string) bool { return allowed[identity] },
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cacheyd:", err)
		os.Exit(1)
	}
	return []server.Option{server.WithTLSConfig(cfg)}
}

func mustReadFile(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cacheyd:", err)
		os.Exit(1)
	}
	return b
}
