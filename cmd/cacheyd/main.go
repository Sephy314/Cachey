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
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: cacheyd <address> [data-dir] [flags]\n")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	args := fs.Args()
	if len(args) < 1 {
		fs.Usage()
		os.Exit(1)
	}
	addr := args[0]
	dir := "data"
	if len(args) >= 2 {
		dir = args[1]
	}

	opts := mTLSOptions(*insecure, *tlsCA, *tlsCert, *tlsKey, allowClients)

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
	if *insecure {
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
