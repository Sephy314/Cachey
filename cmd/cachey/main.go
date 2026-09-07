// Command cachey is the Cachey command-line client: a TCP client for a
// cacheyd server (see cmd/cacheyd). It talks NDJSON over the same protocol as
// pkg/client and reuses that client plus internal/mtls, so it inherits the
// server's TLS posture: connections are mTLS by default, and plaintext
// requires the explicit -insecure-plaintext development flag.
//
// With no command arguments it runs an interactive shell; otherwise it runs a
// single command and exits:
//
//	cachey [flags] [address] [command [args...]]
//
//	address defaults to :8080 and must contain ':' when given explicitly.
//	Commands: get KEY, put KEY VALUE..., del KEY, ttl KEY MILLIS, alv.
package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/pkg/client"
)

const (
	defaultAddr  = ":8080"
	maxRedirects = 8 // bound on cluster leader-redirect hops
)

func main() {
	fs := flag.NewFlagSet("cachey", flag.ExitOnError)
	tlsCA := fs.String("tls-ca", "", "path to the PEM CA that signs the server certificate (mTLS)")
	tlsCert := fs.String("tls-cert", "", "path to this client's PEM certificate (mTLS)")
	tlsKey := fs.String("tls-key", "", "path to this client's PEM private key (mTLS)")
	serverName := fs.String("server-name", "", "expected server identity, its certificate's DNS SAN (mTLS)")
	insecure := fs.Bool("insecure-plaintext", false, "connect WITHOUT TLS — development only, never in production")
	fs.Usage = func() { usage(fs) }
	fs.Parse(os.Args[1:])

	cfg, err := clientTLS(*tlsCA, *tlsCert, *tlsKey, *serverName)
	if err != nil {
		fatal(err)
	}
	if *insecure && cfg != nil {
		fatal(errors.New("-insecure-plaintext cannot be combined with mTLS flags"))
	}
	if cfg == nil && !*insecure {
		fatal(errors.New("connection requires mTLS flags (-tls-ca, -tls-cert, -tls-key, -server-name) or -insecure-plaintext (development only)"))
	}

	addr, cmdWords := splitArgs(fs.Args())
	sess := &session{addr: addr, cfg: cfg}

	if len(cmdWords) == 0 {
		if err := repl(sess, os.Stdin); err != nil {
			fatal(err)
		}
		return
	}
	cmd, err := parseCommand(cmdWords)
	if err != nil {
		fatal(err)
	}
	if err := runOnce(sess, cmd); err != nil {
		fatal(err)
	}
}

// usage prints the command's help text.
func usage(fs *flag.FlagSet) {
	fmt.Fprintf(fs.Output(), `Cachey client — talk to a cacheyd server over NDJSON.

Usage:
  cachey [flags] [address] [command [args...]]

  address defaults to %s and must contain ':' when given explicitly.
  With no command, cachey starts an interactive shell (type 'help' inside).

Commands:
  get KEY          fetch the value of KEY
  put KEY VALUE... store VALUE (tokens joined with spaces) under KEY
  del KEY          remove KEY
  ttl KEY MILLIS   expire KEY after MILLIS milliseconds
  alv              print the server's liveness status

Examples:
  cachey -insecure-plaintext                 # shell against local :8080
  cachey -insecure-plaintext put user alice
  cachey -insecure-plaintext get user
  cachey -tls-ca ca.pem -tls-cert c.pem -tls-key c.key \
         -server-name cachey.local :8443 get user

Flags:
`, defaultAddr)
	fs.PrintDefaults()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "cachey:", err)
	os.Exit(1)
}

// clientTLS resolves the mTLS flags into a *tls.Config for dialing. All flags
// empty means plaintext (nil config); any flag set requires all of them.
func clientTLS(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	if caPath == "" && certPath == "" && keyPath == "" && serverName == "" {
		return nil, nil
	}
	if caPath == "" || certPath == "" || keyPath == "" || serverName == "" {
		return nil, errors.New("mTLS requires all of -tls-ca, -tls-cert, -tls-key and -server-name")
	}
	return mtls.Client(readFile(caPath), readFile(certPath), readFile(keyPath), serverName)
}

func readFile(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		fatal(err)
	}
	return b
}

// splitArgs picks the address out of the trailing args: the first arg counts
// as the address iff it contains ':', otherwise the default address is used
// and every arg is a command word.
func splitArgs(args []string) (addr string, cmd []string) {
	if len(args) > 0 && strings.Contains(args[0], ":") {
		return args[0], args[1:]
	}
	return defaultAddr, args
}

// parseCommand turns command words (as split from a shell line or an argv
// tail) into a protocol.Command. For PUT the value is everything after the
// key, joined with single spaces.
func parseCommand(words []string) (protocol.Command, error) {
	if len(words) == 0 {
		return protocol.Command{}, errors.New("missing command (try get, put, del, ttl, alv)")
	}
	usageFor := func(usage string) error {
		return fmt.Errorf("%s: usage: %s", words[0], usage)
	}
	switch strings.ToUpper(words[0]) {
	case string(protocol.GET):
		if len(words) != 2 {
			return protocol.Command{}, usageFor("get KEY")
		}
		return protocol.Command{Type: protocol.GET, Key: words[1]}, nil
	case string(protocol.PUT):
		if len(words) < 3 {
			return protocol.Command{}, usageFor("put KEY VALUE...")
		}
		return protocol.Command{Type: protocol.PUT, Key: words[1], Val: strings.Join(words[2:], " ")}, nil
	case string(protocol.DEL):
		if len(words) != 2 {
			return protocol.Command{}, usageFor("del KEY")
		}
		return protocol.Command{Type: protocol.DEL, Key: words[1]}, nil
	case string(protocol.TTL):
		if len(words) != 3 {
			return protocol.Command{}, usageFor("ttl KEY MILLIS")
		}
		ms, err := strconv.ParseInt(words[2], 10, 64)
		if err != nil {
			return protocol.Command{}, fmt.Errorf("ttl: invalid millis %q", words[2])
		}
		return protocol.Command{Type: protocol.TTL, Key: words[1], TTL: ms}, nil
	case string(protocol.ALV):
		if len(words) != 1 {
			return protocol.Command{}, usageFor("alv")
		}
		return protocol.Command{Type: protocol.ALV}, nil
	default:
		return protocol.Command{}, fmt.Errorf("unknown command %q", words[0])
	}
}

// session is a client connection to one Cachey server that re-dials after
// connection failures and follows "not leader: <addr>" redirects so the
// caller always talks to the current cluster leader.
type session struct {
	addr string
	cfg  *tls.Config // nil for plaintext
	cl   *client.Client
}

func (s *session) dial() error {
	if s.cl != nil {
		return nil
	}
	var opts []client.Option
	if s.cfg != nil {
		opts = append(opts, client.WithTLSConfig(s.cfg))
	}
	cl, err := client.NewClient(s.addr, opts...)
	if err != nil {
		return err
	}
	s.cl = cl
	return nil
}

func (s *session) drop() {
	if s.cl != nil {
		s.cl.Close()
		s.cl = nil
	}
}

// do sends cmd and returns the raw response, transparently reconnecting after
// connection failures and following leader-redirect hints.
func (s *session) do(cmd protocol.Command) (*string, error) {
	for i := 0; i < maxRedirects; i++ {
		if err := s.dial(); err != nil {
			return nil, err
		}
		resp, err := s.cl.SendCommand(cmd)
		if err == nil {
			return resp, nil
		}
		if next, ok := client.RedirectLeader(err); ok {
			s.drop()
			s.addr = next
			continue
		}
		var st *protocol.Status
		if !errors.As(err, &st) {
			// A network-level failure, not a server verdict: drop the stale
			// connection so the next command re-dials.
			s.drop()
		}
		return nil, err
	}
	return nil, errors.New("too many leader redirects")
}

// runOnce executes a single command and prints its result.
func runOnce(sess *session, cmd protocol.Command) error {
	resp, err := sess.do(cmd)
	if err != nil {
		return err
	}
	return printResult(cmd, *resp)
}

// printResult renders the server's echoed command. GET and ALV carry their
// answer in Val; the mutating commands are acknowledged with OK.
func printResult(cmd protocol.Command, raw string) error {
	var echo protocol.Command
	if err := json.Unmarshal([]byte(raw), &echo); err != nil {
		// Not a command echo; show the raw response rather than guess.
		fmt.Println(raw)
		return nil
	}
	switch cmd.Type {
	case protocol.GET, protocol.ALV:
		fmt.Println(echo.Val)
	default:
		fmt.Println("OK")
	}
	return nil
}

const commandHelp = `Commands:
  get KEY          fetch the value of KEY
  put KEY VALUE... store VALUE (tokens joined with spaces) under KEY
  del KEY          remove KEY
  ttl KEY MILLIS   expire KEY after MILLIS milliseconds
  alv              print the server's liveness status
  help             show this help
  exit, quit       leave the shell
`

func repl(sess *session, in io.Reader) error {
	sc := bufio.NewScanner(in)
	fmt.Printf("Cachey client at %s. Type 'help' for commands, 'exit' to quit.\n", sess.addr)
	for {
		fmt.Printf("%s> ", sess.addr)
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		words := strings.Fields(line)
		switch strings.ToLower(words[0]) {
		case "exit", "quit":
			return nil
		case "help", "?":
			fmt.Print(commandHelp)
			continue
		}
		cmd, err := parseCommand(words)
		if err != nil {
			fmt.Println("(error)", err)
			continue
		}
		if err := runOnce(sess, cmd); err != nil {
			fmt.Println("(error)", err)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}
