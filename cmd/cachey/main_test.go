package main

import (
	"strings"
	"testing"

	"github.com/Sephy314/Cachey/internal/protocol"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		in   string
		want protocol.Command
	}{
		{"get foo", protocol.Command{Type: protocol.GET, Key: "foo"}},
		{"GET foo", protocol.Command{Type: protocol.GET, Key: "foo"}},
		{"put foo bar baz", protocol.Command{Type: protocol.PUT, Key: "foo", Val: "bar baz"}},
		{"put foo single", protocol.Command{Type: protocol.PUT, Key: "foo", Val: "single"}},
		{"del foo", protocol.Command{Type: protocol.DEL, Key: "foo"}},
		{"ttl foo 1500", protocol.Command{Type: protocol.TTL, Key: "foo", TTL: 1500}},
		{"alv", protocol.Command{Type: protocol.ALV}},
	}
	for _, tc := range tests {
		cmd, err := parseCommand(strings.Fields(tc.in))
		if err != nil {
			t.Errorf("parseCommand(%q) error = %v, want nil", tc.in, err)
			continue
		}
		if cmd != tc.want {
			t.Errorf("parseCommand(%q) = %+v, want %+v", tc.in, cmd, tc.want)
		}
	}
}

func TestParseCommandRejectsMalformed(t *testing.T) {
	for _, in := range []string{
		"",
		"get",
		"get a b",
		"put foo",
		"del",
		"del a b",
		"ttl foo",
		"ttl foo nope",
		"ttl",
		"alv extra",
		"bogus x",
	} {
		if _, err := parseCommand(strings.Fields(in)); err == nil {
			t.Errorf("parseCommand(%q) succeeded, want error", in)
		}
	}
}

func TestSplitArgs(t *testing.T) {
	tests := []struct {
		in   string
		addr string
		cmd  []string
	}{
		{"", ":8080", nil},
		{":8080", ":8080", nil},
		{":9090 get k", ":9090", []string{"get", "k"}},
		{"get k", ":8080", []string{"get", "k"}},
	}
	for _, tc := range tests {
		addr, cmd := splitArgs(strings.Fields(tc.in))
		if addr != tc.addr {
			t.Errorf("splitArgs(%q) addr = %q, want %q", tc.in, addr, tc.addr)
		}
		if len(cmd) != len(tc.cmd) {
			t.Errorf("splitArgs(%q) cmd = %v, want %v", tc.in, cmd, tc.cmd)
			continue
		}
		for i := range cmd {
			if cmd[i] != tc.cmd[i] {
				t.Errorf("splitArgs(%q) cmd = %v, want %v", tc.in, cmd, tc.cmd)
				break
			}
		}
	}
}
