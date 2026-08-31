// Package wal implements Cachey's write-ahead log for crash-safe durability.
//
// A single WAL Writer goroutine serializes all appends (log_index order =
// append order = durability order), and a background WAL Manager seals,
// snapshots, and rotates WAL files. See README "Durability & WAL Persistence".
package wal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL file names.
const (
	activeWALName   = "wal.ndjson"
	tmpWALName      = "wal.tmp.ndjson"
	snapshotName    = "snapshot"
	snapshotTmpName = "snapshot.tmp"
	rebuildTmpName  = "wal.ndjson.rebuild.tmp"
)

// ErrBusy is returned when the writer cannot accept a mutation right now
// (hold queue / temporary WAL full). Callers should retry.
var ErrBusy = errors.New("wal: busy, retry later")

// Op identifies the type of a WAL record.
type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DEL"
	OpTTL    Op = "TTL"
	// OpNoop marks a Raft no-op entry (committed to advance the commit index);
	// it carries no state change.
	OpNoop Op = "NOOP"
)

// Record is a single logical WAL entry. LogIndex is assigned by the writer in
// append order and must be strictly increasing.
//
// Term and RaftIndex are set when the WAL is used as the persistence backend
// for the Raft replicated log: Term is the entry's Raft term and RaftIndex is
// its index in the Raft log (which may repeat after a Raft truncation). They
// are ignored by the store's FSM apply.
type Record struct {
	Op        Op     `json:"op"`
	Key       string `json:"key"`
	Val       string `json:"val,omitempty"`
	Exp       int64  `json:"exp,omitempty"` // absolute Unix-ms expiry (TTL op)
	LogIndex  uint64 `json:"log_index"`
	Term      uint64 `json:"term,omitempty"`       // raft term (raft log entries)
	RaftIndex uint64 `json:"raft_index,omitempty"` // raft log index (raft log entries)
}

// marshal serializes a record as one NDJSON line (including trailing newline).
func (r Record) marshal() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// SnapshotEntry is one key-value pair persisted in a snapshot.
type SnapshotEntry struct {
	Key string
	Val string
	Exp int64
}

// ControlID identifies a control message so retries are idempotent.
type ControlID uint64

// Mutation carries one record from the logger to the writer. ResultCh must be
// buffered (capacity 1) so the writer never blocks if the caller vanished.
type Mutation struct {
	Record   Record
	ResultCh chan error
}

// StartSealing asks the writer to close the active WAL at a boundary and switch
// new appends to the temporary WAL. AckCh receives the boundary index.
type StartSealing struct {
	ID    ControlID
	AckCh chan uint64
}

// FinishRotation asks the writer to stop appending to the temporary WAL.
// AckCh receives the last index written to the temporary WAL.
type FinishRotation struct {
	ID    ControlID
	AckCh chan uint64
}

// RotationComplete tells the writer the manager finished renaming the temporary
// WAL into place; the writer returns to IDLE and drains held mutations.
type RotationComplete struct {
	ID    ControlID
	AckCh chan error
}

// Hooks wires the WAL to an external store.
type Hooks struct {
	// ApplySnapshot loads a snapshot's entries into the store (recovery).
	ApplySnapshot func([]SnapshotEntry) error
	// ApplyRecord replays one WAL record into the store (recovery).
	ApplyRecord func(Record) error
	// Snapshot returns the store's current entries for snapshotting.
	Snapshot func() ([]SnapshotEntry, error)
}

// Config tunes the WAL.
type Config struct {
	Dir           string
	ChannelSize   int
	Threshold     int64 // active WAL records that trigger sealing
	TmpLimit      int64 // temporary WAL records before backpressure
	HoldLimit     int   // held mutations before transient errors
	CheckInterval time.Duration
	AckTimeout    time.Duration
	MaxRetries    int
	// DisableRotation turns off the background sealing/snapshot/rotation
	// manager. Used when the WAL backs the Raft log, where compaction must
	// wait until snapshot/InstallSnapshot support exists.
	DisableRotation bool
}

// DefaultConfig returns production defaults for a WAL rooted at dir.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:           dir,
		ChannelSize:   1024,
		Threshold:     2000,
		TmpLimit:      100000,
		HoldLimit:     4096,
		CheckInterval: 100 * time.Millisecond,
		AckTimeout:    5 * time.Second,
		MaxRetries:    5,
	}
}

// Logger enqueues records on the WAL channel.
type Logger struct {
	ch chan<- any
}

// Append writes rec to the WAL and waits for durability (append + fsync). It
// honours ctx cancellation both while enqueueing (backpressure) and while
// waiting for the result. On success the record is durable.
func (l Logger) Append(ctx context.Context, rec Record) error {
	res := make(chan error, 1)
	m := Mutation{Record: rec, ResultCh: res}
	select {
	case l.ch <- m:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WAL is a running write-ahead log: a writer goroutine (durability), a manager
// goroutine (sealing/snapshot/rotation), and the bounded channel between them.
type WAL struct {
	dir       string
	ch        chan any
	logger    Logger
	writer    *Writer
	manager   *Manager
	closeOnce sync.Once
}

// Open runs recovery, starts the writer and manager goroutines, and returns a
// live WAL. The apply hooks are invoked during recovery only.
func Open(cfg Config, hooks Hooks) (*WAL, error) {
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 1024
	}
	if cfg.TmpLimit <= 0 {
		cfg.TmpLimit = 100000
	}
	if cfg.HoldLimit <= 0 {
		cfg.HoldLimit = 4096
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	last, err := Recover(cfg.Dir, hooks)
	if err != nil {
		return nil, err
	}

	ch := make(chan any, cfg.ChannelSize)
	w := &Writer{
		dir:        cfg.Dir,
		ch:         ch,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		activePath: filepath.Join(cfg.Dir, activeWALName),
		tmpPath:    filepath.Join(cfg.Dir, tmpWALName),
		lastIndex:  last,
		tmpLimit:   cfg.TmpLimit,
		holdLimit:  cfg.HoldLimit,
		controlRes: make(map[ControlID]controlResult),
	}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	go w.run()

	m := newManager(cfg, w, ch, hooks.Snapshot)
	go m.run()

	return &WAL{
		dir:     cfg.Dir,
		ch:      ch,
		logger:  Logger{ch: ch},
		writer:  w,
		manager: m,
	}, nil
}

// Append writes rec to the WAL, waiting for durability.
func (w *WAL) Append(ctx context.Context, rec Record) error {
	return w.logger.Append(ctx, rec)
}

// MetaCount is an approximate count of records in the current active WAL.
func (w *WAL) MetaCount() int64 { return w.writer.MetaCount() }

// Close stops the manager and writer goroutines and closes open files. It is
// idempotent: repeated calls are no-ops.
func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.manager.stop)
		<-w.manager.done
		close(w.writer.stop)
		<-w.writer.done
	})
	return err
}
