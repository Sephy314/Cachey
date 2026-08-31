package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Manager observes WAL growth and drives sealing, snapshots, and rotation in
// the background. It never mutates writer state directly; every control
// request goes through the shared WAL channel.
type Manager struct {
	dir        string
	ch         chan<- any
	writer     *Writer
	snapshotFn func() ([]SnapshotEntry, error)
	onFatal    func(error)
	interval   time.Duration
	threshold  int64
	ackTimeout time.Duration
	maxRetries int
	backoff    time.Duration
	stop       chan struct{}
	done       chan struct{}

	activePath      string
	tmpPath         string
	snapshotPath    string
	snapshotTmpPath string

	nextID   ControlID
	rotating bool

	// disableRotation suppresses the rotation manager (raft-log mode).
	disableRotation bool
}

func newManager(cfg Config, w *Writer, ch chan any, snapshotFn func() ([]SnapshotEntry, error)) *Manager {
	m := &Manager{
		dir:             cfg.Dir,
		ch:              ch,
		writer:          w,
		snapshotFn:      snapshotFn,
		onFatal:         func(err error) { log.Fatalf("wal: %v", err) },
		interval:        cfg.CheckInterval,
		threshold:       cfg.Threshold,
		ackTimeout:      cfg.AckTimeout,
		maxRetries:      cfg.MaxRetries,
		backoff:         250 * time.Millisecond,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		activePath:      filepath.Join(cfg.Dir, activeWALName),
		tmpPath:         filepath.Join(cfg.Dir, tmpWALName),
		snapshotPath:    filepath.Join(cfg.Dir, snapshotName),
		snapshotTmpPath: filepath.Join(cfg.Dir, snapshotTmpName),
	}
	if m.interval <= 0 {
		m.interval = 100 * time.Millisecond
	}
	if m.ackTimeout <= 0 {
		m.ackTimeout = 5 * time.Second
	}
	if m.maxRetries <= 0 {
		m.maxRetries = 5
	}
	if m.threshold <= 0 {
		m.threshold = 2000
	}
	m.disableRotation = cfg.DisableRotation
	return m
}

func (m *Manager) run() {
	defer close(m.done)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.tick()
		case <-m.stop:
			return
		}
	}
}

func (m *Manager) tick() {
	if m.disableRotation {
		return
	}
	if m.rotating || m.writer.MetaCount() < m.threshold {
		return
	}
	m.rotating = true
	m.rotateWithRetry()
	m.rotating = false
}

// rotateWithRetry runs one full rotation, retrying with exponential backoff.
// On permanent failure it calls onFatal (process exit in production).
func (m *Manager) rotateWithRetry() {
	backoff := m.backoff
	for attempt := 1; ; attempt++ {
		err := m.rotateOnce()
		if err == nil {
			return
		}
		if attempt >= m.maxRetries {
			m.onFatal(fmt.Errorf("wal: rotation failed after %d attempts: %w", attempt, err))
			return
		}
		time.Sleep(backoff)
		backoff *= 2
	}
}

func (m *Manager) rotateOnce() error {
	boundary, err := m.sendStartSealing()
	if err != nil {
		return fmt.Errorf("seal: %w", err)
	}
	if err := m.writeSnapshot(boundary); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if _, err := m.sendFinishRotation(); err != nil {
		return fmt.Errorf("finish: %w", err)
	}
	if err := m.rotateFiles(); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	if err := m.sendRotationComplete(); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	return nil
}

func (m *Manager) newID() ControlID {
	m.nextID++
	return m.nextID
}

func (m *Manager) sendStartSealing() (uint64, error) {
	return m.roundTripUint(m.newID(), func(id ControlID, ack chan uint64) any {
		return StartSealing{ID: id, AckCh: ack}
	})
}

func (m *Manager) sendFinishRotation() (uint64, error) {
	return m.roundTripUint(m.newID(), func(id ControlID, ack chan uint64) any {
		return FinishRotation{ID: id, AckCh: ack}
	})
}

func (m *Manager) sendRotationComplete() error {
	_, err := m.roundTripErr(m.newID(), func(id ControlID, ack chan error) any {
		return RotationComplete{ID: id, AckCh: ack}
	})
	return err
}

// roundTripUint sends a control message and waits for a uint64 ack, retrying
// the SAME control ID on timeout with a fresh ack channel (design §13.1). The
// writer answers idempotently from its per-ID cache.
func (m *Manager) roundTripUint(id ControlID, mk func(ControlID, chan uint64) any) (uint64, error) {
	for attempt := 0; attempt < m.maxRetries; attempt++ {
		ack := make(chan uint64, 1)
		ctx, cancel := context.WithTimeout(context.Background(), m.ackTimeout)
		select {
		case m.ch <- mk(id, ack):
		case <-ctx.Done():
			cancel()
			return 0, ctx.Err()
		}
		select {
		case v := <-ack:
			cancel()
			return v, nil
		case <-ctx.Done():
			cancel()
		}
	}
	return 0, fmt.Errorf("wal: control %d not acked after %d attempts", id, m.maxRetries)
}

func (m *Manager) roundTripErr(id ControlID, mk func(ControlID, chan error) any) (error, error) {
	for attempt := 0; attempt < m.maxRetries; attempt++ {
		ack := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), m.ackTimeout)
		select {
		case m.ch <- mk(id, ack):
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
		select {
		case v := <-ack:
			cancel()
			return v, nil
		case <-ctx.Done():
			cancel()
		}
	}
	return nil, fmt.Errorf("wal: control %d not acked after %d attempts", id, m.maxRetries)
}

// writeSnapshot atomically replaces the snapshot: temp file + fsync + rename
// + directory fsync.
func (m *Manager) writeSnapshot(boundary uint64) error {
	entries, err := m.snapshotFn()
	if err != nil {
		return err
	}
	data := make(map[string]snapEntry, len(entries))
	for _, e := range entries {
		data[e.Key] = snapEntry{Val: e.Val, Exp: e.Exp}
	}
	b, err := json.Marshal(snapshotData{LastLogIndex: boundary, Data: data})
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.OpenFile(m.snapshotTmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(m.snapshotTmpPath, m.snapshotPath); err != nil {
		return err
	}
	return syncDir(m.dir)
}

// rotateFiles atomically renames the temporary WAL into the active WAL. It is
// idempotent: if the temp file is already gone, the rotation already happened.
func (m *Manager) rotateFiles() error {
	if _, err := os.Stat(m.tmpPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if err := os.Rename(m.tmpPath, m.activePath); err != nil {
		return err
	}
	return syncDir(m.dir)
}

// syncDir fsyncs a directory so that renames/unlinks become durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
