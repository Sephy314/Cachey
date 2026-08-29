package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// snapEntry is one key in a persisted snapshot.
type snapEntry struct {
	Val string `json:"val"`
	Exp int64  `json:"exp,omitempty"`
}

// snapshotData is the on-disk snapshot format.
type snapshotData struct {
	LastLogIndex uint64               `json:"last_log_index"`
	Data         map[string]snapEntry `json:"data"`
}

// Recover inspects the WAL directory and rebuilds store state by invoking the
// apply hooks. It returns the last durable log index (next index = last + 1).
//
// Supported crash states:
//   - bootstrap: no snapshot / WALs → empty store, last = 0
//   - case A: snapshot + wal + tmp → replay all, merge into one active WAL
//   - case B: snapshot + wal → replay; partial tails are truncated
//   - case C: only tmp → verified and restored as the active WAL
func Recover(dir string, hooks Hooks) (uint64, error) {
	activePath := filepath.Join(dir, activeWALName)
	tmpPath := filepath.Join(dir, tmpWALName)
	snapshotPath := filepath.Join(dir, snapshotName)
	snapshotTmpPath := filepath.Join(dir, snapshotTmpName)

	// An incomplete temporary snapshot is never trusted.
	if _, err := os.Stat(snapshotTmpPath); err == nil {
		if err := os.Remove(snapshotTmpPath); err != nil {
			return 0, err
		}
		_ = syncDir(dir)
	}

	last := uint64(0)
	if _, err := os.Stat(snapshotPath); err == nil {
		snap, err := readSnapshot(snapshotPath)
		if err != nil {
			return 0, fmt.Errorf("wal: corrupt snapshot: %w", err)
		}
		entries := make([]SnapshotEntry, 0, len(snap.Data))
		for k, e := range snap.Data {
			entries = append(entries, SnapshotEntry{Key: k, Val: e.Val, Exp: e.Exp})
		}
		if hooks.ApplySnapshot != nil {
			if err := hooks.ApplySnapshot(entries); err != nil {
				return 0, err
			}
		}
		last = snap.LastLogIndex
	}

	if _, err := os.Stat(activePath); err == nil {
		if err := replayWALFile(activePath, &last, hooks.ApplyRecord); err != nil {
			return 0, err
		}
	}

	tmpExisted := false
	if _, err := os.Stat(tmpPath); err == nil {
		tmpExisted = true
		if err := replayWALFile(tmpPath, &last, hooks.ApplyRecord); err != nil {
			return 0, err
		}
	}

	// Merge a leftover temporary WAL into the single active WAL.
	if tmpExisted {
		if err := rebuildActive(dir, activePath, tmpPath); err != nil {
			return 0, err
		}
	}
	return last, nil
}

func readSnapshot(path string) (*snapshotData, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s snapshotData
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if s.Data == nil {
		s.Data = map[string]snapEntry{}
	}
	return &s, nil
}

// replayWALFile applies every complete record in path, validating strict index
// continuity. An unterminated final chunk is handled like a partial write:
// truncated if it isn't valid JSON, otherwise applied and framed with a
// trailing newline. Invalid records elsewhere are corruption (fail-fast).
func replayWALFile(path string, last *uint64, apply func(Record) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	lines := bytes.Split(data, []byte{'\n'})
	var partial []byte
	if !bytes.HasSuffix(data, []byte{'\n'}) {
		partial = lines[len(lines)-1]
		lines = lines[:len(lines)-1]
	} else if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	for i, line := range lines {
		if len(line) == 0 {
			return fmt.Errorf("wal: empty record line %d in %s", i, path)
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("wal: corrupt record at line %d in %s: %w", i, path, err)
		}
		if err := validateRec(rec, *last, path); err != nil {
			return err
		}
		if rec.LogIndex == *last+1 {
			if apply != nil {
				if err := apply(rec); err != nil {
					return err
				}
			}
			*last = rec.LogIndex
		}
		// rec.LogIndex <= *last: already covered by the snapshot (a crash
		// during sealing leaves the sealed WAL on disk) — skip idempotently.
	}

	if len(partial) == 0 {
		return nil
	}
	var rec Record
	if err := json.Unmarshal(partial, &rec); err != nil {
		// Partial write → truncate to the last complete newline.
		cut := len(data) - len(partial)
		return os.Truncate(path, int64(cut))
	}
	// Valid JSON without a trailing newline: apply it and fix the framing so
	// later appends don't corrupt the file.
	if err := validateRec(rec, *last, path); err != nil {
		return err
	}
	if rec.LogIndex == *last+1 {
		if apply != nil {
			if err := apply(rec); err != nil {
				return err
			}
		}
		*last = rec.LogIndex
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte{'\n'}); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func validateRec(rec Record, last uint64, path string) error {
	switch rec.Op {
	case OpPut, OpDelete, OpTTL:
	default:
		return fmt.Errorf("wal: unknown op %q in %s", rec.Op, path)
	}
	if rec.LogIndex > last+1 {
		return fmt.Errorf("wal: index gap in %s: got %d, want <= %d", path, rec.LogIndex, last+1)
	}
	return nil
}

// rebuildActive merges the active and temporary WALs into a fresh active WAL
// via a temp file + fsync + rename, then removes the temporary WAL. Missing
// sources (e.g. a tmp-only crash state) are treated as empty.
func rebuildActive(dir, activePath, tmpPath string) error {
	rebuildPath := filepath.Join(dir, rebuildTmpName)
	out, err := os.OpenFile(rebuildPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, p := range []string{activePath, tmpPath} {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		in, err := os.Open(p)
		if err != nil {
			out.Close()
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			return err
		}
		in.Close()
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(rebuildPath, activePath); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	return syncDir(dir)
}
