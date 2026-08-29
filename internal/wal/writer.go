package wal

import (
	"os"
	"sync/atomic"
)

// WriterState is the state of the writer goroutine.
type WriterState int32

const (
	// StateIDLE is normal operation: appends go to the active WAL.
	StateIDLE WriterState = iota
	// StateSEALING means the active WAL is sealed; appends go to the temp WAL.
	StateSEALING
	// StateROTATING means the temp WAL is closed and mutations are held.
	StateROTATING
)

// controlResult is the idempotent outcome of a control message, keyed by ID.
type controlResult struct {
	boundary uint64
	lastTmp  uint64
	err      error
}

// Writer is the single goroutine that appends to WAL files. All file I/O and
// state transitions happen on its own goroutine, so the mutable fields below
// need no locking.
type Writer struct {
	dir        string
	ch         <-chan any
	stop       chan struct{}
	done       chan struct{}
	activePath string
	tmpPath    string
	tmpLimit   int64
	holdLimit  int

	lastIndex    uint64
	sealBoundary uint64
	active       *os.File
	tmp          *os.File
	tmpCount     int64
	state        WriterState
	hold         []Mutation
	controlRes   map[ControlID]controlResult

	// read by the manager / tests without touching the writer goroutine
	stateAtomic atomic.Int32
	countAtomic atomic.Int64
	indexAtomic atomic.Uint64
}

func (w *Writer) run() {
	defer close(w.done)
	defer w.closeFiles()
	for {
		select {
		case m := <-w.ch:
			if m == nil {
				return
			}
			w.handle(m)
		case <-w.stop:
			return
		}
	}
}

func (w *Writer) closeFiles() {
	if w.active != nil {
		w.active.Close()
	}
	if w.tmp != nil {
		w.tmp.Close()
	}
}

func (w *Writer) openActive() error {
	f, err := os.OpenFile(w.activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.active = f
	w.state = StateIDLE
	w.stateAtomic.Store(int32(StateIDLE))
	return nil
}

func (w *Writer) handle(m any) {
	switch v := m.(type) {
	case Mutation:
		w.handleMutation(v)
	case StartSealing:
		w.handleStartSealing(v)
	case FinishRotation:
		w.handleFinishRotation(v)
	case RotationComplete:
		w.handleRotationComplete(v)
	}
}

func (w *Writer) handleMutation(m Mutation) {
	switch w.state {
	case StateROTATING:
		w.holdMutation(m)
	case StateSEALING:
		if w.tmpCount >= w.tmpLimit {
			w.holdMutation(m)
			return
		}
		w.append(m, w.tmp)
	default: // StateIDLE
		w.append(m, w.active)
	}
}

// holdMutation defers a mutation until the writer returns to IDLE. If the
// bounded hold queue is full the mutation is failed with a transient error
// rather than blocking the single writer goroutine.
//
// ponytail: a full hold queue returns ErrBusy instead of applying true
// backpressure at the enqueue stage. The queue cap (HoldLimit) bounds memory;
// the ROTATING window it covers is a single rename, so overflow is not
// expected. Upgrade path: a dedicated bounded hold channel with a select on a
// rotation-complete signal to drain before sending.
func (w *Writer) holdMutation(m Mutation) {
	if len(w.hold) >= w.holdLimit {
		m.ResultCh <- ErrBusy
		return
	}
	w.hold = append(w.hold, m)
}

func (w *Writer) append(m Mutation, f *os.File) {
	if f == nil {
		m.ResultCh <- ErrBusy
		return
	}
	w.lastIndex++
	rec := m.Record
	rec.LogIndex = w.lastIndex
	line, err := rec.marshal()
	if err == nil {
		_, err = f.Write(line)
	}
	if err == nil {
		err = f.Sync()
	}
	w.indexAtomic.Store(w.lastIndex)
	if err != nil {
		m.ResultCh <- err
		return
	}
	if f == w.active {
		w.countAtomic.Add(1)
	} else {
		w.tmpCount++
	}
	m.ResultCh <- nil
}

func (w *Writer) handleStartSealing(ss StartSealing) {
	if res, ok := w.controlRes[ss.ID]; ok {
		ss.AckCh <- res.boundary
		return
	}
	if w.state == StateSEALING || w.state == StateROTATING {
		// Already sealed: ack the original boundary (idempotent).
		w.controlRes[ss.ID] = controlResult{boundary: w.sealBoundary}
		ss.AckCh <- w.sealBoundary
		return
	}
	// First sealing: open the temporary WAL first, then close the active one.
	// If opening tmp fails we stay IDLE and let the manager retry.
	tmp, err := os.OpenFile(w.tmpPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return // no ack → manager times out and retries
	}
	w.sealBoundary = w.lastIndex
	if w.active != nil {
		w.active.Sync()
		w.active.Close()
		w.active = nil
	}
	w.tmp = tmp
	w.tmpCount = 0
	w.state = StateSEALING
	w.stateAtomic.Store(int32(StateSEALING))
	w.controlRes[ss.ID] = controlResult{boundary: w.sealBoundary}
	ss.AckCh <- w.sealBoundary
}

func (w *Writer) handleFinishRotation(fr FinishRotation) {
	if res, ok := w.controlRes[fr.ID]; ok {
		fr.AckCh <- res.lastTmp
		return
	}
	if w.tmp != nil {
		w.tmp.Sync()
		w.tmp.Close()
		w.tmp = nil
	}
	w.state = StateROTATING
	w.stateAtomic.Store(int32(StateROTATING))
	w.controlRes[fr.ID] = controlResult{lastTmp: w.lastIndex}
	fr.AckCh <- w.lastIndex
}

func (w *Writer) handleRotationComplete(rc RotationComplete) {
	if res, ok := w.controlRes[rc.ID]; ok {
		rc.AckCh <- res.err
		return
	}
	active, err := os.OpenFile(w.activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return // no ack → manager times out and retries
	}
	w.active = active
	w.countAtomic.Store(0)
	w.state = StateIDLE
	w.stateAtomic.Store(int32(StateIDLE))
	// Drain the hold queue before consuming new channel mutations so held
	// mutations are never overtaken by newer ones (FIFO fairness).
	hold := w.hold
	w.hold = nil
	for _, m := range hold {
		w.append(m, w.active)
	}
	w.controlRes[rc.ID] = controlResult{}
	rc.AckCh <- nil
}

// MetaCount is the approximate number of records appended to the current
// active WAL since it was last opened.
func (w *Writer) MetaCount() int64 { return w.countAtomic.Load() }

// LastIndex is the last durable log index (for tests and observability).
func (w *Writer) LastIndex() uint64 { return w.indexAtomic.Load() }

// State exposes the writer's state (for tests).
func (w *Writer) State() WriterState { return WriterState(w.stateAtomic.Load()) }
