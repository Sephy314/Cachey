package pbft

import (
	"context"
	"sort"
)

// This file implements the view-change protocol (PBFT §4.4): when a backup
// suspects the primary it multicasts a VIEW-CHANGE carrying its state above
// its executed watermark; once 2f+1 distinct replicas have moved, the next
// view's primary assembles a NEW-VIEW whose pre-prepares replay every request
// between the cluster's last executed sequence and its highest known one, and
// all replicas converge on the new view.
//
// ponytail: PBFT anchors view changes on stable checkpoints (a 2f+1 proof that
// every correct replica executed through some sequence) and uses state
// transfer to catch replicas up. This engine has neither (deferred), so a view
// change uses floor = max(executed watermark over the collected view-changes):
// that is safe as long as no correct replica permanently missed committed
// requests while connected. A correct replica that was partitioned long enough
// to skip committed requests cannot catch up without state transfer.

// StartViewChange suspects the current primary and initiates a view change to
// view+1. It is normally called by the suspicion timer; tests call it directly
// for determinism.
func (n *Replica) StartViewChange() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.startViewChangeLocked()
}

// startViewChangeLocked multicasts a VIEW-CHANGE for view+1 carrying this
// replica's state above its executed watermark, and, if this replica is the
// next view's primary, starts assembling the NEW-VIEW. Must hold n.mu.
func (n *Replica) startViewChangeLocked() {
	target := n.view + 1
	if n.vcSent[target] {
		return
	}
	n.vcSent[target] = true
	vc := n.buildViewChangeLocked(target)
	vc.Sig = n.sign(vc)
	n.addViewChangeLocked(vc) // count ourselves
	n.broadcast(func(p string) {
		_ = n.tr.SendViewChange(context.Background(), p, vc)
	})
	n.maybeBuildNewViewLocked(target)
}

// buildViewChangeLocked snapshots every known request above the executed
// watermark into a VIEW-CHANGE for target. Entries this replica holds a
// prepared certificate for carry it as verifiable evidence; the rest are
// merely-known (their pre-prepare was accepted but never prepared). Must hold
// n.mu.
func (n *Replica) buildViewChangeLocked(target uint64) *ViewChange {
	vc := &ViewChange{View: target, S: n.nextExec - 1, Sender: n.id}
	for seq, e := range n.log {
		if seq <= n.nextExec-1 {
			continue // executed; the new primary does not need them
		}
		en := ViewEntry{Seq: seq, Digest: e.digest, Req: e.req}
		if e.sentCommit { // reached prepared (2f prepares) in this view
			en.Cert = n.preparedCertLocked(e)
		}
		vc.Entries = append(vc.Entries, en)
	}
	sort.Slice(vc.Entries, func(i, j int) bool { return vc.Entries[i].Seq < vc.Entries[j].Seq })
	return vc
}

// preparedCertLocked assembles the verifiable prepared certificate for a
// prepared entry: the signed pre-prepare that ordered it plus every retained
// signed matching prepare. Must hold n.mu.
func (n *Replica) preparedCertLocked(e *entry) *PreparedCert {
	cert := &PreparedCert{PrePrepare: e.pp}
	for _, pm := range e.prepares {
		cert.Prepares = append(cert.Prepares, pm)
	}
	sort.Slice(cert.Prepares, func(i, j int) bool {
		return cert.Prepares[i].Sender < cert.Prepares[j].Sender
	})
	return cert
}

// addViewChangeLocked records a VIEW-CHANGE for a target view. Must hold n.mu.
func (n *Replica) addViewChangeLocked(vc *ViewChange) {
	set := n.viewChanges[vc.View]
	if set == nil {
		set = make(map[string]*ViewChange)
		n.viewChanges[vc.View] = set
	}
	set[vc.Sender] = vc
}

// HandleViewChange records a peer's VIEW-CHANGE for the next view. When 2f+1
// distinct replicas (including this one if it suspects) have moved and this
// replica is the next view's primary, it assembles and multicasts the
// NEW-VIEW; other replicas keep collecting until the NEW-VIEW arrives.
func (n *Replica) HandleViewChange(m *ViewChange) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.verify(m.Sender, m.Sig, m) {
		return // unauthenticated
	}
	if !n.isPeer(m.Sender) {
		return
	}
	if m.View <= n.view || m.View != n.view+1 {
		return // stale, or a replica jumping more than one view ahead
	}
	if !n.validViewChange(m) {
		return
	}
	n.addViewChangeLocked(m)
	n.maybeBuildNewViewLocked(m.View)
}

// validViewChange reports whether every carried entry matches its digest and
// every claimed prepared certificate is genuinely valid. A Byzantine replica
// can assert anything in its own VIEW-CHANGE, so prepared-ness must be proven:
// a non-nil Cert is only trusted after validCert verifies its signatures and
// quorum. Must hold n.mu.
func (n *Replica) validViewChange(vc *ViewChange) bool {
	for _, en := range vc.Entries {
		if en.Seq == 0 || digestOf(en.Req) != en.Digest {
			return false
		}
		if en.Cert != nil && !n.validCert(vc, en) {
			return false
		}
	}
	return true
}

// validCert verifies a claimed prepared certificate: the pre-prepare must be
// authentic, issued by the primary of its view, for this exact sequence and
// request in the view being left, and at least 2f distinct replicas must have
// sent authentic matching prepares. Without those, a Byzantine replica cannot
// prove a request was prepared, so its claim is rejected. Must hold n.mu.
func (n *Replica) validCert(vc *ViewChange, en ViewEntry) bool {
	pp := en.Cert.PrePrepare
	if pp == nil || pp.Seq != en.Seq || pp.Digest != en.Digest || pp.View+1 != vc.View {
		return false
	}
	// The pre-prepare must come from the primary of its own view and be
	// authentic (a non-primary cannot order requests).
	if !n.members[pp.Sender] || pp.Sender != primaryID(n.all, pp.View) {
		return false
	}
	if !verifyPayload(n.peerKeys[pp.Sender], pp.Sig, pp) {
		return false
	}
	// At least 2f distinct authentic prepares must match the pre-prepare. PBFT
	// counts prepares from distinct BACKUPS only: the view's primary already
	// voted with its pre-prepare, and the certificate holder's own vote is the
	// certificate itself, so neither may pad the quorum.
	prim := primaryID(n.all, pp.View)
	seen := make(map[string]bool, len(en.Cert.Prepares))
	for _, p := range en.Cert.Prepares {
		if p == nil || p.View != pp.View || p.Seq != pp.Seq || p.Digest != pp.Digest {
			return false
		}
		if p.Sender == prim || p.Sender == vc.Sender {
			return false
		}
		if !n.members[p.Sender] || seen[p.Sender] {
			return false
		}
		if !verifyPayload(n.peerKeys[p.Sender], p.Sig, p) {
			return false
		}
		seen[p.Sender] = true
	}
	return len(seen) >= 2*n.f
}

// maybeBuildNewViewLocked makes the next view's primary emit a NEW-VIEW once
// it has collected 2f+1 distinct VIEW-CHANGE messages for target. Must hold
// n.mu.
func (n *Replica) maybeBuildNewViewLocked(target uint64) {
	if n.nvSent[target] || primaryID(n.all, target) != n.id {
		return
	}
	if len(n.viewChanges[target]) < 2*n.f+1 {
		return
	}
	n.nvSent[target] = true
	o := n.buildOEntriesLocked(target)
	set := n.viewChanges[target]
	V := make([]*ViewChange, 0, len(set))
	for _, vc := range set {
		V = append(V, vc)
	}
	nv := &NewView{View: target, V: V, O: o, Sender: n.id}
	nv.Sig = n.sign(nv)
	n.broadcast(func(p string) {
		_ = n.tr.SendNewView(context.Background(), p, nv)
	})
	n.enterViewLocked(target, o)
}

// buildOEntriesLocked derives the NEW-VIEW pre-prepares: every request with a
// sequence number in (floor, maxSeq], where floor is the highest executed
// watermark among the collected view-changes and maxSeq the highest known
// sequence. Nothing below floor is replayed because every view-changing
// correct replica already executed it.
//
// Per sequence, a request backed by a verifiable prepared certificate always
// wins over a merely-known request, no matter how many replicas merely report
// the latter (or claim a certificate they cannot prove); among requests with
// the same status the most-reported wins, with the smallest digest as a
// deterministic tie-break. Every collected VIEW-CHANGE was validated on receipt
// (validViewChange), so a non-nil Cert here is a genuine prepared certificate.
// This preserves the view-change safety invariant: a request any correct
// replica prepared — hence any request that could have committed — is never
// displaced in the new view. Must hold n.mu.
func (n *Replica) buildOEntriesLocked(target uint64) []PrePrepare {
	set := n.viewChanges[target]
	var floor, maxSeq uint64
	// seen[seq][digest] -> request; preparedFor[seq][digest] -> backed by a
	// valid certificate; reports[seq][digest] -> number of reporters.
	seen := make(map[uint64]map[string]*Request)
	preparedFor := make(map[uint64]map[string]bool)
	reports := make(map[uint64]map[string]int)
	for _, vc := range set {
		if vc.S > floor {
			floor = vc.S
		}
		for _, en := range vc.Entries {
			if en.Seq > maxSeq {
				maxSeq = en.Seq
			}
		}
	}
	for _, vc := range set {
		for _, en := range vc.Entries {
			if en.Seq <= floor {
				continue
			}
			if seen[en.Seq] == nil {
				seen[en.Seq] = make(map[string]*Request)
				preparedFor[en.Seq] = make(map[string]bool)
				reports[en.Seq] = make(map[string]int)
			}
			if _, ok := seen[en.Seq][en.Digest]; !ok {
				req := en.Req
				seen[en.Seq][en.Digest] = &req
			}
			preparedFor[en.Seq][en.Digest] = preparedFor[en.Seq][en.Digest] || en.Cert != nil
			reports[en.Seq][en.Digest]++
		}
	}
	var o []PrePrepare
	for seq := floor + 1; seq <= maxSeq; seq++ {
		if len(seen[seq]) == 0 {
			continue
		}
		// A certified request outranks every merely-known one; within the same
		// rank, the most-reported request wins (smallest digest tie-break).
		bestD := ""
		bestRank, bestN := -1, -1
		for d := range seen[seq] {
			rank := 0
			if preparedFor[seq][d] {
				rank = 1
			}
			n := reports[seq][d]
			if rank > bestRank || (rank == bestRank && (n > bestN || (n == bestN && (bestD == "" || d < bestD)))) {
				bestD, bestRank, bestN = d, rank, n
			}
		}
		o = append(o, PrePrepare{
			View:   target,
			Seq:    seq,
			Digest: bestD,
			Req:    *seen[seq][bestD],
			Sender: n.id,
		})
	}
	// Each O pre-prepare is signed by the new primary so it can later serve as
	// verifiable certificate evidence in the next view change.
	for i := range o {
		o[i].Sig = n.sign(&o[i])
	}
	return o
}

// HandleNewView validates and enters the new view proposed by its primary. The
// message must carry 2f+1 distinct valid VIEW-CHANGEs for the next view and
// pre-prepares consistent with them; then the replica adopts every pre-prepare
// and runs the normal-case protocol in the new view.
func (n *Replica) HandleNewView(nv *NewView) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.verify(nv.Sender, nv.Sig, nv) {
		return // unauthenticated
	}
	if !n.isPeer(nv.Sender) || nv.Sender != primaryID(n.all, nv.View) {
		return // only the view's primary sends NEW-VIEW
	}
	if nv.View <= n.view || nv.View != n.view+1 {
		return
	}
	// V holds 2f+1 view-changes from distinct replicas and legitimately
	// includes this replica's own, so membership (not isPeer) is the check.
	senders := make(map[string]bool, len(nv.V))
	for _, vc := range nv.V {
		if vc == nil || vc.View != nv.View || !n.members[vc.Sender] || senders[vc.Sender] {
			return
		}
		if !n.validViewChange(vc) {
			return
		}
		if !n.verify(vc.Sender, vc.Sig, vc) {
			return // every collected view-change must be authentic
		}
		senders[vc.Sender] = true
	}
	if len(senders) < 2*n.f+1 {
		return
	}
	for _, pp := range nv.O {
		if pp.View != nv.View || digestOf(pp.Req) != pp.Digest {
			return
		}
		if !viewChangeCovers(nv.V, pp) {
			return // every replayed request must come from a view-change
		}
	}
	// A NEW-VIEW must not displace a prepared request: every sequence in the
	// replay range that some collected view-change certifies must appear in O
	// with exactly that request, and O must not propose a conflicting one.
	// This guards against a Byzantine new primary that ignores certificates.
	need := make(map[uint64]string) // seq -> required digest
	for _, vc := range nv.V {
		for _, en := range vc.Entries {
			if en.Cert == nil {
				continue
			}
			if d, ok := need[en.Seq]; ok && d != en.Digest {
				return // two conflicting prepared certificates: invalid NEW-VIEW
			}
			need[en.Seq] = en.Digest
		}
	}
	for _, pp := range nv.O {
		if d, ok := need[pp.Seq]; ok && d != pp.Digest {
			return // O displaces a certified request
		}
		delete(need, pp.Seq)
	}
	if len(need) != 0 {
		return // a certified request was omitted from O
	}
	for _, vc := range nv.V {
		n.addViewChangeLocked(vc)
	}
	n.vcSent[nv.View] = true
	n.enterViewLocked(nv.View, nv.O)
}

// viewChangeCovers reports whether some view-change in V carried a request for
// pp.Seq with pp's digest.
func viewChangeCovers(V []*ViewChange, pp PrePrepare) bool {
	for _, vc := range V {
		for _, en := range vc.Entries {
			if en.Seq == pp.Seq && en.Digest == pp.Digest {
				return true
			}
		}
	}
	return false
}

// enterViewLocked switches this replica to target and adopts every NEW-VIEW
// pre-prepare, restarting the 3-phase protocol in the new view for requests
// that never committed. Must hold n.mu.
func (n *Replica) enterViewLocked(target uint64, o []PrePrepare) {
	n.view = target
	// Pending prepares/commits from superseded views are useless; keep those
	// for the new view (peers may have sent them before we entered) and the
	// current one.
	for k := range n.pendingPrepares {
		if k.view < target {
			delete(n.pendingPrepares, k)
		}
	}
	for k := range n.pendingCommits {
		if k.view < target {
			delete(n.pendingCommits, k)
		}
	}
	// A new primary must never reuse a sequence number from an earlier view,
	// so it continues from the highest sequence number it knows about.
	last := uint64(0)
	for seq := range n.log {
		if seq > last {
			last = seq
		}
	}
	for _, pp := range o {
		if pp.Seq > last {
			last = pp.Seq
		}
		n.adoptNewViewPrePrepareLocked(pp)
	}
	if last > n.lastAssigned {
		n.lastAssigned = last
	}
	n.executeReady()
}

// adoptNewViewPrePrepareLocked creates (or restarts) the entry for a NEW-VIEW
// pre-prepare and multicasts a matching Prepare. Already-executed sequence
// numbers and requests already executed under a different sequence number are
// left alone. Must hold n.mu.
func (n *Replica) adoptNewViewPrePrepareLocked(pp PrePrepare) {
	seq := pp.Seq
	if seq < n.nextExec {
		return // already executed (defensive; floor prevents this normally)
	}
	d := digestOf(pp.Req)
	if d != pp.Digest {
		return
	}
	if s, ok := n.executedAt[reqKey{pp.Req.Client, pp.Req.Timestamp}]; ok && s != seq {
		return // an executed request cannot be re-ordered to a new sequence
	}
	if old := n.log[seq]; old != nil && old.view == n.view && old.prePrepared && old.digest == d {
		return // already adopted in this view
	}
	e := n.newEntry(n.view, seq, d, pp.Req)
	e.prePrepared = true
	e.pp = &pp // the O pre-prepare (signed by the new primary) is certificate evidence
	n.log[seq] = e
	n.seen[reqKey{pp.Req.Client, pp.Req.Timestamp}] = d
	n.dropConflictingPending(n.view, seq, d)
	// Backups multicast a matching PREPARE for the new view's proposal. The new
	// primary does not: it proposed via the O pre-prepare, so its vote is
	// already counted (PBFT counts 2f prepares from distinct backups).
	if primaryID(n.all, n.view) != n.id {
		pr := &Prepare{View: n.view, Seq: seq, Digest: d, Sender: n.id}
		pr.Sig = n.sign(pr)
		n.broadcast(func(p string) {
			_ = n.tr.SendPrepare(context.Background(), p, pr)
		})
	}
	n.syncEntry(seq)
}

// hasUnexecutedLocked reports whether this replica holds an accepted proposal
// it has not executed (the condition for suspecting a stuck primary). Must
// hold n.mu.
func (n *Replica) hasUnexecutedLocked() bool {
	for seq, e := range n.log {
		if seq >= n.nextExec && e.prePrepared {
			return true
		}
	}
	return false
}
