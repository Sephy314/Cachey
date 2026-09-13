# PBFT → Chained HotStuff Migration Design

> Status: **v7 — HS-M1…HS-M5 implemented, Phase 1 hardening complete**
> (`internal/hotstuff/` + the `internal/server` integration; race-clean; PBFT removed).
> `cacheyd -consensus hotstuff` wiring (the runnable binary) is deferred — M5's
> completed scope is "core integration".
> Basis: Yin, Malkhi, Reiter, Golan-Gueta, Abraham, *"HotStuff: BFT Consensus with
> Linearity and Responsiveness"* (PODC '19), arXiv:1803.05069. The chain rules,
> commit/lock rules and vote rules are pinned to that paper's §5 (Chained), §6
> (Event-driven Implementation, Alg. 4/6) and Appendix B (implementation
> pseudocode and safety proof).
>
> v7 (Phase 1 hardening — transport security, identity lifecycle, validation):
> - **mTLS transport**: `TCPTransport.EnableTLS(ca, cert, key)` wraps the listener
>   in `mtls.Server` (TLS 1.3, `RequireAndVerifyClientCert`, CA chain check) and
>   dials through `mtls.Client` with `ServerName` = the peer's node id, so a
>   certificate from another CA or for another name is refused. The listener's
>   accept predicate admits only configured validator identities.
> - **Two identity layers, neither replacing the other**: mTLS authenticates the
>   *transport* peer (certificate DNS SAN = node id); Ed25519 authenticates the
>   *consensus* sender (Hello key + per-message signatures). `exchangeHello`
>   additionally requires `mtls.PeerIdentity(conn) == Hello.ID`, so transport and
>   consensus identity cannot disagree. The consensus key and the TLS key are
>   always distinct keypairs bound to the same node id.
> - **Fetch-response validation (P0)**: `HandleBlock` now admits a `BlockMsg` only
>   (a) as the answer to a fetch this replica actually issued (TTL-bounded
>   `fetches` set) and (b) when it carries the same structural evidence a proposal
>   must (`Justify` is a genuine QC, `Justify.NodeID == Parent`,
>   `Height > Justify.Height`). `Justify` is not covered by the content-derived
>   block id, so without both gates any member could inject fabricated blocks into
>   the tree and move the head. The fetch TTL also removes a permanent stall: a
>   request lost while a peer was unreachable could otherwise never be re-issued.
> - **QC height agreement**: `qcValid` rejects a QC whose claimed height disagrees
>   with the block it certifies, when that block is known locally.
> - **Wire/message hardening**: `readWireLine` enforces a 4 MiB limit *before* the
>   reader blocks again (a `ReadSlice`-only check waits forever for a delimiter),
>   the Hello exchange runs under a read deadline (a peer that never introduces
>   itself cannot pin a goroutine), `Close` also drops accepted inbound
>   connections (read loops would otherwise leak a goroutine and a socket each),
>   and `ConnectPeers` skips a peer that is still down instead of aborting the
>   whole mesh exchange.
> - **Lock order fixed**: `connMu` is always taken before `pc.mu` (`peerConn`
>   dials and runs the Hello handshake without holding `pc.mu`), removing the
>   ABBA deadlock between `Close` and a sender that held `pc.mu`.
> - **Identity lifecycle**: `hsidentity.json` is still the durable Ed25519
>   identity, but a missing identity file in a **non-empty** data directory is now
>   an error instead of a silent key rotation (a rotated key invalidates every
>   signature in past QCs). `HotStuffNodeConfig` carries only public keys; TLS
>   material is optional and must be supplied as a complete set.
> - **Phase 1 test suites**: mTLS identity enforcement, transport faults
>   (partition, peer restart, dead/rogue peers, oversized frames), vote/QC
>   validation regressions, Byzantine equivocation (in-memory and over real mTLS
>   TCP), crash recovery (QC base, held vote, higher-view rejoin), node rejoin,
>   identity persistence, and a concurrent send/flap/`Close` guard.
>
> v6.1 security-review follow-ups (all P0/P1/P2 from two review rounds):
> - **Quorum-gated view progress**: no jump to a future view from a single vc or
>   proposal — `maybeActivateLocked` enters a view only after 2f+1 view changes
>   (the pre-jump in `HandleViewChange` is gone).
> - **Vote height validation**: `voteForBlockLocked` requires `v.Height == block.Height`
>   — a correctly signed vote at the wrong height can no longer pollute the vote set
>   and permanently wedge the block's QC.
> - **Stronger genesis trust**: `qcValid` trusts a genesis QC only when
>   `NodeID==genesis && Height==0` — a forged "genesis QC" inflating the height is
>   rejected.
> - **Durable vote gate**: a failed pkVoted write suppresses the vote (a restart
>   must never re-vote at the same height, which would break QC uniqueness).
>   block/qc/watermark persistence stays best-effort.
> - **FSM persistence/recovery (P0)**: `server.OpenHotStuffNode` shares one WAL
>   between the store FSM and the engine; the durable applyFn appends each committed
>   mutation as its own store record (OpPut, …) → a restart rebuilds the FSM from
>   those records while the engine never re-executes below its watermark. Data
>   survival is pinned by `TestHotStuffPersistentRestart`.
> - **Identity/key-pin persistence (P0)**: the node's private key is persisted
>   (`hsidentity.json`, injected as `Config.PrivateKey`) so the public key is stable
>   across restarts and past QC signatures keep verifying. Validator public keys are
>   fixed before the listener opens (`HotStuffNodeConfig.ValidatorKeys`); Hello only
>   proves possession of the configured key (no TOFU). A different key for an
>   existing pin is refused, and pins persist to `hspeers.json`.
> - **Transport Close**: closes `stopCh` and lets the accept loop exit on
>   `net.ErrClosed` (no busy-loop).
> - **Store flush correction**: Chained HotStuff followers commit one block behind
>   the leader → the store's `propose()` flushes one extra empty block after the
>   leader commits so followers fold the final QC.
>
> v2 changes: (1) the commit rule is precisely pinned in §4.2/4.3 (parent/justify
> based, with a worked example). (2) HS-M1 explicitly excludes authentication and
> cryptography, and safety/liveness tests are separated. (3) The
> Core/Transport/Auth/WAL layering is made explicit. (4) HotStuff's own state model
> is specified (PBFT phases are not reused). (5) Safety and liveness are separate
> conceptual tests. (6) Fixed membership. (7) Code-level invariants (§4.8) added.

## 1. Background & Goals

- Cachey currently has two consensus engines: `internal/raft` (CFT) and
  `internal/pbft` (BFT).
- The BFT engine is replaced by **Chained HotStuff instead of PBFT**.
  - PBFT is 2 RTT/decision under an honest stable leader, but leader replacement
    (view-change) costs $O(n^3)$ messages and the implementation is complex and
    bug-prone (prepared certificates, checkpoints, state transfer…).
  - Chained HotStuff achieves safety in the same $n=3f+1$ partial-synchrony model
    with **two message types** (proposal / vote) and the **3-chain commit rule**
    alone, and leader replacement costs the same $O(n)$ as the normal path. The
    pacemaker (view synchronization) is simple.
  - PBFT's proven skeleton is carried over: no bespoke leader election (a
    deterministic `p = view % N` over the sorted member set), Ed25519 message
    authentication, WAL persistence and store integration
    (`PbftClusterStore`).

## 2. Positioning of the Two Engines (design philosophy)

| | **Raft** (`internal/raft`) | **HotStuff** (`internal/hotstuff`, new) |
|---|---|---|
| Fault model | CFT (crash) | BFT (byzantine, up to $f$, $n=3f+1$) |
| Decision latency | ~2 message round trips (leader→log) | 3-chain: three QCs must accumulate after a proposal (deliberately slow) |
| Leader | elected, stable | rotates per height (view), resynchronized on timeout |
| Communication (leader change) | $O(n)$ log | $O(n)$ on both the normal and the replacement path |
| Purpose | light and cheap — trusted small-scale/low-latency | slow and stable — adversarial environments, safety first |

- HotStuff is designed to be **slow and stable**: commits require a 3-chain,
  leaders change only on suspicion/timeout, and the safety rules are never
  compromised. Performance tuning (block batching, threshold signatures) is
  deliberately deferred.
- Raft is left exactly as it is (kept light and cheap).

## 3. System Model

- Fixed membership of $n = 3f+1$ (1, 4, 7, …). Up to $f$ Byzantine replicas,
  including the leader.
- Partial synchrony: bounded delay $\Delta$ after GST. Safety always, liveness
  after GST.
- Message authentication: leader proposals, votes, view changes and block
  replies are Ed25519-signed (the PBFT M3 approach). Authentication is layered —
  see below.
- Key distribution: validator public keys are fixed as part of the cluster
  configuration, and Hello only proves possession of that key. Trust on first
  receive is never used to establish a validator identity.
- Transport authentication (Phase 1): the peer transport runs under mutual TLS so
  connections are only accepted from a validator's own certificate (DNS SAN = node
  id), on top of — never instead of — the Ed25519 consensus signatures. The
  consensus key and the TLS key are separate keypairs for the same node id.
- Leader: deterministic round-robin over the sorted member set. An incumbent leader
  that keeps making progress stays (§6 pacemaker's "incumbent leader chaining");
  on timeout the cluster resynchronizes to the next leader.
- Client writes go to the current leader (primary) only; non-leaders return
  `ErrNotLeader` plus a redirect hint.

## 4. Chained HotStuff Protocol (implementation reference)

### 4.1 Blocks and QCs

- **Block** $b$: `(height, cmd, parent, justify)`.
  - `height`: monotonically increasing. `parent`: the parent in the block tree
    (hash/pointer).
  - `cmd`: the client command to execute (one cache write = one block; batching is
    deferred).
  - `justify`: the QC this block carries (certifying an ancestor towards the
    parent).
- **QC (quorum certificate)**: a set of $2f+1$ distinct partial signatures (votes)
  over a specific (height, block digest). No threshold-signature library is used:
  individual signatures are listed and verified one by one (no new dependency; the
  same approach as PBFT). QC size is $O(n)$ — a deferred optimization target.
- **genesis** $b_0$: carries a hard-coded QC pointing at itself. Initially
  `b_exec = b_lock = b_leaf = b_0` and `qc_high` = the QC of $b_0$.
- Tree retention: blocks are kept as a tree via parent/justify links. Missing
  ancestors are requested from peers by digest (see §4.6).

### 4.2 Chain Rules — precise definition (paper §5 + Appendix B notation)

The parent link `b.parent` (block tree) and the justification `b.justify` (the QC a
block carries) are defined separately. The block a QC certifies is written as
`b.justify.node`.

- **one-chain test** `oneChainedBy(parent a, child c)`:
  `c.parent == a` **and** `c.justify.node == a` (the child directly certifies its
  parent). → paper notation $a(\Leftarrow\land\leftarrow)c$.
- When a replica learns a new QC (certifying block $c$), it runs `update` (§4.3):
  - **lock (2-chain)**: if $c$ one-chains its parent, lock that parent (the lower
    of two direct one-chain steps: $b' = c.parent$, condition
    `oneChainedBy(b', c)`).
  - **commit (3-chain)**: if that parent $b'$ in turn one-chains its own parent
    $b$, commit $b$ (condition `oneChainedBy(b, b')`).
- **Worked example** (a consecutive chain where each block directly certifies its
  parent, with `B1` carrying cmd1):

  ```
  B0(genesis) ← B1 + QC(B0) ← B2 + QC(B1) ← B3 + QC(B2) ← B4 + QC(B3)
  ```

  | Newly certified block (received/formed) | lock | commit |
  |---|---|---|
  | QC(B1) → c=B1 | B0 (harmless) | — |
  | QC(B2) → c=B2 | B1 | B0 (no cmd, harmless) |
  | QC(B3) → c=B3 | B2 | **B1 (cmd1 committed!)** |
  | QC(B4) → c=B4 | B3 | B2 |

  In other words, **for leaders and followers alike, a block commits once the QC two
  blocks above it forms** (cmd1 commits when QC(B3) forms = the paper's "committed
  at the end of v4"). A leader aggregates votes itself, so it learns its own block's
  commit one block earlier than a follower, but commit is a local decision and that
  is safe. A follower reaches the same commit through the QC carried by the next
  proposal.

  > ⚠️ Review correction: the original diagram said "B1 commits once B3 arrives",
  > but per the paper that point is the **lock** boundary (on receiving B3:
  > lock=B1, commit=B0). B1 commits when B4 (the proposal carrying QC(B3)) arrives or
  > forms. The rule is pinned as 2-chain lock / 3-chain commit, and the HS-M1 tests
  > pin this exact boundary.

### 4.3 Replica State and Rules (implementation reference)

State: `b_exec` (last executed), `b_lock` (the lock), `qc_high` (highest QC),
`head` (last proposed/accepted block), `vheight` (last voted height). Genesis
$b_0$ carries a hard-coded QC certifying itself, so initially
`b_exec=b_lock=head=b0`, `qc_high=QC(b0)`.

- **`update(qc')`** (on receiving a QC; executes the §4.2 rules): if the QC is
  higher, raise `qc_high` → walk the direct one-chain below the certified block $c$
  to advance lock/commit → execute the `cmd` of every committed block in chain
  order. This follows `update()` from paper §6 Alg. 4, except that **M1 only handles
  direct one-chains (no gaps)**; gaps (dummy nodes/relaxation) are introduced
  together with HS-M2's view synchronization.
  (ponytail: M1 requires consecutive heights and `justify.node == parent`. Gap
  tolerance arrives in M2.)
- **Vote rule (safeNode)**: vote for a leader proposal $b$ iff
  1. $b.height > vheight$ (monotonicity — no re-voting at a height ⇒ Lemma 1's QC
     uniqueness), and
  2. $b$'s branch extends `b_lock` **or** $b.justify.node.height > b_lock.height$.
  Structural validation (M2): the proposal must come from the leader of the block's
  own view, `justify` must be valid (≥2f+1), `justify.node == parent`, and
  `height > parent.height` (gaps allowed — a post-view-change block skips the
  deposed leader's in-flight height). Stale-view proposals are ignored; a
  future-view proposal makes the receiver advance its view (fast recovery).
  (Signature verification arrives in M3.)
- **Leader**: proposes via `createLeaf` (parent = `head`, justify = `qc_high`).
  Proposals are multicast to everyone; votes go to the leader (incumbent-leader
  assumption in M1). The leader also votes for its own block, so it is part of the
  quorum. Leader succession/rotation is swapped in by the HS-M2 pacemaker behind a
  `leader(height)` hook.
- **HS-M1/M2 have no client queue or driver**: the leader creates blocks only
  through an explicit `Propose(cmd)`, and the caller (test or higher layer) manually
  proposes the following blocks (including empty ones) to advance commits. The
  client queue, automatic progress and `Submit` are added in **HS-M5** (needed as
  `Submit`+`WaitCommitted` for store integration). (Review: keep the consensus core
  minimal, safety first.)

### 4.4 Message Types (two in total, plus sync helpers)

1. **Proposal** (= `new-view` + proposal): the leader multicasts a new block
   (including its parent and justify).
2. **Vote** (partial signature): a replica's signature over `(height, digest)` →
   the next leader.
3. View synchronization (collecting new-views, fetching missing blocks) is built
   from combinations of the two types plus auxiliary request/response messages (see
   §4.6). This matches the paper's §5 message minimality, and PBFT's separate
   view-change messages (VIEW-CHANGE/NEW-VIEW) disappear.

### 4.5 Pacemaker (liveness, paper §6 + §4.4)

- When a replica sees no progress from the current leader (proposal-receive
  timeout), it sends a `new-view` (= the highest QC it knows) to the next leader,
  growing its **timeout by exponential backoff**.
- The next leader picks the **highest QC** among $2f+1$ new-views and continues
  proposing from there (no PBFT-style "certificate collection" — choosing the
  highest QC alone is safe, per §4.4's liveness proof).
- Rotation: round-robin over the sorted member set. It is deterministic, so tests
  can trigger it directly (giving the same deterministic test lever as PBFT's
  `StartViewChange`).
- Locks need no separate unlock proof: hotstuff's "changing your mind" 3-phase
  structure removes the need for one.

### 4.6 State Synchronization (lightweight)

- Commit/lock/vote need only **the blocks and QCs on the current branch**. Stale
  blocks may be GC'd (ponytail: checkpoint-based state transfer is deferred — on a
  gap, only fetch the missing blocks by digest from a peer; assumes at most $f$
  faulty and connectivity).
- A restarting replica rejoins with the block tree/watermark recovered from its WAL
  (HS-M4).

### 4.7 Implementation Layers (separation of concerns — review)

```
HotStuffNode
├── Core        (this package: block tree/QC/lock/commit rule/safeNode/vote state)
│    events: Propose, ReceiveProposal, ReceiveVote, ReceiveQC, (M2+) Timeout
├── Transport   (interface separate from Core — in-memory in M1, TCP later)
├── Auth        (M3: Ed25519 sign/verify — a layer on top of Core)
└── WAL         (M4: persistence — independent of Core)
```

- **Core is a pure state machine**: it has no dependency on Transport/Auth/WAL and
  receives only events such as `Propose/HandleProposal/HandleVote`. M1 is therefore
  verified with fully deterministic in-memory tests, and attaching TCP later reduces
  the amount of consensus logic that must be re-verified.
- M1's vote expresses only **who voted** (a set of voter ids), with no signatures —
  cryptography is added in M3 as the `Signed Vote → QC verification` layer. This
  keeps consensus bugs and authentication bugs independently verifiable.
- M1 does not introduce network/TLS/WAL/Ed25519 at the same time. Membership is
  fixed at $3f+1$ (review). The implementation uses HotStuff's own state model
  (block tree/QC chain) and does not reuse PBFT's
  pre-prepare/prepare/commit/view-change phases under new names.

### 4.8 Code-Level Invariants (review — pinned by tests)

Invariants that the implementation and tests must always verify:

```
QC requires ≥ 2f+1 valid member votes          (non-members cannot vote)
Committed block ⇒ has a valid 3-chain          (two direct one-chains + QC)
Committed blocks ⇒ form a single prefix        (no fork)
Conflicting blocks ⇒ cannot both be committed  (includes QC uniqueness per height)
Non-validator ⇒ cannot contribute to quorum
```

- **Safety tests**: two conflicting blocks are never committed simultaneously.
- **Liveness tests**: with a healthy network and a quorum, commits eventually
  happen. "Nothing commits" (latency) and "a wrong block committed" (safety
  violation) are distinguished by separate tests.

## 5. Reuse / Deletion Map Against Existing Code

**Reused as-is (unchanged)**
- `internal/store`, `internal/wal` (persistence backend), `internal/mtls` (transport
  TLS), `internal/server`'s `Server`/`Handler`, `pkg/client`, `internal/protocol`.
- The entire raft family is untouched.

**Patterns carried over (copied and adapted from PBFT into the new package)**
- `internal/hotstuff/`: Ed25519 sign/verify helpers, the TCP NDJSON multicast
  transport plus the configured-validator-key/Hello-possession pattern, the TLS
  on/off pattern, the WAL `LogStore` pattern, and in-memory
  transport/cluster-bootstrap test helpers.
- `internal/server/hotstuff_cluster.go`: the counterpart of `PbftClusterStore` —
  `NewHotstuffClusterStore`, `NewHotstuffApply`, read-your-writes, leader redirect.

**Deleted (PBFT removal)**
- All of `internal/pbft/` (normal case + viewchange + auth + persist + transports
  + tests).
- `internal/server/pbft_cluster.go`, `internal/server/pbft_cluster_test.go`.
- Reference cleanup: the `-consensus pbft` branch in `cmd/cacheyd/main.go` (wording
  updated to the hotstuff baseline — the wiring itself is a separate step), the
  `OpPBFT` comments in `internal/store/store.go` and `internal/wal/*`, the
  `internal/mtls` comments, and `README.md`'s engine table/feature list.
- *(Resolved)*: `wal.OpPBFT` was removed and `wal.OpHotStuff` now carries consensus
  records; the WAL recovery path keeps its shape.

## 6. Milestones (parallel to the PBFT M1–M5 numbering)

Each milestone closes with `go build ./...` + `go test ./...` green at that stage
(repo rule: serialize packages with `-p 1`). Checkpoint after each stage before
moving on.

- **HS-M1 — Normal-case core (in-memory, unauthenticated, fixed leader)**:
  block tree, QC (vote collection), 3-chain commit / 2-chain lock, safeNode,
  deterministic in-order execution. No network/TLS/WAL/Ed25519. Leader = the single
  `Config.Leader` (rotation and view sync are M2). No client queue or auto driver
  (explicit `Propose`).
  - Files: `internal/hotstuff/block.go`, `message.go`, `node.go`, `node_test.go`,
    `e2e_test.go`.
  - Test matrix (review): block without a QC rejected / chain with only one QC (no
    commit) / 2-chain (lock only) / 3-chain (commit) / divergent forks (single
    prefix) / wrong parent / conflicting blocks at one height (no re-vote, QC
    uniqueness) / commit attempt below an already-committed block ignored. Plus the
    quorum boundary (2f, 2f+1), non-member votes ignored, single-leader commit with
    every replica executing in the same order (liveness), separate safety/liveness
    tests, and assertions for the §4.8 invariants.
- **HS-M2 — Pacemaker / view synchronization (unauthenticated) — done**:
  `view`/`leaderOf(v)` deterministic rotation (view-0 leader = `Config.Leader`),
  `StartViewChange` (deterministic suspicion, the same pattern as pbft) +
  `SetViewTimeout` (real timer with exponential backoff; an active leader never
  suspects itself), collecting `ViewChange`s → on 2f+1 **adopt the highest QC as the
  base** (reset `head` to the QC-certified block + skip one height via `freshBase`,
  which is always above the deposed leader's in-flight block and so preserves vote
  monotonicity), advance the view on a future-view proposal (fast recovery), ignore
  stale-view proposals, and recover missing ancestors bottom-up with
  `Fetch`/`BlockMsg` (`pendingBlocks`: chain-insert as parents arrive). The client
  queue / automatic progress (`Submit`) moved to HS-M5.
  - Files: `viewchange.go`, `viewchange_test.go` (plus ViewChange/Fetch/BlockMsg and
    `Block.View` in `message.go`).
  - Tests: leader death → 2f+1 suspicions → new leader activates → progress resumes
    (committed prefix preserved), partition/rejoin (catch up via fetch), no progress
    without suspicion/timeout, sub-quorum suspicion leaves the leader inactive,
    automatic election via the real timer, height gaps accepted and voted, stale
    leader proposals ignored (`TestLeaderDeathViewChangeResumes`,
    `TestRejoinAfterPartition`, `TestViewTimeoutFires`,
    `TestViewChangeNeedsQuorum`, `TestGapAcceptedAndVoted`,
    `TestStaleLeaderProposalIgnored`, `TestNoProgressWithoutViewChange`, …).
  - ponytail limits: (a) an isolated replica that wrongly advanced to a view higher
    than the majority does not walk itself back (it recovers on the next leader
    change) — safety and majority liveness still hold under normal partitions.
    (b) fetch assumes the peer holding the missing block (the original proposer)
    answers. (c) gaps are accepted, but a block always directly certifies its parent
    (`justify.node == parent`) — dummy nodes / indirect relaxation are not applied
    (the current design is sufficient).
- **HS-M3 — Ed25519 authentication — done**: sign/verify for proposals, votes, view
  changes and block replies; a QC is 2f+1 individually verified `(voter, sig)`
  entries; a genesis QC is special-cased as the trust root (safe because only B1 can
  carry it); key wiring bootstraps trust from preconfigured validator public keys
  (`SetPeerKey`; Hello proves possession of that key); every received message must
  satisfy `From == signer` (forgery rejected) and tampering is rejected.
  - Files: `auth.go` (new), `message.go` (the `json:"sig,omitempty"` tag — a
    lowercase tag is required for the canonical-form `sig` removal to work),
    `node.go`/`viewchange.go` (sign all outgoing messages + verify inbound +
    `qcValid` filter), `node_test.go` (signing helpers `testKeyOf`/`wirePhantom`/
    `signVote`, signed `prop`/`quorumQC`/`mainChain`), `e2e_test.go` (key wiring in
    `startCluster`), `auth_test.go` (new).
  - Tests: forged sender rejected (`TestForgedSenderRejected`), tampering rejected
    (`TestTamperedMessageRejected`), an authentic Byzantine leader equivocating is
    accepted by each follower in isolation (the threat model,
    `TestByzantineLeaderEquivocatesAuthentically`), a fabricated high QC stuffed into
    an authentic vc is filtered by the new leader
    (`TestViewChangeWithFabricatedHighQCRejected`), non-member votes ignored,
    sub-quorum QCs rejected (from M3 on, a sub-quorum QC over genesis is the trust
    root, so the test uses a real block), and all cluster-level e2e/view-change tests
    (real key wiring).
  - Bugs fixed: (a) a missing `json:"sig,omitempty"` tag meant canonical form did not
    delete `sig`, so the signature covered itself (every message failed
    verification); (b) follower-originated votes were not signed, so the leader
    rejected them all (no QC ever formed); (c) `HandleProposal` read `n.leader`
    outside the lock, racing the timer-driven view change → capture it under the
    lock; (d) `TestViewTimeoutFires`' 20 ms default timer lost a timing race against
    signature cost (+race overhead) and evicted the new leader before it could
    propose → raised to 150 ms (intent preserved, flake removed).
- **HS-M4 — WAL persistence & recovery — done**: accepted blocks (pkBlock, with
  their Justify QC) · qcHigh raises (pkQC — a leader's vote-aggregated QC is not
  embedded in any block, so raises are recorded separately) · the executed watermark
  (pkApplied — the bExec id after `commitUpToLocked`) · the vote height (pkVoted — on
  every vHeight raise; prevents re-voting after a crash = QC uniqueness) are
  synchronously persisted as `wal.OpHotStuff` records. On restart, WAL replay
  restores the tree/watermark/vHeight, then `FinishRecovery()` replays the recovered
  QCs structurally (no signature re-verification — persisted QCs were verified when
  accepted) to recompute qcHigh/head (= the highest QC's block)/lock/exec and marks
  the chain at or below the watermark applied → applyFn never re-runs (idempotent
  recovery).
  - Files: `persist.go` (new: `LogStore`/`NewWALLogStore`/`ApplyRecoveredRecord`/
    `FinishRecovery`), `node.go` (persistence hooks in
    proposeLocked/addBlockLocked/onNewQCLocked/handleProposalLocked/commitUpToLocked),
    `internal/wal` (the `OpHotStuff` op + the recovery allow-list),
    `wal_persist_test.go` (new).
  - Tests: `TestWALPersistenceRestart` (single node commits → restart → no
    re-execution + keeps committing), `TestWALRecoveryRebuildsTree` (uncommitted
    accepted blocks/QCs recovered, then committed), `TestWALVoteHeightSurvivesRestart`
    (follower vote height restored → no re-vote + voting resumes above it),
    `TestWALRecoveryIgnoresForeignOps` (non-HotStuff records ignored).
  - Phase 1 additions: `TestWALQCRecoveryRestoresBase` (the recovered
    qcHigh/head/lock/exec equal the pre-crash state), `TestWALRestartRejoinsAtHigherView`
    (a restarted replica joins a higher view only with a 2f+1 VC certificate, then
    votes above its recovered height), `TestWALHeldVoteBlocksRestartedDoubleVote`
    (a vote that was persisted but never sent still blocks a conflicting vote at the
    same height after restart).
  - ponytail limits: (a) persistence of the watermark, etc. is best-effort (a failure
    logs, the handler continues) — as in PBFT M4, so a later crash may re-execute the
    last span (production should make write failures fatal or retried). (b) WAL growth
    is unbounded (no engine-level snapshot; `DisableRotation` log mode) — compaction
    is deferred. (c) A leader's vote-aggregated QC is persisted, but a crash at a
    moment when a certified block has not arrived (e.g. the base QC during a fetch)
    can lose that QC (recovery is by rejoining; safety is unaffected).
- **HS-M5 — Server integration & PBFT removal — done**:
  `internal/hotstuff/tcp_transport.go` (NDJSON TCP, a Hello that verifies the
  preconfigured validator key on every connection + `ConnectPeers` full-mesh key
  exchange), `internal/server/hotstuff_cluster.go` (`HotStuffClusterStore` — leader-only
  writes; the leader flushes empty blocks above a command block to force the 3-chain
  commit; `NewHotStuffApply`), `internal/server/hotstuff_cluster_test.go` (4-node TCP
  cluster: write/read convergence, follower `ErrNotLeader` rejection, leader hint,
  DEL). All of `internal/pbft` plus `server/pbft_cluster.go`/`_test.go` were deleted,
  `OpPBFT` was removed from `wal` (the store treats `OpHotStuff` as a no-op), and the
  `cmd/cacheyd`/`README.md`/`mtls` references were cleaned up.
  `cacheyd -consensus hotstuff` wiring is deferred (a static peer list with fixed
  membership).
  - Pitfall found: HotStuff's message pattern (proposals leader→all, votes all→leader)
    never connects followers to each other, so the transport builds a full mesh at
    startup with `ConnectPeers` (pbft broadcasts, so it was naturally full-mesh). Key
    trust, however, was already settled by the validator configuration before that
    connection.
- **Phase 1 — Hardening (before cacheyd wiring) — done**: mutual TLS on the peer
  transport with node-id SAN pinning; fetch-response validation; QC height agreement;
  bounded wire frames, Hello deadlines and inbound-connection teardown; the
  `connMu → pc.mu` lock order; identity-file and TLS-configuration lifecycle rules;
  and the Byzantine/recovery/transport-fault/concurrency test suites listed in the
  status block above.
- **After HS-M5 / Phase 1 (deferred)**: `cacheyd -consensus hotstuff` wiring (static
  peer list), a leader driver with command batching, threshold-signature QCs,
  checkpoints/state transfer, and dynamic membership.

## 7. ponytail Limits (explicit trade-offs)

- Reads are primary read-your-writes (the same limitation as the PBFT ClusterStore;
  quorum-linearizable reads are deferred alongside raft read-index). Each site is
  marked with a `ponytail:` comment.
- Commit latency is 3 blocks (slow on purpose). No block batching → one block per
  write.
- No threshold signatures in QCs → QC size is $O(n)$ signatures.
- Fixed membership, no reconfiguration. Leader rotation is deterministic (no
  randomness) — liveness is guaranteed by exponential backoff.
- Phase 1 additions: the transport is authenticated by mTLS, but the suspicion
  timer is not yet wired into `OpenHotStuffNode` (tests drive `StartViewChange`
  deterministically), so an unattended cluster currently relies on an external
  driver for leader-failure liveness. A restarted follower also catches up only by
  riding the next chain advance (no separate state-sync protocol).
- Persistence of blocks/QCs/watermark stays best-effort (a failed write logs and
  the handler proceeds); WAL growth is unbounded (no engine-level snapshot) and
  `blocks`/`applied` are never GC'd.

## 8. References

- Yin et al., HotStuff (arXiv:1803.05069), §4–§6, Appendix A/B.
- This repo: `internal/raft` (conventions and test-structure reference),
  `internal/mtls` (authentication/transport), `internal/server/hotstuff_node.go`
  (durable wiring), and repo memory `gotchas.md` (PBFT/Raft pitfalls are reused to
  avoid repeating the same mistakes during HotStuff work).
