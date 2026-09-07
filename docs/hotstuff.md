# PBFT → Chained HotStuff 전환 기획서

> 상태: **v4 — HS-M1·HS-M2·HS-M3 구현 완료** (`internal/hotstuff/`, race 클린).
> HS-M4(WAL 영속화 & 복구) 대기.
> 기준: Yin, Malkhi, Reiter, Golan-Gueta, Abraham, *"HotStuff: BFT Consensus with
> Linearity and Responsiveness"* (PODC '19), arXiv:1803.05069. 체인 규칙/커밋·락
> 규칙/투표 규칙은 위 논문 §5(Chained), §6(Event-driven Implementation, Alg. 4/6),
> Appendix B(구현 의사코드 안전성 증명) 기준으로 고정한다.
>
> v2 변경: (1) commit rule을 §4.2/4.3에서 정밀 고정(부모/justify 기반 판정,
> 작업 예시 포함). (2) HS-M1에 인증/암호 미포함 명시 + 안전/활성 테스트 분리.
> (3) Core/Transport/Auth/WAL 계층 분리 명시. (4) HotStuff 고유 상태 모델
> (PBFT 단계 재사용 금지) 명시. (5) 안전/활성 별도 개념 테스트. (6) 멤버십 고정.
> (7) 코드 레벨 불변식(§4.8) 추가.

## 1. 배경 & 목적

- 현재 Cachey는 두 합의 엔진을 가진다: `internal/raft`(CFT)와 `internal/pbft`(BFT).
- BFT 엔진을 **PBFT 대신 Chained HotStuff**로 교체한다.
  - PBFT는 안정 리더 가정하 2 RTT/결정이지만 리더 교체(view-change) 시 통신이
    $O(n^3)$이고 구현이 복잡·버그에 취약하다(준비 인증서, 체크포인트, 상태 전송…).
  - Chained HotStuff는 동일한 $n=3f+1$ 부분 동기 모델에서 **두 가지 메시지 타입**
    (proposal / vote)과 **3-chain 커밋 규칙**만으로 안전성을 얻고, 리더 교체가
    정상 경로와 동일한 $O(n)$ 비용이다. 뷰 동기화(pacemaker)가 단순하다.
  - PBFT의 자체 leader-election 부재(정렬된 멤버셋에 `p = view % N`), Ed25519 메시지
    인증, WAL 영속화, store 통합(`PbftClusterStore`)이라는 검증된 골격은 그대로
    이어받는다.

## 2. 두 엔진의 포지셔닝 (설계 철학)

| | **Raft** (`internal/raft`) | **HotStuff** (`internal/hotstuff`, 신규) |
|---|---|---|
| 장애 모델 | CFT (crash) | BFT (byzantine, 최대 $f$, $n=3f+1$) |
| 결정 지연 | ~2 메시지 왕복 (leader→log) | 3-chain: 제안 후 3개 QC가 쌓여야 커밋 (의도적으로 느림) |
| 리더 | 선거로 선출, 안정적 | 높이(뷰) 단위로 교체, 타임아웃 시 동기화(회전) |
| 통신 (리더 교체) | $O(n)$ log | 정상/교체 모두 $O(n)$ |
| 용도 | 가볍고 싸게 — 신뢰 가능한 소규모/저지연 | 느리고 안정적 — 적대적 환경, 안전 우선 |

- HotStuff는 **느리고 안정적이게** 설계: 커밋에 3-chain을 요구하고, 의심/타임아웃
  기반으로만 리더를 바꾸며, 안전 규칙을 절대 타협하지 않는다. 성능 튜닝(블록 배칭,
  threshold signature)은 의도적으로 후순위.
- Raft는 기존 그대로 두고 건드리지 않는다(가볍고 싸게 유지).

## 3. 시스템 모델

- $n = 3f+1$ 고정 멤버(단독 1, 4, 7, …). 리더 포함 최대 $f$개의 Byzantine.
- 부분 동기: GST 후 유계 지연 $\Delta$. 안전은 항상, 활성(liveness)은 GST 후.
- 메시지 인증: 리더 제안/부분 투표를 Ed25519로 서명 (PBFT M3 방식 계승).
- 키 분배: 최초 홉 TOFU(첫 수신 시 공개키 등록) — PBFT와 동일한 한계/업그레이드 경로
  (클러스터 설정에 키 고정, 또는 전송 TLS).
- 리더: 정렬된 멤버셋에 대해 결정적 라운드-로빈. 현재 리더가 진행(progress)하면
  유임(§6 pacemaker의 "incumbent leader chaining"), 타임아웃 시 다음 리더로 동기화.
- 클라이언트 쓰기는 현재 리더(primary)로만; 비리더는 `ErrNotLeader` + 리다이렉트 힌트.

## 4. Chained HotStuff 프로토콜 (구현 기준)

### 4.1 블록/QC

- **블록** $b$: `(height, cmd, parent, justify)`.
  - `height`: 단조 증가. `parent`: 블록 트리에서 부모(해시/포인터).
  - `cmd`: 실행할 클라이언트 커맨드 (캐시 쓰기 1건 = 블록 1개; 배칭은 후순위).
  - `justify`: 이 블록이 지니는 QC(부모 방향의 조상 블록을 증명).
- **QC (quorum certificate)**: 특정 (height, 블록 다이제스트)에 대한 $2f+1$개 서로 다른
  부분 서명(투표) 집합. threshold signature 라이브러리는 쓰지 않고 개별 서명을 나열·
  검증한다(신규 의존성 없음, PBFT와 동일 접근). QC 크기는 $O(n)$ — 후순위 최적화 대상.
- **genesis** $b_0$: 자기 자신을 가리키는 하드코딩 QC 포함. 초기 `b_exec = b_lock = b_leaf =
  b_0`, `qc_high` = $b_0$의 QC.
- 트리 보관: 부모/justify 링크로 블록 트리 유지. 결손 조상은 다이제스트로 피어에게 요청해
  채운다(아래 4.6).

### 4.2 체인 규칙 — 정밀 정의 (논문 §5 + Appendix B Notation)

부모 링크 `b.parent`(블록 트리)와 정당화 `b.justify`(블록이 지니는 QC)를 분리해
정의한다. QC가 증명하는 블록을 `b.justify.node`라 한다.

- **one-chain 판정** `oneChainedBy(parent a, child c)`:
  `c.parent == a` **이고** `c.justify.node == a` (자식이 부모를 직접 증명).
  → 논문 표기 $a(\Leftarrow\land\leftarrow)c$.
- 리플리카가 새 QC(블록 $c$를 증명)를 알게 되면 (§4.3의 `update`):
  - **lock (2-chain)**: $c$가 부모를 one-chain하면 그 부모를 락. (직접 one-chain 두 개
    중 아래쪽: $b' = c.parent$, 조건 `oneChainedBy(b', c)`)
  - **commit (3-chain)**: 위의 부모 $b'$가 다시 자기 부모 $b$를 one-chain하면
    $b$를 커밋. (조건 `oneChainedBy(b, b')`)
- **작업 예제** (각 블록이 직접 부모를 증명하는 연속 체인, `B1`이 cmd1 보유):

  ```
  B0(genesis) ← B1 + QC(B0) ← B2 + QC(B1) ← B3 + QC(B2) ← B4 + QC(B3)
  ```

  | 새로 증명된 블록(수신/형성) | lock | commit |
  |---|---|---|
  | QC(B1) → c=B1 | B0 (무해) | — |
  | QC(B2) → c=B2 | B1 | B0 (무cmd, 무해) |
  | QC(B3) → c=B3 | B2 | **B1 (cmd1 커밋!)** |
  | QC(B4) → c=B4 | B3 | B2 |

  즉 **리더/팔로워 공통으로 "블록 2개 위의 QC가 형성되면 그 블록이 커밋"**
  (cmd1은 QC(B3) 형성 시 커밋 = 논문 "v4 끝에 커밋"). 리더는 QC를 직접 집계하므로
  자기 블록 커밋을 팔로워보다 한 블록 먼저 알지만, 커밋은 로컬 판정이라 안전하다.
  팔로워는 다음 제안이 실어 보내는 QC로 같은 커밋에 도달한다.

  > ⚠️ 리뷰 예시 보정: 다이어그램이 "B3까지 오면 B1 commit"으로 그려졌는데,
  > 이는 논문 기준 **lock 지점**(B3 수신 시 lock=B1, commit=B0)이다. B1 커밋은
  > B4(QC(B3)를 실은 제안) 도착/형성 시점이다. 2-chain lock / 3-chain commit으로
  > 고정하고, HS-M1 테스트가 이 경계를 직접 고정한다.

### 4.3 replica 상태 & 규칙 (구현 기준)

상태: `b_exec`(마지막 실행), `b_lock`(락), `qc_high`(가장 높은 QC), `head`(마지막 제안/
수용 블록), `vheight`(마지막 투표 높이). genesis $b_0$는 자기 자신을 증명하는 하드코딩
QC를 가져 초기 `b_exec=b_lock=head=b0`, `qc_high=QC(b0)`.

- **`update(qc')`** (QC 수신 시, §4.2 규칙의 실행): 더 높은 QC면 `qc_high` 갱신 →
  증명 블록 $c$ 기준 one-chain walk로 lock/commit 전진 → 커밋된 블록의 `cmd`를 체인
  순서로 실행. 논문 §6 Alg. 4의 `update()`를 따르되 **M1은 직접 one-chain(틈 없음)**
  만 처리하고, 갭(더미 노드/완화)은 HS-M2의 뷰 동기화와 함께 확장한다.
  (ponytail: M1은 연속 높이 + `justify.node == parent` 제약. 갭 허용은 M2.)
- **투표 규칙 (safeNode)**: 리더 제안 $b$에 투표 iff
  1. $b.height > vheight$ (단조성 — 같은 높이 재투표 금지 ⇒ Lemma 1의 QC 유일성), 그리고
  2. $b$의 branch가 `b_lock` 확장 **또는** $b.justify.node.height > b_lock.height$.
  구조 검증(M2): 제안은 블록이 속한 뷰의 리더가, `justify` 유효(≥2f+1),
  `justify.node == parent`, `height > parent.height`(갭 허용 — 뷰 전환 후
  deposed 리더의 in-flight 높이를 건너뀜). 낡은 뷰 제안은 무시, 미래 뷰 제안은
  수신자가 뷰를 전진(빠른 복귀). (서명 검증은 M3.)
- **리더**: `createLeaf`로 제안(부모 = `head`, justify = `qc_high`). 제안은 모두에게
  멀티캐스트, 투표는 리더에게(유임 리더 가정 — M1). 리더는 자기 블록에도 스스로 투표해
  쿼럼에 포함된다. 리더 전환/회전은 HS-M2 pacemaker에서 `leader(height)` 훅으로 교체.
- **HS-M1/M2에는 클라이언트 큐/드라이버가 없다**: 리더는 명시적 `Propose(cmd)`로만
  블록을 만들고, 다음 블록(빈 블록 포함)은 테스트/상위 계층이 수동으로 이어 제안해
  커밋을 진행시킨다. 클라이언트 대기열·자동 진행·Submit은 **HS-M5**(store 통합에서
  `Submit`+`WaitCommitted`로 필요)에 추가. (리뷰: 합의 코어를 최소화, safety 우선.)

### 4.4 메시지 타입 (전체 2종 + 동기화 보조)

1. **Proposal**(=`new-view`+제안): 리더가 새 블록(부모/justify 포함)을 멀티캐스트.
2. **Vote**(부분 서명): `(height, digest)`에 대한 리플리카 서명 → 다음 리더.
3. 뷰 동기화(new-view 수집, 결손 블록 fetch)는 위 두 타입의 조합 + 보조 요청/응답으로
   구현(아래 4.6). 이는 논문 §5의 메시지 최소성과 일치하고, PBFT의 별도 view-change
   메시지(VIEW-CHANGE/NEW-VIEW)가 사라진다.

### 4.5 Pacemaker (liveness, 논문 §6 + §4.4)

- 각 리플리카는 현 리더가 진행을 못 만들면(제안 수신 타임아웃) **타임아웃을
  지수 백오프**로 늘리며 `new-view`(=자신이 아는 최고 QC)를 다음 리더로 전송.
- 다음 리더는 $2f+1$개의 new-view에서 **최고 QC**를 골라 그 지점에서 이어 제안
  (PBFT식 "증명 수집" 불필요 — 최고 QC만 고르면 안전, §4.4 liveness 증명).
- 회전: 정렬 멤버셋 라운드-로빈. 결정적이라 테스트에서 직접 트리거 가능
  (PBFT `StartViewChange`와 같은 결정적 테스트 수단 제공).
- 락은 hotstuff가 "마음을 바꾸는" 3-phase 구조 덕에 별도 unlock 증명이 필요 없다.

### 4.6 상태 동기화(경량)

- 커밋/락/투표에 필요한 건 **현재 branch의 블록 + QC**뿐. 낡은 블록은 GC 가능
  (ponytail: 체크포인트 기반 상태 전송은 후순위 — 결손 시 다이제스트로 피어에게
  블록 fetch만 수행. $f$ 이하 faulty·연결 가정).
- 리스타트 리플리카는 WAL에서 복구된 블록 트리/watermark로 재참여(HS-M4).

### 4.7 구현 계층 (관심사 분리 — 리뷰)

```
HotStuffNode
├── Core        (본 패키지: 블록 트리/QC/락/commit rule/safeNode/투표 상태)
│    이벤트: Propose, ReceiveProposal, ReceiveVote, ReceiveQC, (M2+) Timeout
├── Transport   (Core와 분리된 인터페이스 — M1 인메모리, 이후 TCP)
├── Auth        (M3: Ed25519 서명/검증 — Core 위에 얹는 계층)
└── WAL         (M4: 영속화 — Core와 독립)
```

- **Core는 순수 상태 머신**: Transport/Auth/WAL에 대한 의존성이 없고,
  `Propose/HandleProposal/HandleVote` 같은 이벤트만 받는다. 따라서 M1은 완전 결정적
  인메모리 테스트로 검증하고, 이후 TCP 전송을 붙여도 합의 로직 재검증이 줄어든다.
- M1의 vote는 **서명 없는 "누가 투표했는가"** 만 표현(투표자 id 집합) — 암호학은 M3에서
  `Signed Vote → QC 검증` 계층으로 추가. 합의 버그와 인증 버그를 독립 검증한다.
- M1 단계에서 network/TLS/WAL/Ed25519를 동시에 넣지 않는다. 멤버십은 고정
  $3f+1$ (리뷰). HotStuff 고유의 상태 모델(블록 트리/QC 체인)로 구현하며 PBFT의
  pre-prepare/prepare/commit/view-change 단계를 이름만 바꿔 재사용하지 않는다.

### 4.8 코드 레벨 불변식 (리뷰 — 테스트로 고정)

구현/테스트가 항상 성립을 검증할 불변식:

```
QC requires ≥ 2f+1 valid member votes          (비멤버 투표 불가)
Committed block ⇒ has a valid 3-chain          (직접 one-chain 2단 + QC)
Committed blocks ⇒ form a single prefix        (분기 없음)
Conflicting blocks ⇒ cannot both be committed  (같은 높이 QC 유일성 포함)
Non-validator ⇒ cannot contribute to quorum
```

- **Safety 테스트**: 서로 다른 두 conflicting 블록을 동시에 커밋하지 않음.
- **Liveness 테스트**: 정상 네트워크+쿼럼이면 결국 커밋. "commit이 안 됨"(지연)과
  "잘못된 블록이 커밋됨"(안전 위반)을 별도 테스트로 구분.

## 5. 기존 코드 재사용 / 삭제 매핑

**그대로 재사용 (변경 없음)**
- `internal/store`, `internal/wal`(영속 백엔드), `internal/mtls`(전송 TLS),
  `internal/server`의 `Server`/`Handler`, `pkg/client`, `internal/protocol`.
- Raft 전 계열은 불변.

**패턴 계승 (PBFT에서 복사-수정, 새 패키지로 이동)**
- `internal/hotstuff/`: Ed25519 sign/verify 헬퍼, TCP NDJSON 멀티캐스트 transport +
  Hello 키 교환(TOFU) 패턴, TLS on/off 패턴, WAL LogStore 패턴, 테스트용
  인메모리 transport/클러스터 부트스트랩 헬퍼.
- `internal/server/hotstuff_cluster.go`: `PbftClusterStore`의 쌍대 —
  `NewHotstuffClusterStore`, `NewHotstuffApply`, read-your-writes, 리더 리다이렉트.

**삭제 (PBFT 제거)**
- `internal/pbft/` 전체 (normal case + viewchange + auth + persist + transports + tests).
- `internal/server/pbft_cluster.go`, `internal/server/pbft_cluster_test.go`.
- 참조 정리: `cmd/cacheyd/main.go`의 `-consensus pbft` 분기(문구를 hotstuff 기준으로
  갱신 — wiring 자체는 별도 단계), `internal/store/store.go`, `internal/wal/*`의
  OpPBFT 주석, `internal/mtls` 주석, `README.md` 엔진 표/기능 목록.
- **공개 결정(open item, HS-M4)**: `wal.OpPBFT` 레코드 op는 consensus-log 레코드
  용도이므로 값은 유지하되 이름을 엔진 중립적으로 다룰지(HotStuff가 이 op를 계승해
  재사용할지) 구현 시 확정. (WAL 회복 경로는 불변으로 유지하려 함.)

## 6. 마일스톤 (PBFT M1–M5 넘버링과 평행)

각 마일스톤은 그 단계에서 `go build ./...` + `go test ./...`(repo 규칙: 패키지 직렬
`-p 1`)가 초록이 되게 마감한다. 단계마다 체크포인트 후 다음으로.

- **HS-M1 — Normal-case 코어 (인메모리, 무인증, 리더 고정)**: 블록 트리, QC(투표
  수집), 3-chain 커밋·2-chain 락, safeNode, 결정적 순서 실행. network/TLS/WAL/Ed25519
  없음. 리더 = `Config.Leader` 단일 지정(회전·뷰 동기화는 M2). 클라이언트 큐/자동
  드라이버 없음(명시적 `Propose`).
  - 파일: `internal/hotstuff/block.go`, `message.go`, `node.go`, `node_test.go`,
    `e2e_test.go`.
  - 테스트 매트릭스(리뷰): QC 없는 블록 거부 / QC 1개뿐 chain(커밋 없음) / 2-chain
    (락만) / 3-chain(커밋) / 서로 다른 fork(단일 프리픽스) / 잘못된 parent / 같은
    높이 conflicting 블록(재투표 금지·QC 유일성) / 이미 커밋된 블록보다 과거 블록
    커밋 시도 무시. + 쿼럼 경계(2f, 2f+1), 비멤버 투표 무시, 단일 리더 커밋&전
    리플리카 동일 순서 실행(liveness), Safety/Liveness 테스트 구분, §4.8 불변식 assert.
- **HS-M2 — Pacemaker / 뷰 동기화 (무인증) — 구현 완료**: `view`/`leaderOf(v)`
  결정적 회전(뷰-0 리더 = `Config.Leader`), `StartViewChange`(결정적 의심, pbft와
  같은 패턴) + `SetViewTimeout`(지수 백오프 실타이머, 활성 리더는 자가 의심 안 함),
  `ViewChange` 수집 → 2f+1 시 **최고 QC를 베이스로 채택**(`head`를 QC 증명 블록으로
  재설정 + `freshBase`로 한 높이 스킵 — deposed 리더의 in-flight 블록보다 항상
  높아 투표 단조성을 보존), 미래 뷰 제안으로 뷰 전진(빠른 복귀), 낡은 뷰 제안 무시,
  `Fetch`/`BlockMsg`로 결손 조상을 bottom-up 복귀(`pendingBlocks`: 부모 도착 시
  연쇄 삽입). 클라이언트 큐/자동 진행(Submit)은 HS-M5로 이동.
  - 파일: `viewchange.go`, `viewchange_test.go` (+ `message.go`에
    ViewChange/Fetch/BlockMsg, `Block.View`).
  - 테스트: 리더 사망 → 2f+1 의심 → 새 리더 활성화 → 진행 재개(커밋 프리픽스 보존),
    분할/복귀(fetch로 따라잡기), 의심/타임아웃 없이는 진행 없음, 쿼럼 미달 의심은
    리더 비활성 유지, 실타이머로 자동 선출, 높이 갭 수용&투표, 스테일 리더 제안 무시
    (`TestLeaderDeathViewChangeResumes`, `TestRejoinAfterPartition`,
    `TestViewTimeoutFires`, `TestViewChangeNeedsQuorum`, `TestGapAcceptedAndVoted`,
    `TestStaleLeaderProposalIgnored`, `TestNoProgressWithoutViewChange` 등).
  - ponytail 한계: (a) 다수보다 높은 뷰로 잘못 전진한 고립 리플리카는 스스로
    내려오지 않음(리더 교체 시 복귀) — 정상 파티션에서 안전·다수 활성은 보존.
    (b) fetch는 결손 블록을 가진 피어(원 제안자)가 응답한다고 가정.
    (c) 갭은 수용하되 블록은 항상 직접 부모(`justify.node == parent`)를 증명 —
    더미 노드/비직접 완화는 미적용(현 설계로 충분).
- **HS-M3 — Ed25519 인증 — 구현 완료**: 제안/투표/뷰체인지/블록응답 서명·검증, QC =
  (voter, sig) 2f+1 개별 검증, genesis QC는 신뢰 루트로 특례(B1만 실을 수 있어
  안전), 키 배선은 신뢰 부트스트랩(`SetPeerKey`, pbft TOFU와 대칭; TCP/mTLS는 후순위),
  수신 모든 메시지가 `From == 서명자`여야 통과(위조 거부), 변조 거부.
  - 파일: `auth.go`(신규), `message.go`(`json:"sig,omitempty"` 태그 — canonical에서
    `sig` 삭제가 동작하도록 소문자 태그 필수), `node.go`/`viewchange.go`(모든 발신
    서명 + 수신 검증 + `qcValid` 필터), `node_test.go`(서명 헬퍼 `testKeyOf`/
    `wirePhantom`/`signVote`, `prop`/`quorumQC`/`mainChain` 서명화), `e2e_test.go`
    (`startCluster`에 키 배선), `auth_test.go`(신규).
  - 테스트: 위조 발신자 거부(`TestForgedSenderRejected`), 변조 거부
    (`TestTamperedMessageRejected`), 정통 악의 리더 이중 제안 각각 유효(위협 모델,
    `TestByzantineLeaderEquivocatesAuthentically`), 정통 vc에 실은 조작 고QC는 신규
    리더가 필터(`TestViewChangeWithFabricatedHighQCRejected`), 비멤버 투표 무시,
    sub-quorum QC 거부(M3부터 genesis 위 sub-quorum은 신뢰 루트라 실재 블록 기준으로
    테스트 재구성), e2e/뷰체인지 전 클러스터 테스트(실 키 배선).
  - 버그 수정: (a) 서명 필드에 `json:"sig,omitempty"` 태그 누락 → canonical에서
    `sig` 삭제가 안 되어 서명이 자기 자신을 포함(전 메시지 검증 실패);
    (b) 팔로워 발신 투표에 서명 누락 → 리더가 투표 전부 거부(QC 미형성);
    (c) `HandleProposal`이 락 밖에서 `n.leader`를 읽어 타이머 뷰체인지와 데이터
    레이스 → 락 안에서 캡처; (d) `TestViewTimeoutFires` 20ms 기본 타이머가 서명
    비용(+race) 타이밍 경쟁으로 새 리더를 제안 전 축출 → 150ms로 상향(의도 보존).
- **HS-M4 — WAL 영속화 & 복구**: 수용한 블록·QC·실행 watermark 영속화, 리스타트 시
  트리/watermark 복구, watermark 아래 재실행 금지(멱등 복구). `wal_persist_test.go`
  스타일 크래시-재시작 테스트.
- **HS-M5 — 서버 통합 & PBFT 삭제**: `internal/server/hotstuff_cluster.go`
  (HotStuffClusterStore + persistent node 빌더, raftnode.go와 대칭),
  `pbft_cluster_test.go`의 대체 클러스터 테스트(TCP, 실제 store FSM). 성공 후
  `internal/pbft` 및 `pbft_cluster.go` 삭제 + 참조/문서 정리. 이 시점 전체 트리 초록.

**이번 범위**: HS-M1 ~ HS-M5. **별도 단계(후순위)**: `cacheyd -consensus hotstuff`
wiring(bootstrap/join), 커맨드 배칭, threshold signature QC, 체크포인트/상태 전송,
동적 멤버십.

## 7. ponytail 한계 (명시적 트레이드오프)

- 읽기는 primary의 read-your-writes(PBFT ClusterStore와 동일 한계; quorum 선형화 읽기는
  raft read-index와 함께 후순위). 각 위치 `ponytail:` 주석으로 표기.
- 커밋 지연이 3-block(느림은 의도). 블록 배칭 없음 → 쓰기 1건당 블록 1개.
- QC에 threshold signature 미사용 → QC 크기 $O(n)$ 서명.
- 멤버 고정/재구성 없음. 리더 회전은 결정적(랜덤성 없음) — 활성은 지수 백오프로 보장.

## 8. 참고

- Yin et al., HotStuff (arXiv:1803.05069), §4–§6, Appendix A/B.
- 본 repo `internal/pbft`(교체 대상) 및 `internal/raft`(관례·테스트 구조 참조),
  `internal/mtls`(인증/전송), repo memory `gotchas.md`(PBFT/Raft 함정 기록은 HotStuff
  구현 중 같은 실수 회피에 활용).
