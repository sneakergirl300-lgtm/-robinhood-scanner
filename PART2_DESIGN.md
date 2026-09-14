# Part 2: find strength early within a public-RPC budget

This is an observation-engine redesign, not a claim that the full runner detector is ready. Discovery must continue even when valuation and safety evidence are unavailable. All actionable alerts remain disabled until the required evidence adapters exist and have been validated.

## Inspected production behavior (14 September 2026)

Inspected `main` at `1e60893f9742515b84f647de6b4b83da2e462f41`, then the scanner-generated update `972ce9ff6f557a32d2f71798f5243587f80cb82e`.

- `collector.py` is v7; its saved initial state contained 10,424 candidates, a 6,009-second discovery window and PARTIAL coverage. The collector implementation is unchanged in this proposal.
- `.github/workflows/scan.yml` runs every 15 minutes, serializes collector and analyzer under one concurrency group, and writes both sets of outputs at the end.
- Actual run [34826320425](https://github.com/sneakergirl300-lgtm/-robinhood-scanner/actions/runs/34826320425) completed successfully, but Part 2 took **1,079.11 seconds** (17m59s); collector took 8m39s. Success did not mean timely detection.
- The original analyzer selects 80 contracts, rescans global v4 Initialize and Swap logs over a 90-minute baseline / 20-minute recurring overlap, rebuilds transfer deltas and repeats metadata. Five-attempt 429 backoff has no total runtime bound.
- Its price comes from post-swap sqrtPrice, not executed amount ratios. Its MC uses totalSupply rather than verified effective supply. Partial-window positive transfer deltas are not authoritative holder balances. Generic safety checks remain unresolved.
- Failed run [34825671499](https://github.com/sneakergirl300-lgtm/-robinhood-scanner/actions/runs/34825671499) crashed on a timestamp RPC 429 and skipped saving results.
- `.github/.github/workflows/scan.yml` is an inert nested legacy file, not the active Actions workflow. It is left untouched.

## Implemented in this proposal

| Component | Behavior | Limit / confidence |
|---|---|---|
| Discovery | Original collector and broad candidate universe retained | Existing 15-minute cadence and PARTIAL reporting retained |
| Workflow separation | Analyzer starts after collector success using its own concurrency group | No analyzer job holds the collector lock; overlapping runs may still share the RPC |
| Admission | Local exact-address merge into persistent v3 state | Reads the existing JSON universe; does not RPC-scan historical contracts |
| Hot selection | Eight due contracts; alternating new and oldest-attempted follow-ups | Capacity backlog explicitly reported; no promise that every launch gets analyzed |
| Work fairness | Six round-robin turns, small resumable tasks | Receipt, metadata and stream work persist across runs |
| Runtime | 180-second wall-clock signal, 48 requests, four-second socket timeout, 0.3-second spacing | Additional workflow timeout; final JSON save happens after RPC deadline |
| Rate limits | First HTTP/JSON rate-limit response opens run-wide circuit | Zero retries, no Retry-After sleep; missing evidence stays unresolved |
| Pool resolution | Decode Initialize logs in Part 1's exact discovery receipts | No global v4 rescan; only pools present in those receipts are known |
| Streams | Exact-token transfers; PoolManager swaps filtered by exact pool ID | At most 500 blocks per request; successful cursor plus merged outstanding gaps |
| Freshness | Fresh tail first; a later turn repairs one historical gap | Skipped intervals remain recorded, never presented as complete coverage |
| Finality | Request finalized head | Unsupported or >5-minute-old finalized head defers the run; no silent fallback |
| Price | Fresh executed amount ratio, correct decimals, direct USDG quote | Five-minute freshness; >20% disagreement across fresh pools suppresses preferred price |
| Metadata | Cache decimals; refresh reported supply; opportunistic owner balance | totalSupply is not certified effective supply; owner() is not a complete privilege audit |
| Observations | Preserve initial 16 plus most recent 48 samples, with exact ranges | Transfer recipient counts are explicitly not buyers or holders; v4 sender is not an end user |
| Safety | Reported risk evidence or owner control >=20% causes rejection | No inferred PASS; SYNTH is a user-reported precautionary hold, not independently rediscovered evidence |
| Cold state | Old bootstrap entries / six hours without new observed activity become cold; new discovery blocks reactivate | State retained; not deleted from discovery |
| Persistence | Atomic checkpoint after resumable units and on exit; legacy state archived | Corrupt JSON fails visibly rather than resetting cursors |

No historical winner address is imported into production discovery or analyzer code. `tests/historical_cases.json` is an evaluation manifest only. The SYNTH risk entry cannot add a candidate: it only blocks promotion if that exact address is already discovered.

The request ceiling bounds load, not capacity sufficiency. The observed collector added hundreds of contracts while this default admits eight per run. Queue depth, attempted count and outstanding gaps must guide tuning. Increasing the hot limit without RPC capacity would just spread unresolved work more thinly. The current 500-block sample can represent less than a minute on this chain; it is deliberately a labelled sample, not a full 15-minute flow measurement. Gap repair can fall behind indefinitely under this budget.

## Implemented hybrid enrichment boundary

`enricher.py` now consumes only the bounded `latest_analysis.json` hot set and writes
`decision_candidates.json`. It does not call the Robinhood RPC and it cannot discover,
drop, or reject a contract. Its first adapter performs keyless DexScreener searches by
contract address in one chain-scoped batch, then accepts pair data only after an exact
base-token address and `robinhood` chain match. It records third-party provenance, confidence, pair age, price,
MC/FDV, liquidity, 5m/1h flow, buys/sells, profile links, prior-run deltas, and explicitly
labels swap counts as neither buyers nor independent wallets.

Every Part 2 row produces a `NEEDS_MODEL_REVIEW` packet even when DexScreener is absent,
rate-limited, or malformed. Structural `REJECT` blocks positive promotion. Structural
`UNRESOLVED` remains observation-only and is never described as clean. Fomo, independent
social attention, holder growth, concentration, creator linkage, liquidity control,
sellability/tax, and public scam trackers remain explicit unresolved/not-implemented
fields rather than being inferred from silence. The workflow persists the packets and
their compact prior snapshot so consecutive observations can be compared.

## Proposed next implementation: strength before expensive enrichment

1. Add a launchpad-specific adapter for the most productive *verified* launch mechanism. Resolve lifecycle and creator from authenticated event layouts and contract code, including internal factory deployments. Generic receipt.from is not proof of the token creator.
2. Spend follow-up budget first on transaction-level buyer attribution for fresh swaps. Resolve actual beneficial buyer/seller from transaction receipts and token transfers, exclude routers/LP/system contracts, and keep an UNKNOWN class. Repeated addresses and shared funding are evidence, not automatic proof of independent humans or sybils.
3. Maintain holder balances only from a verified deployment baseline with contiguous transfer coverage and supply reconciliation. Gaps, nonstandard token accounting or negative balances make holder totals unresolved. Verify largest balances at the observation block before using concentration.
4. Use protocol-specific reserves/positions and custody/lock controls for liquid depth and removal risk. Do not label v4 active-liquidity units as USD TVL. Require realistic executable depth or size-dependent quote and recent observed sells; successful sells alone do not certify future sellability.
5. Certify decimals, effective supply, mint/upgrade authority and price denomination. A direct USDG amount ratio is exposed as a quote observation, not canonical USD MC. Add verified USD conversion and freshness-controlled WETH references only when their provenance is established. Keep `MC UNRESOLVED` otherwise.
6. Compare equal-duration or explicitly time-normalized consecutive samples. Proposed Scout requires the safety gate plus independent-buyer growth, positive holder growth, retained liquidity and positive dollar-weighted buy flow across at least two qualified observations. Repeat buyers and improving concentration strengthen it; raw transactions and volume cannot qualify it alone. Confirmed requires another qualifying observation without new structural risk. Participation growth relative to valuation growth is a corroborating signal, not a substitute for safe supply control.
7. Promote validated acceleration to a priority follow-up lane with bounded reserved capacity. Current activity-based follow-up is not acceleration ranking. Only record missed 5m/15m/30m/45m/60m milestones as missed; never interpolate measurements or promise five-minute observations from a fifteen-minute scheduler.

Thresholds are proposals, not calibrated facts. Avoid turning a weighted score into permission to bypass missing structural checks. Unknown safety produces observation-only output; credible dangerous control produces hard rejection. Cheap discovery and observation continue in either case.

## Unavailable / unresolved

No verified effective-supply reconstruction, canonical USD MC, authoritative holder count, independent/repeat buyers, dollar-weighted end-user flow, linked-wallet analysis, executable depth, liquidity lock proof, honeypot/tax simulation, arbitrary mint/admin analysis, factory creator attribution/history, Fomo/community feed, or migration adapter is implemented here. There is no Scout/Confirmed emission path or notification sender. That is an explicit remaining implementation gap, not a working strength detector hidden behind a conservative score.

Historical early recognition of JACOB, Maple, EXAMCN, SIH and Fortune500 remains unverified. Required replay evidence: timestamped original discovery inputs, deployment/pool records, ordered observations, contemporaneously available risk flags, and first-public-visibility timestamps. Replay discovery blind, then join identities for evaluation. Measure discovery latency, first safe qualification time, false promotions, unresolved rate, RPC use and queue delay. A late Fomo flag cannot be treated as information available at SYNTH launch. The hard hold proves policy enforcement only; generic owner-control tests exercise a separate address and are not evidence of SYNTH's historical structure.

## Verification and rollout

Local deterministic tests exercise budget/circuit behavior, cursor failure and gap repair, queue rotation, >10,000 identities, stale/conflicting quotes, owner rejection, SYNTH hold and fixture isolation. A two-run offline integration serializes state and verifies that new activity continues from persisted cursors with explicit gaps.

The PR workflow runs those tests and a separate read-only public RPC probe (8 calls / 30 seconds). Probe artifacts report unresolved results even if the process exits successfully. Neither an offline test nor a green workflow proves historical runner recognition. Production scheduling, automatic state commits, sustained rotation, provider log completeness and finalized-tag compatibility require actual live workflow evidence after rollout. Keep this PR draft until those limits are reviewed. No production code is merged by creating this draft.
