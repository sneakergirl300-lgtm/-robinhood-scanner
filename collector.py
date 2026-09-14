#!/usr/bin/env python3
"""
Robinhood Chain discovery collector v7.

Discovery-first collector:
- ERC-20 zero-address mint logs
- known launchpad/factory corroboration
- targeted Uniswap v4 Initialize events (new markets)
- exact-address deduplication
- >=90-minute overlap on every normal run
- failed-range persistence/retry
- persistent multi-source observations per contract

No API key required.
"""

import json
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"

STATE = Path("state.json")
OUTPUT = Path("candidates.json")

# Minimum required discovery overlap.
FIRST_RUN_LOOKBACK_SECONDS = 90 * 60
OVERLAP_SECONDS = 90 * 60

# Keep individual eth_getLogs requests reasonably small.
MAX_BLOCKS_PER_QUERY = 500


FACTORIES = [
    # hood.fun
    "0x5fcc1df0dc020cf454e742e9a8ae2554c37a452c",

    # LaunchHood
    "0x62b33a039d289cbda50ebeb72fe4261449e61bcf",

    # Virtuals
    "0xd4ccbfa37e2f35611b3042e4096ad7a3459bd007",

    # Flap.sh
    "0x26605f322f7ff986f381bb9a6e3f5dab0beaeb09",

    # Klik Finance
    "0x16cf6788b762ee8969744586ed16fc5705140dd7",

    # Doppler
    "0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862",
    "0x22e99278308b393ea1260859b181ad7e78f5eeed",

    # Ape.store
    "0x6e4910ea5a04376032f6564da9a9e4e88b7a87c1",

    # Bags.fm
    "0xe8cc4431adf8b5a847c113ef0c6af9043219cb37",

    # Clanker
    "0xd3f2cc1731b7fd17f28798835c2e02f0a1839a94",

    # Pons V2
    "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e",
    "0xe33e9e479df8802cb0866d5d05258bec4cf62948",

    # pools.trade
    "0x0000ffffbe8efe702c8703ae3477ff5de3d319c0",
    "0x00004c4ccc709ef590f7c81102c0689f0263d4e9",

    # trench.today
    "0x77dc6f6361b7b99456fc3761ce5b7dda80d83f9d",
]


POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


IGNORE = set(a.lower() for a in FACTORIES) | {
    POOL_MANAGER.lower(),
    WETH.lower(),
    USDG.lower(),
}


TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)

ZERO_TOPIC = "0x" + "0" * 64

# Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)
V4_INITIALIZE_TOPIC = (
    "0xdd466e674ea557f56295e2d0218a125e"
    "a4b4f0f6f3307b95f85e6110838d6438"
)


def rpc(method, params, timeout=15):
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode()

    req = urllib.request.Request(
        RPC,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "robinhood-scanner/7.0",
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        obj = json.load(response)

    if "error" in obj:
        raise RuntimeError(obj["error"])

    return obj["result"]


def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def block_timestamp(block_number):
    block = rpc(
        "eth_getBlockByNumber",
        [hex(block_number), False],
    )

    return int(block["timestamp"], 16)


def find_block_at_or_before_timestamp(head, target_ts):
    """
    Binary-search the chain for the latest block whose timestamp is <= target_ts.
    """

    low = 0
    high = head

    while low < high:
        mid = (low + high + 1) // 2
        ts = block_timestamp(mid)

        if ts <= target_ts:
            low = mid
        else:
            high = mid - 1

    return low


def normalize_address(value):
    if not isinstance(value, str):
        return None

    raw = value.lower()

    if raw.startswith("0x"):
        raw = raw[2:]

    if len(raw) != 40:
        return None

    try:
        int(raw, 16)
    except ValueError:
        return None

    if raw == "0" * 40:
        return None

    return "0x" + raw


def topic_address(topic):
    """
    Decode an indexed address topic.
    """

    if not isinstance(topic, str):
        return None

    raw = topic[2:] if topic.startswith("0x") else topic

    if len(raw) != 64:
        return None

    if raw[:24] != "0" * 24:
        return None

    return normalize_address(raw[-40:])


def merge_ranges(ranges):
    """
    Normalize and merge overlapping/adjacent block ranges.

    Input/output format:
    [
        {"start": 123, "end": 456},
        ...
    ]
    """

    cleaned = []

    for row in ranges or []:
        try:
            start = int(row["start"])
            end = int(row["end"])
        except (KeyError, TypeError, ValueError):
            continue

        if start > end:
            start, end = end, start

        cleaned.append((start, end))

    cleaned.sort()

    merged = []

    for start, end in cleaned:
        if not merged or start > merged[-1][1] + 1:
            merged.append([start, end])
        else:
            merged[-1][1] = max(
                merged[-1][1],
                end,
            )

    return [
        {
            "start": start,
            "end": end,
        }
        for start, end in merged
    ]


def scan_logs(base_filter, ranges, label):
    """
    Query eth_getLogs over every requested range.

    Crucially, failed sub-ranges are returned instead of being silently
    forgotten.
    """

    rows = []
    failed = []

    requested = merge_ranges(ranges)

    for requested_range in requested:
        current = requested_range["start"]
        end = requested_range["end"]

        while current <= end:
            to_block = min(
                end,
                current + MAX_BLOCKS_PER_QUERY - 1,
            )

            query_filter = dict(base_filter)

            query_filter["fromBlock"] = hex(current)
            query_filter["toBlock"] = hex(to_block)

            try:
                rows.extend(
                    rpc(
                        "eth_getLogs",
                        [query_filter],
                        timeout=20,
                    )
                )

            except Exception as exc:
                failed.append(
                    {
                        "start": current,
                        "end": to_block,
                    }
                )

                print(
                    f"WARNING: {label} failed range "
                    f"{current}-{to_block}: {exc}"
                )

            current = to_block + 1

    return rows, merge_ranges(failed), requested


def observation_from_log(log, source):
    return {
        "block": int(log["blockNumber"], 16),
        "tx": log.get("transactionHash"),
        "source": source,
        "observed_at_unix": int(time.time()),
    }


def observation_key(obs):
    return (
        int(obs.get("block", 0)),
        str(obs.get("tx") or "").lower(),
        str(obs.get("source") or ""),
    )


def classify_candidate(candidate):
    """
    Important distinction:

    zero mint / factory-backed mint
        => launch evidence

    v4 Initialize only
        => new-market evidence, NOT proof of token birth

    More sophisticated age/reactivation classification belongs in the
    next enrichment stage once contract-creation history is available.
    """

    sources = set(candidate.get("sources") or [])

    has_factory_launch = any(
        source.startswith("factory_mint:")
        for source in sources
    )

    has_zero_mint = (
        "zero_address_mint" in sources
    )

    has_new_market = (
        "uniswap_v4_initialize" in sources
    )

    if has_factory_launch:
        candidate["classification"] = "NEW_LAUNCH"
        candidate["classification_confidence"] = "HIGH"

    elif has_zero_mint:
        candidate["classification"] = "NEW_LAUNCH"
        candidate["classification_confidence"] = "MEDIUM"

    elif has_new_market:
        candidate["classification"] = "NEW_MARKET"
        candidate["classification_confidence"] = "MEDIUM"

    else:
        candidate["classification"] = "UNRESOLVED"
        candidate["classification_confidence"] = "LOW"


def candidate_from_legacy(row):
    """
    Upgrade an existing v6-style candidate to the v7 schema.

    Existing fields are preserved for backwards compatibility:
    - address
    - first_seen_block
    - tx
    - source
    - collected_at_unix
    """

    address = normalize_address(
        row.get("address")
    )

    if not address:
        return None

    first_block = row.get(
        "first_seen_block"
    )

    source = str(
        row.get("source") or "unknown"
    )

    tx = row.get("tx")

    collected = int(
        row.get("collected_at_unix")
        or time.time()
    )

    observations = row.get(
        "observations"
    )

    if not isinstance(
        observations,
        list,
    ):
        observations = []

    if (
        first_block is not None
        and not observations
    ):
        observations = [
            {
                "block": int(first_block),
                "tx": tx,
                "source": source,
                "observed_at_unix": collected,
            }
        ]

    sources = row.get("sources")

    if not isinstance(sources, list):
        sources = []

    if (
        source
        and source not in sources
    ):
        sources.append(source)

    for obs in observations:
        obs_source = str(
            obs.get("source") or ""
        )

        if (
            obs_source
            and obs_source not in sources
        ):
            sources.append(obs_source)

    blocks = [
        int(obs["block"])
        for obs in observations
        if obs.get("block") is not None
    ]

    if first_block is not None:
        blocks.append(
            int(first_block)
        )

    first_seen_block = (
        min(blocks)
        if blocks
        else 0
    )

    last_seen_block = (
        max(blocks)
        if blocks
        else first_seen_block
    )

    candidate = {
        "address": address,

        # Backwards-compatible primary discovery fields.
        "first_seen_block": first_seen_block,
        "tx": tx,
        "source": source,
        "collected_at_unix": collected,

        # v7 persistent state.
        "last_seen_block": int(
            row.get("last_seen_block")
            or last_seen_block
        ),

        "sources": sorted(
            set(sources)
        ),

        "observations": observations,
    }

    classify_candidate(candidate)

    return candidate


def merge_observation(
    candidate,
    observation,
):
    """
    Attach a new observation to an existing exact-contract object.
    """

    existing_keys = {
        observation_key(obs)
        for obs in candidate.get(
            "observations",
            [],
        )
        if isinstance(obs, dict)
    }

    key = observation_key(
        observation
    )

    if key not in existing_keys:
        candidate.setdefault(
            "observations",
            [],
        ).append(observation)

    source = observation["source"]

    sources = set(
        candidate.get("sources")
        or []
    )

    sources.add(source)

    candidate["sources"] = sorted(
        sources
    )

    block = int(
        observation["block"]
    )

    candidate["last_seen_block"] = max(
        int(
            candidate.get(
                "last_seen_block"
            )
            or block
        ),
        block,
    )

    first_block = candidate.get(
        "first_seen_block"
    )

    if (
        first_block is None
        or block < int(first_block)
    ):
        candidate["first_seen_block"] = block
        candidate["tx"] = observation.get(
            "tx"
        )
        candidate["source"] = source
        candidate["collected_at_unix"] = (
            observation[
                "observed_at_unix"
            ]
        )

    classify_candidate(candidate)


def add(
    found,
    address,
    log,
    source,
):
    address = normalize_address(
        address
    )

    if (
        not address
        or address in IGNORE
    ):
        return

    observation = (
        observation_from_log(
            log,
            source,
        )
    )

    candidate = found.get(
        address
    )

    if candidate is None:
        candidate = {
            "address": address,

            "first_seen_block":
                observation["block"],

            "tx":
                observation.get("tx"),

            "source":
                source,

            "collected_at_unix":
                observation[
                    "observed_at_unix"
                ],

            "last_seen_block":
                observation["block"],

            "sources": [],

            "observations": [],
        }

        found[address] = candidate

    merge_observation(
        candidate,
        observation,
    )


def clean_existing(rows):
    """
    Preserve legitimate historical rows while continuing to remove the
    old PoolManager pollution from the earlier collector version.
    """

    keep = []

    for row in rows:
        source = str(
            row.get("source", "")
        )

        sources = (
            row.get("sources")
            or []
        )

        valid = (
            source
            == "zero_address_mint"

            or source.startswith(
                "factory_mint:"
            )

            or source
            == "uniswap_v4_initialize"

            or "zero_address_mint"
            in sources

            or "uniswap_v4_initialize"
            in sources

            or any(
                str(s).startswith(
                    "factory_mint:"
                )
                for s in sources
            )
        )

        if not valid:
            continue

        migrated = candidate_from_legacy(
            row
        )

        if migrated:
            keep.append(
                migrated
            )

    return keep


def current_and_retry_ranges(
    state,
    source_key,
    start,
    head,
):
    """
    Scan both:

    1. this run's normal >=90-minute overlap
    2. any failed block ranges carried over from the previous run
    """

    state_failed = state.get(
        "failed_ranges"
    )

    if isinstance(
        state_failed,
        dict,
    ):
        prior = state_failed.get(
            source_key,
            [],
        )
    else:
        prior = []

    return merge_ranges(
        prior
        + [
            {
                "start": start,
                "end": head,
            }
        ]
    )


def main():
    started = time.time()

    head = int(
        rpc(
            "eth_blockNumber",
            [],
        ),
        16,
    )

    head_ts = block_timestamp(
        head
    )

    state = load_json(
        STATE,
        {},
    )

    if (
        "last_block_timestamp"
        in state
    ):
        target_ts = max(
            0,
            int(
                state[
                    "last_block_timestamp"
                ]
            )
            - OVERLAP_SECONDS,
        )

        start = (
            find_block_at_or_before_timestamp(
                head,
                target_ts,
            )
        )

        mode = "overlap_90m"

    else:
        target_ts = max(
            0,
            head_ts
            - FIRST_RUN_LOOKBACK_SECONDS,
        )

        start = (
            find_block_at_or_before_timestamp(
                head,
                target_ts,
            )
        )

        mode = "baseline_90m"

    start = min(
        start,
        head,
    )

    start_ts = block_timestamp(
        start
    )

    print(
        f"Mode: {mode}"
    )

    print(
        "Scanning current window blocks "
        f"{start}-{head}"
    )

    print(
        "Window seconds: "
        f"{head_ts - start_ts}"
    )

    found = {}

    # ------------------------------------------------------------
    # 1. ERC-20 zero-address mint discovery
    # ------------------------------------------------------------

    mint_ranges = (
        current_and_retry_ranges(
            state,
            "zero_mint",
            start,
            head,
        )
    )

    (
        mint_logs,
        mint_failed,
        mint_requested,
    ) = scan_logs(
        {
            "topics": [
                TRANSFER_TOPIC,
                ZERO_TOPIC,
            ]
        },
        mint_ranges,
        "zero_mint",
    )

    erc20_mint_logs = []

    for log in mint_logs:
        topics = (
            log.get("topics")
            or []
        )

        data = (
            log.get("data")
            or "0x"
        )

        raw_data = (
            data[2:]
            if data.startswith("0x")
            else data
        )

        # ERC-20 Transfer:
        # topic0 = signature
        # topic1 = from
        # topic2 = to
        # data   = value
        #
        # ERC-721 normally uses four topics.
        if (
            len(topics) == 3
            and len(raw_data) == 64
        ):
            erc20_mint_logs.append(
                log
            )

            add(
                found,
                log.get("address"),
                log,
                "zero_address_mint",
            )

    print(
        "Zero-mint logs: "
        f"{len(mint_logs)}"
    )

    print(
        "ERC20-shaped zero-mint logs: "
        f"{len(erc20_mint_logs)}"
    )

    # ------------------------------------------------------------
    # 2. Known launchpad / factory activity
    # ------------------------------------------------------------

    factory_ranges = (
        current_and_retry_ranges(
            state,
            "factory",
            start,
            head,
        )
    )

    (
        factory_logs,
        factory_failed,
        factory_requested,
    ) = scan_logs(
        {
            "address": FACTORIES
        },
        factory_ranges,
        "factory",
    )

    # Map zero-mint tokens by transaction so factory activity can
    # corroborate the launch without guessing arbitrary event words.
    mint_by_tx = {}

    for log in erc20_mint_logs:
        tx = log.get(
            "transactionHash"
        )

        token = normalize_address(
            log.get("address")
        )

        if tx and token:
            mint_by_tx.setdefault(
                tx.lower(),
                [],
            ).append(
                (
                    token,
                    log,
                )
            )

    corroborated_factory_tokens = 0

    for log in factory_logs:
        tx = (
            log.get(
                "transactionHash"
            )
            or ""
        ).lower()

        emitter = normalize_address(
            log.get("address")
        )

        if not tx or not emitter:
            continue

        for (
            token,
            mint_log,
        ) in mint_by_tx.get(
            tx,
            [],
        ):
            add(
                found,
                token,
                mint_log,
                (
                    "factory_mint:"
                    f"{emitter}"
                ),
            )

            corroborated_factory_tokens += 1

    print(
        "Factory logs: "
        f"{len(factory_logs)}"
    )

    print(
        "Factory-corroborated "
        "token mints: "
        f"{corroborated_factory_tokens}"
    )

    # ------------------------------------------------------------
    # 3. Uniswap v4 Initialize discovery
    # ------------------------------------------------------------

    v4_ranges = (
        current_and_retry_ranges(
            state,
            "uniswap_v4_initialize",
            start,
            head,
        )
    )

    (
        init_logs,
        v4_failed,
        v4_requested,
    ) = scan_logs(
        {
            "address":
                POOL_MANAGER,

            "topics": [
                V4_INITIALIZE_TOPIC
            ],
        },
        v4_ranges,
        "uniswap_v4_initialize",
    )

    for log in init_logs:
        topics = (
            log.get("topics")
            or []
        )

        # Initialize:
        #
        # topics[1] = poolId
        # topics[2] = currency0
        # topics[3] = currency1

        if len(topics) < 4:
            continue

        currency0 = topic_address(
            topics[2]
        )

        currency1 = topic_address(
            topics[3]
        )

        if currency0:
            add(
                found,
                currency0,
                log,
                "uniswap_v4_initialize",
            )

        if currency1:
            add(
                found,
                currency1,
                log,
                "uniswap_v4_initialize",
            )

    print(
        "Uniswap v4 Initialize logs: "
        f"{len(init_logs)}"
    )

    # ------------------------------------------------------------
    # 4. Merge with persistent candidate universe
    # ------------------------------------------------------------

    existing_raw = load_json(
        OUTPUT,
        [],
    )

    existing = clean_existing(
        existing_raw
    )

    removed_polluted = (
        len(existing_raw)
        - len(existing)
    )

    by_address = {}

    for row in existing:
        address = normalize_address(
            row.get("address")
        )

        if address:
            by_address[address] = row

    new_count = 0
    updated_count = 0

    for (
        address,
        incoming,
    ) in found.items():

        if address not in by_address:
            by_address[address] = (
                incoming
            )

            new_count += 1
            continue

        existing_candidate = (
            by_address[address]
        )

        before = {
            observation_key(obs)
            for obs
            in existing_candidate.get(
                "observations",
                [],
            )
            if isinstance(obs, dict)
        }

        for observation in incoming.get(
            "observations",
            [],
        ):
            merge_observation(
                existing_candidate,
                observation,
            )

        after = {
            observation_key(obs)
            for obs
            in existing_candidate.get(
                "observations",
                [],
            )
            if isinstance(obs, dict)
        }

        if after != before:
            updated_count += 1

    rows = sorted(
        by_address.values(),
        key=lambda row: (
            row.get(
                "first_seen_block",
                0,
            ),
            row.get(
                "address",
                "",
            ),
        ),
    )

    # IMPORTANT:
    #
    # v6 used:
    #
    #     rows[-10000:]
    #
    # which silently evicted the oldest candidate contracts.
    #
    # v7 persists the entire deduplicated universe.
    OUTPUT.write_text(
        json.dumps(
            rows,
            indent=2,
        )
        + "\n"
    )

    # ------------------------------------------------------------
    # 5. Reporting
    # ------------------------------------------------------------

    source_contracts = {}

    classification_counts = {}

    for candidate in found.values():

        classification = (
            candidate.get(
                "classification",
                "UNRESOLVED",
            )
        )

        classification_counts[
            classification
        ] = (
            classification_counts.get(
                classification,
                0,
            )
            + 1
        )

        for source in candidate.get(
            "sources",
            [],
        ):
            source_contracts.setdefault(
                source,
                set(),
            ).add(
                candidate["address"]
            )

    source_counts = {
        source: len(addresses)
        for (
            source,
            addresses,
        ) in sorted(
            source_contracts.items()
        )
    }

    failed_ranges = {
        "zero_mint":
            mint_failed,

        "factory":
            factory_failed,

        "uniswap_v4_initialize":
            v4_failed,
    }

    # Do not clutter state.json with empty lists.
    failed_ranges = {
        key: value
        for (
            key,
            value,
        ) in failed_ranges.items()
        if value
    }

    # This refers specifically to the collector's own raw RPC
    # primitives. It does NOT claim that every possible Robinhood
    # launch mechanism has been independently covered.
    coverage_status = (
        "COMPLETE"
        if not failed_ranges
        else "PARTIAL"
    )

    state_out = {
        "collector_version": 7,

        "last_block": head,

        "last_block_timestamp":
            head_ts,

        "scan_start": start,

        "scan_start_timestamp":
            start_ts,

        "scan_end": head,

        "scan_end_timestamp":
            head_ts,

        "scan_window_seconds":
            head_ts - start_ts,

        "required_overlap_seconds":
            OVERLAP_SECONDS,

        "mode": mode,

        "coverage_status":
            coverage_status,

        "coverage_complete":
            not bool(
                failed_ranges
            ),

        "requested_ranges": {
            "zero_mint":
                mint_requested,

            "factory":
                factory_requested,

            "uniswap_v4_initialize":
                v4_requested,
        },

        "failed_ranges":
            failed_ranges,

        "zero_mint_logs":
            len(mint_logs),

        "erc20_zero_mint_logs":
            len(
                erc20_mint_logs
            ),

        "factory_logs":
            len(factory_logs),

        "factory_corroborated_token_mints":
            corroborated_factory_tokens,

        "uniswap_v4_initialize_logs":
            len(init_logs),

        "events_seen":
            (
                len(mint_logs)
                + len(factory_logs)
                + len(init_logs)
            ),

        "unique_contracts_this_run":
            len(found),

        # Backwards-compatible field.
        "unique_candidates_this_run":
            len(found),

        "candidate_source_counts":
            source_counts,

        "classification_counts":
            classification_counts,

        "new_contracts_added":
            new_count,

        # Backwards-compatible field.
        "new_candidates":
            new_count,

        "existing_contracts_updated":
            updated_count,

        "stored_candidates":
            len(rows),

        "total_persisted_contracts":
            len(rows),

        "polluted_rows_removed":
            removed_polluted,

        "runtime_seconds":
            round(
                time.time()
                - started,
                2,
            ),

        "updated_at_unix":
            int(time.time()),

        "rpc":
            RPC,
    }

    STATE.write_text(
        json.dumps(
            state_out,
            indent=2,
        )
        + "\n"
    )

    print(
        "Coverage status: "
        f"{coverage_status}"
    )

    print(
        "Outstanding failed ranges: "
        f"{failed_ranges}"
    )

    print(
        "Removed polluted old rows: "
        f"{removed_polluted}"
    )

    print(
        "New contracts: "
        f"{new_count}"
    )

    print(
        "Existing contracts updated: "
        f"{updated_count}"
    )

    print(
        "Stored candidates: "
        f"{len(rows)}"
    )

    print(
        "Runtime: "
        f"{state_out['runtime_seconds']} "
        "seconds"
    )


if __name__ == "__main__":
    main()
