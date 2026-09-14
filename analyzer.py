#!/usr/bin/env python3
"""
Robinhood Chain Part 2 analyzer v1.

Incremental/raw-RPC enrichment for the hottest Part 1 candidates.

What this version measures directly:
- ERC-20 decimals / totalSupply / optional name+symbol / optional owner()
- recent Uniswap v4 pools involving the candidate
- fresh v4 executed-swap price
- USD price/market-cap when the candidate is directly quoted in USDG,
  or quoted in WETH while a fresh WETH/USDG v4 reference exists
- recent swap count and signed token/quote flow
- transfer-derived holder balances for very recent tokens
- persisted snapshots for trajectory analysis

What this version deliberately DOES NOT fake:
- USD TVL from Uniswap v4's raw active-liquidity integer
- end-user unique buyers from PoolManager.sender (often a router)
- honeypot/sellability simulation
- linked-wallet/sybil clustering
- mint/admin privilege proof for arbitrary token ABIs

Those fields are reported as unresolved until protocol-specific checks are added.
"""

import json
import math
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"

CANDIDATES = Path("candidates.json")
COLLECTOR_STATE = Path("state.json")
ANALYSIS_STATE = Path("analysis_state.json")
LATEST = Path("latest_analysis.json")

POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
V4_INITIALIZE_TOPIC = (
    "0xdd466e674ea557f56295e2d0218a125e"
    "a4b4f0f6f3307b95f85e6110838d6438"
)
V4_SWAP_TOPIC = (
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7e"
    "e92e57ba5563708daca94dd84ad7112f"
)

# Standard ERC-20 / Ownable selectors.
SEL_DECIMALS = "0x313ce567"
SEL_TOTAL_SUPPLY = "0x18160ddd"
SEL_SYMBOL = "0x95d89b41"
SEL_NAME = "0x06fdde03"
SEL_OWNER = "0x8da5cb5b"
SEL_BALANCE_OF = "0x70a08231"

# Keep Part 2 bounded. Part 1 remains the broad, persistent universe.
HOT_LIMIT = 220
FIRST_RUN_LOOKBACK_SECONDS = 3 * 60 * 60
RECURRING_OVERLAP_SECONDS = 20 * 60
MAX_BLOCKS_PER_QUERY = 500
MAX_SNAPSHOTS_PER_TOKEN = 20
PRICE_MAX_AGE_SECONDS = 30 * 60


def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def rpc(method, params, timeout=20):
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }).encode()

    req = urllib.request.Request(
        RPC,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "robinhood-scanner-analyzer/1.0",
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        obj = json.load(response)

    if "error" in obj:
        raise RuntimeError(obj["error"])

    return obj["result"]


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
    if not isinstance(topic, str):
        return None
    raw = topic[2:] if topic.startswith("0x") else topic
    if len(raw) != 64:
        return None
    return normalize_address(raw[-40:])


def block_timestamp(block_number):
    block = rpc("eth_getBlockByNumber", [hex(block_number), False])
    return int(block["timestamp"], 16)


def find_block_at_or_before_timestamp(head, target_ts):
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


def merge_ranges(ranges):
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
            merged[-1][1] = max(merged[-1][1], end)

    return [{"start": s, "end": e} for s, e in merged]


def get_logs(base_filter, ranges, label):
    rows = []
    failed = []
    for block_range in merge_ranges(ranges):
        current = block_range["start"]
        end = block_range["end"]

        while current <= end:
            to_block = min(end, current + MAX_BLOCKS_PER_QUERY - 1)
            filt = dict(base_filter)
            filt["fromBlock"] = hex(current)
            filt["toBlock"] = hex(to_block)

            try:
                rows.extend(rpc("eth_getLogs", [filt], timeout=25))
            except Exception as exc:
                print(f"WARNING: {label} failed {current}-{to_block}: {exc}")
                failed.append({"start": current, "end": to_block})

            current = to_block + 1

    return rows, merge_ranges(failed)


def eth_call(address, data):
    try:
        return rpc("eth_call", [{"to": address, "data": data}, "latest"])
    except Exception:
        return None


def uint_from_hex(value):
    try:
        return int(value, 16)
    except Exception:
        return None


def decode_abi_string(value):
    if not value or value == "0x":
        return None
    raw = value[2:] if value.startswith("0x") else value
    try:
        # bytes32-style return
        if len(raw) == 64:
            b = bytes.fromhex(raw).rstrip(b"\x00")
            return b.decode("utf-8", errors="replace") or None

        # Standard ABI dynamic string:
        # [offset][length][data...]
        if len(raw) >= 128:
            length = int(raw[64:128], 16)
            payload = raw[128:128 + length * 2]
            return bytes.fromhex(payload).decode("utf-8", errors="replace")
    except Exception:
        return None
    return None


def signed_word(word, bits=256):
    value = int(word, 16)
    if value >= 1 << (bits - 1):
        value -= 1 << bits
    return value


def word_chunks(data):
    raw = data[2:] if data.startswith("0x") else data
    return [raw[i:i + 64] for i in range(0, len(raw), 64) if len(raw[i:i + 64]) == 64]


def candidate_rank(candidate):
    return max(
        int(candidate.get("last_seen_block") or 0),
        int(candidate.get("first_seen_block") or 0),
    )


def select_hot_candidates(candidates, previous_tokens):
    valid = []
    for row in candidates:
        address = normalize_address(row.get("address"))
        if not address:
            continue

        classification = row.get("classification", "UNRESOLVED")
        if classification not in {"NEW_LAUNCH", "NEW_MARKET", "UNRESOLVED"}:
            continue

        item = dict(row)
        item["address"] = address
        valid.append(item)

    valid.sort(key=candidate_rank, reverse=True)

    selected = valid[:HOT_LIMIT]
    selected_addresses = {row["address"] for row in selected}

    # Preserve still-hot previous tokens if space remains.
    if len(selected) < HOT_LIMIT:
        by_address = {row["address"]: row for row in valid}
        for address, token_state in previous_tokens.items():
            if len(selected) >= HOT_LIMIT:
                break
            if address in selected_addresses:
                continue
            if token_state.get("status") in {"ACTIVE", "WATCH", "UNRESOLVED"}:
                row = by_address.get(address)
                if row:
                    selected.append(row)
                    selected_addresses.add(address)

    return selected


def token_metadata(address):
    decimals_raw = eth_call(address, SEL_DECIMALS)
    supply_raw = eth_call(address, SEL_TOTAL_SUPPLY)
    symbol_raw = eth_call(address, SEL_SYMBOL)
    name_raw = eth_call(address, SEL_NAME)
    owner_raw = eth_call(address, SEL_OWNER)

    decimals = uint_from_hex(decimals_raw)
    total_supply_raw = uint_from_hex(supply_raw)
    owner = None

    if owner_raw and owner_raw != "0x":
        raw = owner_raw[2:] if owner_raw.startswith("0x") else owner_raw
        if len(raw) >= 64:
            owner = normalize_address(raw[-40:])

    if decimals is not None and decimals > 36:
        decimals = None

    total_supply = None
    if decimals is not None and total_supply_raw is not None:
        total_supply = total_supply_raw / (10 ** decimals)

    owner_balance_raw = None
    owner_supply_share = None

    if owner and total_supply_raw:
        arg = owner[2:].rjust(64, "0")
        owner_balance_hex = eth_call(address, SEL_BALANCE_OF + arg)
        owner_balance_raw = uint_from_hex(owner_balance_hex)
        if owner_balance_raw is not None:
            owner_supply_share = owner_balance_raw / total_supply_raw

    code = rpc("eth_getCode", [address, "latest"])
    code_bytes = max(0, (len(code) - 2) // 2) if isinstance(code, str) else None

    return {
        "decimals": decimals,
        "total_supply_raw": total_supply_raw,
        "total_supply": total_supply,
        "symbol": decode_abi_string(symbol_raw),
        "name": decode_abi_string(name_raw),
        "owner": owner,
        "owner_balance_raw": owner_balance_raw,
        "owner_supply_share": owner_supply_share,
        "code_bytes": code_bytes,
    }


def decode_initialize(log):
    topics = log.get("topics") or []
    words = word_chunks(log.get("data") or "0x")

    if len(topics) < 4 or len(words) < 5:
        return None

    currency0 = topic_address(topics[2])
    currency1 = topic_address(topics[3])

    if not currency0 or not currency1:
        return None

    return {
        "pool_id": topics[1].lower(),
        "currency0": currency0,
        "currency1": currency1,
        "fee": int(words[0], 16),
        "tick_spacing": signed_word(words[1]),
        "hooks": normalize_address(words[2][-40:]),
        "sqrt_price_x96_initial": int(words[3], 16),
        "tick_initial": signed_word(words[4]),
        "initialize_block": int(log["blockNumber"], 16),
        "initialize_tx": log.get("transactionHash"),
    }


def decode_swap(log):
    topics = log.get("topics") or []
    words = word_chunks(log.get("data") or "0x")

    if len(topics) < 3 or len(words) < 6:
        return None

    return {
        "pool_id": topics[1].lower(),
        "sender": topic_address(topics[2]),
        "amount0_raw": signed_word(words[0]),
        "amount1_raw": signed_word(words[1]),
        "sqrt_price_x96": int(words[2], 16),
        "active_liquidity_raw": int(words[3], 16),
        "tick": signed_word(words[4]),
        "fee": int(words[5], 16),
        "block": int(log["blockNumber"], 16),
        "tx": log.get("transactionHash"),
        "log_index": int(log.get("logIndex", "0x0"), 16),
    }


def price_currency1_per_currency0(sqrt_price_x96, decimals0, decimals1):
    if sqrt_price_x96 <= 0:
        return None
    raw_ratio = (sqrt_price_x96 / (2 ** 96)) ** 2
    return raw_ratio * (10 ** (decimals0 - decimals1))


def pool_candidate_price_usd(pool, swap, candidate, metadata_by_address, weth_usd):
    c0 = pool["currency0"]
    c1 = pool["currency1"]

    d0 = metadata_by_address.get(c0, {}).get("decimals")
    d1 = metadata_by_address.get(c1, {}).get("decimals")

    if d0 is None or d1 is None:
        return None, None

    ratio_1_per_0 = price_currency1_per_currency0(
        swap["sqrt_price_x96"], d0, d1
    )

    if not ratio_1_per_0 or ratio_1_per_0 <= 0:
        return None, None

    if candidate == c0:
        quote = c1
        quote_per_token = ratio_1_per_0
    elif candidate == c1:
        quote = c0
        quote_per_token = 1 / ratio_1_per_0
    else:
        return None, None

    if quote == USDG:
        return quote_per_token, "USDG"

    if quote == WETH and weth_usd:
        return quote_per_token * weth_usd, "WETH->USDG"

    return None, None


def derive_weth_usd(pools, latest_swap_by_pool, metadata_by_address):
    choices = []

    for pool_id, pool in pools.items():
        if {pool["currency0"], pool["currency1"]} != {WETH, USDG}:
            continue

        swap = latest_swap_by_pool.get(pool_id)
        if not swap:
            continue

        d0 = metadata_by_address.get(pool["currency0"], {}).get("decimals")
        d1 = metadata_by_address.get(pool["currency1"], {}).get("decimals")

        if d0 is None or d1 is None:
            continue

        ratio = price_currency1_per_currency0(
            swap["sqrt_price_x96"], d0, d1
        )
        if not ratio:
            continue

        if pool["currency0"] == WETH:
            weth_usd = ratio
        else:
            weth_usd = 1 / ratio

        choices.append((swap["block"], weth_usd, pool_id))

    if not choices:
        return None, None

    choices.sort(reverse=True)
    _, price, pool_id = choices[0]
    return price, pool_id


def transfer_state_for_hot(hot_addresses, history_start, head):
    balances = {address: {} for address in hot_addresses}
    stats = {
        address: {
            "transfer_events": 0,
            "unique_transfer_addresses": set(),
        }
        for address in hot_addresses
    }

    if not hot_addresses:
        return balances, stats, []

    logs, failed = get_logs(
        {
            "address": sorted(hot_addresses),
            "topics": [TRANSFER_TOPIC],
        },
        [{"start": history_start, "end": head}],
        "hot_transfers",
    )

    zero = "0x" + "0" * 40

    for log in logs:
        token = normalize_address(log.get("address"))
        topics = log.get("topics") or []
        words = word_chunks(log.get("data") or "0x")

        if token not in balances or len(topics) != 3 or not words:
            continue

        from_addr = topic_address(topics[1]) or zero
        to_addr = topic_address(topics[2]) or zero
        value = int(words[0], 16)

        token_balances = balances[token]

        if from_addr != zero:
            token_balances[from_addr] = token_balances.get(from_addr, 0) - value
            stats[token]["unique_transfer_addresses"].add(from_addr)

        if to_addr != zero:
            token_balances[to_addr] = token_balances.get(to_addr, 0) + value
            stats[token]["unique_transfer_addresses"].add(to_addr)

        stats[token]["transfer_events"] += 1

    for address in stats:
        stats[address]["unique_transfer_addresses"] = len(
            stats[address]["unique_transfer_addresses"]
        )

    return balances, stats, failed


def snapshot_trends(snapshots):
    if len(snapshots) < 2:
        return {
            "mc_change_pct": None,
            "holder_change": None,
            "swap_count_change": None,
        }

    newest = snapshots[-1]
    older = snapshots[-2]

    mc_new = newest.get("market_cap_usd")
    mc_old = older.get("market_cap_usd")

    mc_change_pct = None
    if mc_new is not None and mc_old not in (None, 0):
        mc_change_pct = (mc_new / mc_old - 1) * 100

    holders_new = newest.get("holder_count_recent")
    holders_old = older.get("holder_count_recent")

    holder_change = None
    if holders_new is not None and holders_old is not None:
        holder_change = holders_new - holders_old

    swaps_new = newest.get("swap_count_window")
    swaps_old = older.get("swap_count_window")

    swap_change = None
    if swaps_new is not None and swaps_old is not None:
        swap_change = swaps_new - swaps_old

    return {
        "mc_change_pct": mc_change_pct,
        "holder_change": holder_change,
        "swap_count_change": swap_change,
    }


def main():
    started = time.time()

    candidates = load_json(CANDIDATES, [])
    collector_state = load_json(COLLECTOR_STATE, {})
    previous = load_json(
        ANALYSIS_STATE,
        {
            "version": 1,
            "tokens": {},
            "pools": {},
            "failed_ranges": {},
        },
    )

    if not isinstance(candidates, list):
        raise RuntimeError("candidates.json is malformed")

    head = int(rpc("eth_blockNumber", []), 16)
    head_ts = block_timestamp(head)

    hot = select_hot_candidates(
        candidates,
        previous.get("tokens", {}),
    )
    hot_addresses = {row["address"] for row in hot}

    # Initial run backfills 3h. Recurring runs keep a 20m overlap.
    last_block = previous.get("last_block")
    if last_block:
        overlap_ts = max(0, int(previous.get("last_block_timestamp", head_ts)) - RECURRING_OVERLAP_SECONDS)
        history_start = find_block_at_or_before_timestamp(head, overlap_ts)
        mode = "incremental"
    else:
        history_start = find_block_at_or_before_timestamp(
            head,
            max(0, head_ts - FIRST_RUN_LOOKBACK_SECONDS),
        )
        mode = "baseline_3h"

    # Carry retry gaps forward.
    previous_failed = previous.get("failed_ranges") or {}

    pool_ranges = previous_failed.get("pool_events", []) + [
        {"start": history_start, "end": head}
    ]

    init_logs, init_failed = get_logs(
        {
            "address": POOL_MANAGER,
            "topics": [V4_INITIALIZE_TOPIC],
        },
        pool_ranges,
        "v4_initialize",
    )

    pools = dict(previous.get("pools") or {})

    for log in init_logs:
        decoded = decode_initialize(log)
        if decoded:
            pools[decoded["pool_id"]] = decoded

    # Keep pools relevant to hot candidates plus WETH/USDG reference pools.
    relevant_pool_ids = set()
    for pool_id, pool in pools.items():
        currencies = {pool["currency0"], pool["currency1"]}
        if currencies & hot_addresses or currencies == {WETH, USDG}:
            relevant_pool_ids.add(pool_id)

    swap_logs, swap_failed = get_logs(
        {
            "address": POOL_MANAGER,
            "topics": [V4_SWAP_TOPIC],
        },
        pool_ranges,
        "v4_swap",
    )

    swaps_by_pool = {}
    latest_swap_by_pool = {}

    for log in swap_logs:
        decoded = decode_swap(log)
        if not decoded:
            continue

        pool_id = decoded["pool_id"]
        if pool_id not in relevant_pool_ids:
            continue

        swaps_by_pool.setdefault(pool_id, []).append(decoded)

        old = latest_swap_by_pool.get(pool_id)
        if (
            old is None
            or (decoded["block"], decoded["log_index"])
            > (old["block"], old["log_index"])
        ):
            latest_swap_by_pool[pool_id] = decoded

    # Fetch metadata only for hot tokens and the canonical quote assets.
    metadata_by_address = {}

    for address in sorted(hot_addresses | {WETH, USDG}):
        try:
            metadata_by_address[address] = token_metadata(address)
        except Exception as exc:
            print(f"WARNING: metadata failed for {address}: {exc}")
            metadata_by_address[address] = {
                "decimals": None,
                "total_supply_raw": None,
                "total_supply": None,
                "symbol": None,
                "name": None,
                "owner": None,
                "owner_balance_raw": None,
                "owner_supply_share": None,
                "code_bytes": None,
            }

    weth_usd, weth_usd_pool = derive_weth_usd(
        pools,
        latest_swap_by_pool,
        metadata_by_address,
    )

    balances, transfer_stats, transfer_failed = transfer_state_for_hot(
        hot_addresses,
        history_start,
        head,
    )

    block_ts_cache = {}

    def ts_for_block(block_number):
        if block_number not in block_ts_cache:
            block_ts_cache[block_number] = block_timestamp(block_number)
        return block_ts_cache[block_number]

    latest_rows = []
    token_state = dict(previous.get("tokens") or {})

    for candidate in hot:
        address = candidate["address"]
        metadata = metadata_by_address[address]

        token_pools = []
        freshest = None

        for pool_id, pool in pools.items():
            if address not in {pool["currency0"], pool["currency1"]}:
                continue

            latest_swap = latest_swap_by_pool.get(pool_id)
            swap_count = len(swaps_by_pool.get(pool_id, []))

            entry = {
                "pool_id": pool_id,
                "currency0": pool["currency0"],
                "currency1": pool["currency1"],
                "initialize_block": pool["initialize_block"],
                "swap_count_window": swap_count,
                "latest_swap_block": latest_swap["block"] if latest_swap else None,
                "active_liquidity_raw": latest_swap["active_liquidity_raw"] if latest_swap else None,
            }
            token_pools.append(entry)

            if latest_swap:
                price_usd, route = pool_candidate_price_usd(
                    pool,
                    latest_swap,
                    address,
                    metadata_by_address,
                    weth_usd,
                )

                candidate_price = {
                    "pool_id": pool_id,
                    "swap": latest_swap,
                    "price_usd": price_usd,
                    "route": route,
                }

                if (
                    freshest is None
                    or (latest_swap["block"], latest_swap["log_index"])
                    > (freshest["swap"]["block"], freshest["swap"]["log_index"])
                ):
                    freshest = candidate_price

        price_usd = None
        price_confidence = "UNRESOLVED"
        price_route = None
        price_age_seconds = None
        freshest_swap_block = None
        freshest_swap_tx = None

        if freshest:
            freshest_swap_block = freshest["swap"]["block"]
            freshest_swap_tx = freshest["swap"]["tx"]
            swap_ts = ts_for_block(freshest_swap_block)
            price_age_seconds = max(0, head_ts - swap_ts)

            if (
                freshest["price_usd"] is not None
                and price_age_seconds <= PRICE_MAX_AGE_SECONDS
            ):
                price_usd = freshest["price_usd"]
                price_route = freshest["route"]
                price_confidence = (
                    "HIGH"
                    if price_route == "USDG"
                    else "MEDIUM"
                )

        market_cap_usd = None
        if (
            price_usd is not None
            and metadata.get("total_supply") is not None
        ):
            market_cap_usd = price_usd * metadata["total_supply"]

        recent_balances = balances.get(address, {})
        holder_count_recent = sum(
            1 for value in recent_balances.values()
            if value > 0
        )

        candidate_first_block = int(candidate.get("first_seen_block") or 0)
        holders_complete = candidate_first_block >= history_start

        owner_share = metadata.get("owner_supply_share")
        structural_flags = []

        if metadata.get("code_bytes") == 0:
            structural_flags.append("NO_RUNTIME_CODE")

        if owner_share is not None and owner_share >= 0.20:
            structural_flags.append("OWNER_BALANCE_GTE_20PCT")

        structural_status = "UNRESOLVED"
        if "NO_RUNTIME_CODE" in structural_flags:
            structural_status = "REJECT"

        # We explicitly do not mark CLEAN until sellability/admin/liquidity
        # controls and cluster checks are implemented.
        unresolved_checks = [
            "sellability_honeypot_simulation",
            "mint_admin_permissions_generic",
            "linked_wallet_sybil_clusters",
            "liquidity_removability_lock",
            "creator_history",
            "usd_liquidity_tvl",
            "end_user_unique_buyers_sellers",
        ]

        swap_count_window = sum(
            len(swaps_by_pool.get(pool["pool_id"], []))
            for pool in token_pools
        )

        snapshot = {
            "timestamp_unix": int(time.time()),
            "head_block": head,
            "classification": candidate.get("classification"),
            "price_usd": price_usd,
            "price_confidence": price_confidence,
            "price_route": price_route,
            "price_age_seconds": price_age_seconds,
            "market_cap_usd": market_cap_usd,
            "freshest_swap_block": freshest_swap_block,
            "freshest_swap_tx": freshest_swap_tx,
            "swap_count_window": swap_count_window,
            "holder_count_recent": holder_count_recent,
            "holders_complete_from_launch": holders_complete,
            "transfer_events_window": transfer_stats.get(address, {}).get("transfer_events"),
            "unique_transfer_addresses_window": transfer_stats.get(address, {}).get("unique_transfer_addresses"),
            "owner_supply_share": owner_share,
            "structural_status": structural_status,
            "structural_flags": structural_flags,
        }

        prior = token_state.get(address, {})
        snapshots = list(prior.get("snapshots") or [])
        snapshots.append(snapshot)
        snapshots = snapshots[-MAX_SNAPSHOTS_PER_TOKEN:]

        trends = snapshot_trends(snapshots)

        status = "UNRESOLVED"
        if structural_status == "REJECT":
            status = "REJECT"
        elif market_cap_usd is not None and swap_count_window > 0:
            status = "ACTIVE"

        token_state[address] = {
            "address": address,
            "status": status,
            "first_seen_block": candidate.get("first_seen_block"),
            "classification": candidate.get("classification"),
            "sources": candidate.get("sources") or [candidate.get("source")],
            "metadata": metadata,
            "pools": token_pools,
            "latest": snapshot,
            "trends": trends,
            "unresolved_checks": unresolved_checks,
            "snapshots": snapshots,
        }

        latest_rows.append(token_state[address])

    failed_ranges = {}
    if init_failed or swap_failed:
        failed_ranges["pool_events"] = merge_ranges(init_failed + swap_failed)
    if transfer_failed:
        failed_ranges["hot_transfers"] = transfer_failed

    analysis_coverage = "COMPLETE_FOR_IMPLEMENTED_CHECKS" if not failed_ranges else "PARTIAL"

    output_state = {
        "version": 1,
        "mode": mode,
        "last_block": head,
        "last_block_timestamp": head_ts,
        "analysis_start_block": history_start,
        "analysis_start_timestamp": block_timestamp(history_start),
        "hot_limit": HOT_LIMIT,
        "hot_contracts_analyzed": len(hot),
        "weth_usd": weth_usd,
        "weth_usd_pool": weth_usd_pool,
        "coverage_status": analysis_coverage,
        "failed_ranges": failed_ranges,
        "collector_coverage_status": collector_state.get("coverage_status"),
        "collector_coverage_complete": collector_state.get("coverage_complete"),
        "tokens": token_state,
        "pools": pools,
        "runtime_seconds": round(time.time() - started, 2),
        "updated_at_unix": int(time.time()),
        "rpc": RPC,
    }

    latest_output = {
        "version": 1,
        "head_block": head,
        "head_timestamp": head_ts,
        "coverage_status": analysis_coverage,
        "collector_coverage_status": collector_state.get("coverage_status"),
        "hot_contracts_analyzed": len(latest_rows),
        "weth_usd": weth_usd,
        "results": latest_rows,
        "updated_at_unix": int(time.time()),
    }

    ANALYSIS_STATE.write_text(json.dumps(output_state, indent=2) + "\n")
    LATEST.write_text(json.dumps(latest_output, indent=2) + "\n")

    resolved_mc = sum(
        1 for row in latest_rows
        if row.get("latest", {}).get("market_cap_usd") is not None
    )
    rejected = sum(
        1 for row in latest_rows
        if row.get("status") == "REJECT"
    )

    print(f"Mode: {mode}")
    print(f"Hot contracts analyzed: {len(latest_rows)}")
    print(f"Fresh MC resolved: {resolved_mc}")
    print(f"Structural rejects: {rejected}")
    print(f"WETH/USDG reference: {weth_usd}")
    print(f"Coverage: {analysis_coverage}")
    print(f"Failed ranges: {failed_ranges}")
    print(f"Runtime: {output_state['runtime_seconds']} seconds")


if __name__ == "__main__":
    main()
