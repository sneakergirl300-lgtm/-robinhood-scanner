#!/usr/bin/env python3
"""
Robinhood Chain discovery collector.

What it does:
- Scans recent Robinhood Chain blocks directly from the public RPC.
- Finds ERC-20 zero-address mint logs.
- Scans known launchpad/factory contracts for launch-related activity.
- Scans the Uniswap v4 PoolManager for new-pool/activity logs.
- Extracts exact contract addresses mechanically from topics/data.
- Verifies extracted addresses are contracts with eth_getCode.
- Keeps overlap/backfill between runs and deduplicates exact addresses.

No API key is required.
"""

import json
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"

STATE = Path("state.json")
OUTPUT = Path("candidates.json")

FIRST_RUN_LOOKBACK_SECONDS = 30 * 60
OVERLAP_SECONDS = 15 * 60
MAX_BLOCKS_PER_QUERY = 1000

FACTORIES = {
    "0x5fcc1df0dc020cf454e742e9a8ae2554c37a452c",
    "0x62b33a039d289cbda50ebeb72fe4261449e61bcf",
    "0xd4ccbfa37e2f35611b3042e4096ad7a3459bd007",
    "0x26605f322f7ff986f381bb9a6e3f5dab0beaeb09",
    "0x16cf6788b762ee8969744586ed16fc5705140dd7",
    "0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862",
    "0x22e99278308b393ea1260859b181ad7e78f5eeed",
    "0x6e4910ea5a04376032f6564da9a9e4e88b7a87c1",
    "0xe8cc4431adf8b5a847c113ef0c6af9043219cb37",
    "0xd3f2cc1731b7fd17f28798835c2e02f0a1839a94",
    "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e",
    "0xe33e9e479df8802cb0866d5d05258bec4cf62948",
    "0x0000ffffbe8efe702c8703ae3477ff5de3d319c0",
    "0x00004c4ccc709ef590f7c81102c0689f0263d4e9",
    "0x77dc6f6361b7b99456fc3761ce5b7dda80d83f9d",
}

UNISWAP_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

CANONICAL_ASSETS = {
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73",
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168",
}

IGNORE = FACTORIES | {UNISWAP_V4_POOL_MANAGER} | CANONICAL_ASSETS

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
ZERO_TOPIC = "0x" + "0" * 64


def rpc(method, params, timeout=20):
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }).encode()

    req = urllib.request.Request(
        RPC,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "robinhood-chain-discovery-collector/2.0",
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


def get_logs(filter_base, start_block, end_block):
    all_logs = []

    def fetch(frm, to):
        filt = dict(filter_base)
        filt["fromBlock"] = hex(frm)
        filt["toBlock"] = hex(to)

        try:
            rows = rpc("eth_getLogs", [filt], timeout=30)
            all_logs.extend(rows)
            return
        except Exception as exc:
            if frm >= to:
                print(f"WARNING: log query failed for block {frm}: {exc}")
                return

            mid = (frm + to) // 2
            print(f"Splitting failed range {frm}-{to}")
            fetch(frm, mid)
            fetch(mid + 1, to)

    current = start_block

    while current <= end_block:
        end = min(end_block, current + MAX_BLOCKS_PER_QUERY - 1)
        fetch(current, end)
        current = end + 1

    return all_logs


def normalize_address(value):
    if not isinstance(value, str):
        return None

    value = value.lower()

    if value.startswith("0x"):
        value = value[2:]

    if len(value) != 40:
        return None

    try:
        int(value, 16)
    except ValueError:
        return None

    address = "0x" + value

    if address == "0x" + "0" * 40:
        return None

    return address


def addresses_from_log(log):
    found = set()

    emitter = normalize_address(log.get("address"))
    if emitter:
        found.add(emitter)

    topics = log.get("topics") or []

    for topic in topics[1:]:
        if not isinstance(topic, str):
            continue

        raw = topic[2:] if topic.startswith("0x") else topic

        if len(raw) == 64 and raw[:24] == "0" * 24:
            address = normalize_address(raw[-40:])
            if address:
                found.add(address)

    data = log.get("data") or "0x"
    raw_data = data[2:] if data.startswith("0x") else data

    for i in range(0, len(raw_data) - 63, 64):
        word = raw_data[i:i + 64]

        if word[:24] == "0" * 24:
            address = normalize_address(word[-40:])
            if address:
                found.add(address)

    return found


_contract_cache = {}


def is_contract(address):
    if address in _contract_cache:
        return _contract_cache[address]

    try:
        code = rpc("eth_getCode", [address, "latest"], timeout=15)
        result = bool(code and code != "0x")
    except Exception as exc:
        print(f"WARNING: eth_getCode failed for {address}: {exc}")
        result = False

    _contract_cache[address] = result
    return result


def make_row(address, log, source):
    return {
        "address": address,
        "first_seen_block": int(log["blockNumber"], 16),
        "tx": log.get("transactionHash"),
        "source": source,
        "collected_at_unix": int(time.time()),
    }


def add_candidate(found, address, log, source):
    address = normalize_address(address)

    if not address or address in IGNORE:
        return

    if not is_contract(address):
        return

    row = make_row(address, log, source)

    old = found.get(address)

    if old is None or row["first_seen_block"] < old["first_seen_block"]:
        found[address] = row


def main():
    started = time.time()

    head = int(rpc("eth_blockNumber", []), 16)
    head_ts = block_timestamp(head)

    state = load_json(STATE, {})

    if "last_block" in state:
        previous_block = int(state["last_block"])
        previous_ts = block_timestamp(min(previous_block, head))
        target_ts = max(0, previous_ts - OVERLAP_SECONDS)
        start = find_block_at_or_before_timestamp(head, target_ts)
        mode = "overlap"
    else:
        target_ts = max(0, head_ts - FIRST_RUN_LOOKBACK_SECONDS)
        start = find_block_at_or_before_timestamp(head, target_ts)
        mode = "first_run"

    print(f"Mode: {mode}")
    print(f"Scanning blocks {start}-{head}")
    print(f"Approximate window: {max(0, head_ts - block_timestamp(start))} seconds")

    found = {}

    mint_logs = get_logs(
        {"topics": [TRANSFER_TOPIC, ZERO_TOPIC]},
        start,
        head,
    )

    print(f"Zero-mint logs: {len(mint_logs)}")

    for log in mint_logs:
        token = normalize_address(log.get("address"))
        if token:
            add_candidate(found, token, log, "zero_address_mint")

    factory_logs = []

    # Query each known factory separately for broad RPC compatibility.
    for factory in sorted(FACTORIES):
        rows = get_logs({"address": factory}, start, head)
        factory_logs.extend(rows)

    print(f"Factory logs: {len(factory_logs)}")

    for log in factory_logs:
        emitter = normalize_address(log.get("address"))
        source = f"factory_log:{emitter}"

        for address in addresses_from_log(log):
            add_candidate(found, address, log, source)

    pool_logs = get_logs(
        {"address": UNISWAP_V4_POOL_MANAGER},
        start,
        head,
    )

    print(f"Uniswap v4 PoolManager logs: {len(pool_logs)}")

    for log in pool_logs:
        for address in addresses_from_log(log):
            add_candidate(found, address, log, "uniswap_v4_poolmanager_log")

    existing = load_json(OUTPUT, [])
    by_address = {}

    for row in existing:
        address = normalize_address(row.get("address"))
        if address:
            by_address[address] = row

    new_count = 0

    for address, row in sorted(
        found.items(),
        key=lambda item: item[1]["first_seen_block"]
    ):
        if address not in by_address:
            by_address[address] = row
            new_count += 1
        else:
            existing_row = by_address[address]
            old_block = existing_row.get("first_seen_block")

            if old_block is None or row["first_seen_block"] < old_block:
                by_address[address] = row

    rows = sorted(
        by_address.values(),
        key=lambda row: (
            row.get("first_seen_block", 0),
            row.get("address", "")
        )
    )

    OUTPUT.write_text(json.dumps(rows[-10000:], indent=2) + "\n")

    scan_start_ts = block_timestamp(start)

    state_out = {
        "last_block": head,
        "last_block_timestamp": head_ts,
        "scan_start": start,
        "scan_end": head,
        "scan_start_timestamp": scan_start_ts,
        "scan_end_timestamp": head_ts,
        "scan_window_seconds": head_ts - scan_start_ts,
        "mode": mode,
        "zero_mint_logs": len(mint_logs),
        "factory_logs": len(factory_logs),
        "uniswap_v4_poolmanager_logs": len(pool_logs),
        "new_candidates": new_count,
        "stored_candidates": len(rows),
        "runtime_seconds": round(time.time() - started, 2),
        "updated_at_unix": int(time.time()),
        "rpc": RPC,
    }

    STATE.write_text(json.dumps(state_out, indent=2) + "\n")

    print(f"New candidates: {new_count}")
    print(f"Stored candidates: {len(rows)}")
    print(f"Runtime: {state_out['runtime_seconds']} seconds")


if __name__ == "__main__":
    main()
