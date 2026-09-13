#!/usr/bin/env python3
"""
Optimized Robinhood Chain discovery collector.

Designed for frequent GitHub Actions runs:
- zero-address ERC-20 mint discovery
- combined known launchpad/factory log discovery
- Uniswap v4 PoolManager log discovery
- exact-address extraction from topics/data
- exact-contract deduplication
- bounded overlap between runs
- no expensive per-address eth_getCode checks

No API key required.
"""

import json
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"

STATE = Path("state.json")
OUTPUT = Path("candidates.json")

# First run: short bounded lookback.
FIRST_RUN_LOOKBACK_BLOCKS = 300

# Later runs: rescan a small overlap to protect against temporary misses.
OVERLAP_BLOCKS = 200

# Public RPC safety.
MAX_BLOCKS_PER_QUERY = 300

FACTORIES = [
    "0x5fcc1df0dc020cf454e742e9a8ae2554c37a452c",  # hood.fun
    "0x62b33a039d289cbda50ebeb72fe4261449e61bcf",  # LaunchHood
    "0xd4ccbfa37e2f35611b3042e4096ad7a3459bd007",  # Virtuals
    "0x26605f322f7ff986f381bb9a6e3f5dab0beaeb09",  # Flap.sh
    "0x16cf6788b762ee8969744586ed16fc5705140dd7",  # Klik Finance
    "0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862",  # Doppler Airlock
    "0x22e99278308b393ea1260859b181ad7e78f5eeed",  # Doppler launcher
    "0x6e4910ea5a04376032f6564da9a9e4e88b7a87c1",  # Ape.store
    "0xe8cc4431adf8b5a847c113ef0c6af9043219cb37",  # Bags.fm
    "0xd3f2cc1731b7fd17f28798835c2e02f0a1839a94",  # Clanker
    "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e",  # Pons V2 factory
    "0xe33e9e479df8802cb0866d5d05258bec4cf62948",  # Pons V2 router
    "0x0000ffffbe8efe702c8703ae3477ff5de3d319c0",  # pools.trade current
    "0x00004c4ccc709ef590f7c81102c0689f0263d4e9",  # pools.trade original
    "0x77dc6f6361b7b99456fc3761ce5b7dda80d83f9d",  # trench.today
]

UNISWAP_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

CANONICAL = {
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73",  # WETH
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168",  # USDG
}

IGNORE = set(a.lower() for a in FACTORIES) | {
    UNISWAP_V4_POOL_MANAGER.lower(),
    *CANONICAL,
}

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
ZERO_TOPIC = "0x" + "0" * 64


def rpc(method, params, timeout=15):
    data = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }).encode()

    req = urllib.request.Request(
        RPC,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "robinhood-scanner/3.0"
        }
    )

    with urllib.request.urlopen(req, timeout=timeout) as r:
        obj = json.load(r)

    if "error" in obj:
        raise RuntimeError(obj["error"])

    return obj["result"]


def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


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

    addr = "0x" + raw

    if addr == "0x" + "0" * 40:
        return None

    return addr


def extract_addresses(log):
    """
    Pull possible addresses from indexed topics and ABI-shaped data words.
    False positives are tolerable at this collection layer; later analysis
    validates candidates. This avoids costly per-address RPC verification.
    """
    found = set()

    for topic in (log.get("topics") or [])[1:]:
        if not isinstance(topic, str):
            continue

        raw = topic[2:] if topic.startswith("0x") else topic

        if len(raw) == 64 and raw[:24] == "0" * 24:
            addr = normalize_address(raw[-40:])
            if addr:
                found.add(addr)

    data = log.get("data") or "0x"
    raw_data = data[2:] if data.startswith("0x") else data

    for i in range(0, len(raw_data) - 63, 64):
        word = raw_data[i:i + 64]

        if len(word) == 64 and word[:24] == "0" * 24:
            addr = normalize_address(word[-40:])
            if addr:
                found.add(addr)

    return found


def get_logs(base_filter, start, end):
    rows = []

    current = start
    while current <= end:
        to_block = min(end, current + MAX_BLOCKS_PER_QUERY - 1)

        f = dict(base_filter)
        f["fromBlock"] = hex(current)
        f["toBlock"] = hex(to_block)

        try:
            rows.extend(rpc("eth_getLogs", [f], timeout=20))
        except Exception as exc:
            print(f"WARNING: failed log range {current}-{to_block}: {exc}")

        current = to_block + 1

    return rows


def add(found, address, log, source):
    address = normalize_address(address)

    if not address or address in IGNORE:
        return

    row = {
        "address": address,
        "first_seen_block": int(log["blockNumber"], 16),
        "tx": log.get("transactionHash"),
        "source": source,
        "collected_at_unix": int(time.time())
    }

    old = found.get(address)

    if old is None or row["first_seen_block"] < old["first_seen_block"]:
        found[address] = row


def main():
    started = time.time()

    head = int(rpc("eth_blockNumber", []), 16)

    state = load_json(STATE, {})

    if "last_block" in state:
        start = max(0, int(state["last_block"]) - OVERLAP_BLOCKS)
        mode = "overlap"
    else:
        start = max(0, head - FIRST_RUN_LOOKBACK_BLOCKS)
        mode = "first_run"

    # Never scan beyond current head.
    start = min(start, head)

    print(f"Mode: {mode}")
    print(f"Scanning blocks {start}-{head}")

    found = {}

    # 1. Chain-wide zero-address mint logs.
    mint_logs = get_logs(
        {"topics": [TRANSFER_TOPIC, ZERO_TOPIC]},
        start,
        head
    )

    print(f"Zero-mint logs: {len(mint_logs)}")

    for log in mint_logs:
        add(found, log.get("address"), log, "zero_address_mint")

    # 2. Known launchpads/factories in one address-filter query.
    factory_logs = get_logs(
        {"address": FACTORIES},
        start,
        head
    )

    print(f"Factory logs: {len(factory_logs)}")

    for log in factory_logs:
        emitter = normalize_address(log.get("address"))
        source = f"factory_log:{emitter}"

        for addr in extract_addresses(log):
            add(found, addr, log, source)

    # 3. Uniswap v4 PoolManager.
    pool_logs = get_logs(
        {"address": UNISWAP_V4_POOL_MANAGER},
        start,
        head
    )

    print(f"PoolManager logs: {len(pool_logs)}")

    for log in pool_logs:
        for addr in extract_addresses(log):
            add(found, addr, log, "uniswap_v4_poolmanager_log")

    existing = load_json(OUTPUT, [])
    by_address = {}

    for row in existing:
        addr = normalize_address(row.get("address"))
        if addr:
            by_address[addr] = row

    new_count = 0

    for addr, row in found.items():
        if addr not in by_address:
            by_address[addr] = row
            new_count += 1
        else:
            old_block = by_address[addr].get("first_seen_block")
            if old_block is None or row["first_seen_block"] < old_block:
                by_address[addr] = row

    rows = sorted(
        by_address.values(),
        key=lambda x: (x.get("first_seen_block", 0), x.get("address", ""))
    )

    OUTPUT.write_text(json.dumps(rows[-10000:], indent=2) + "\n")

    state_out = {
        "last_block": head,
        "scan_start": start,
        "scan_end": head,
        "mode": mode,
        "zero_mint_logs": len(mint_logs),
        "factory_logs": len(factory_logs),
        "poolmanager_logs": len(pool_logs),
        "new_candidates": new_count,
        "stored_candidates": len(rows),
        "runtime_seconds": round(time.time() - started, 2),
        "updated_at_unix": int(time.time()),
        "rpc": RPC
    }

    STATE.write_text(json.dumps(state_out, indent=2) + "\n")

    print(f"New candidates: {new_count}")
    print(f"Stored candidates: {len(rows)}")
    print(f"Runtime: {state_out['runtime_seconds']} seconds")


if __name__ == "__main__":
    main()
