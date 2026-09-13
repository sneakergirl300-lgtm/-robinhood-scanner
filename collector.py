#!/usr/bin/env python3
"""
Robinhood Chain discovery collector v4.

Fast, targeted discovery:
- ERC-20 zero-address mint logs
- known launchpad/factory logs
- ONLY Uniswap v4 Initialize events (new pools)
- exact-address deduplication
- bounded overlap for frequent GitHub Actions runs

No API key required.
"""

import json
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"

STATE = Path("state.json")
OUTPUT = Path("candidates.json")

FIRST_RUN_LOOKBACK_BLOCKS = 300
OVERLAP_BLOCKS = 200
MAX_BLOCKS_PER_QUERY = 300

FACTORIES = [
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
]

POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"

IGNORE = set(a.lower() for a in FACTORIES) | {
    POOL_MANAGER.lower(),
    WETH,
    USDG,
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
            "User-Agent": "robinhood-scanner/4.0",
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
    """Decode an indexed address topic."""
    if not isinstance(topic, str):
        return None

    raw = topic[2:] if topic.startswith("0x") else topic

    if len(raw) != 64 or raw[:24] != "0" * 24:
        return None

    return normalize_address(raw[-40:])


def extract_factory_addresses(log):
    """
    Conservative-ish generic extraction for launchpad/factory events.
    Only ABI-shaped zero-padded address words are accepted.
    """
    found = set()

    for topic in (log.get("topics") or [])[1:]:
        address = topic_address(topic)
        if address:
            found.add(address)

    data = log.get("data") or "0x"
    raw = data[2:] if data.startswith("0x") else data

    for i in range(0, len(raw) - 63, 64):
        word = raw[i:i + 64]

        if len(word) == 64 and word[:24] == "0" * 24:
            address = normalize_address(word[-40:])
            if address:
                found.add(address)

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
            print(f"WARNING: failed range {current}-{to_block}: {exc}")

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
        "collected_at_unix": int(time.time()),
    }

    old = found.get(address)

    if old is None or row["first_seen_block"] < old["first_seen_block"]:
        found[address] = row


def clean_existing(rows):
    """
    v3 accidentally treated arbitrary PoolManager data as addresses.
    Drop those polluted historical rows automatically.

    Retain:
    - zero-address mint discoveries
    - factory discoveries
    - correctly targeted v4 Initialize discoveries
    """
    keep = []

    for row in rows:
        source = str(row.get("source", ""))

        if (
            source == "zero_address_mint"
            or source.startswith("factory_log:")
            or source == "uniswap_v4_initialize"
        ):
            keep.append(row)

    return keep


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

    start = min(start, head)

    print(f"Mode: {mode}")
    print(f"Scanning blocks {start}-{head}")

    found = {}

    # 1) ERC-20 launch mints.
    mint_logs = get_logs(
        {"topics": [TRANSFER_TOPIC, ZERO_TOPIC]},
        start,
        head,
    )

    for log in mint_logs:
        add(found, log.get("address"), log, "zero_address_mint")

    print(f"Zero-mint logs: {len(mint_logs)}")

    # 2) Known launchpad/factory activity.
    factory_logs = get_logs(
        {"address": FACTORIES},
        start,
        head,
    )

    for log in factory_logs:
        emitter = normalize_address(log.get("address"))
        source = f"factory_log:{emitter}"

        for address in extract_factory_addresses(log):
            add(found, address, log, source)

    print(f"Factory logs: {len(factory_logs)}")

    # 3) ONLY new Uniswap v4 pools.
    init_logs = get_logs(
        {
            "address": POOL_MANAGER,
            "topics": [V4_INITIALIZE_TOPIC],
        },
        start,
        head,
    )

    for log in init_logs:
        topics = log.get("topics") or []

        # Initialize:
        # topics[1] poolId
        # topics[2] currency0
        # topics[3] currency1
        if len(topics) >= 4:
            currency0 = topic_address(topics[2])
            currency1 = topic_address(topics[3])

            if currency0:
                add(found, currency0, log, "uniswap_v4_initialize")

            if currency1:
                add(found, currency1, log, "uniswap_v4_initialize")

    print(f"Uniswap v4 Initialize logs: {len(init_logs)}")

    existing_raw = load_json(OUTPUT, [])
    existing = clean_existing(existing_raw)

    removed_polluted = len(existing_raw) - len(existing)

    by_address = {}

    for row in existing:
        address = normalize_address(row.get("address"))
        if address:
            by_address[address] = row

    new_count = 0

    for address, row in found.items():
        if address not in by_address:
            by_address[address] = row
            new_count += 1
        else:
            old_block = by_address[address].get("first_seen_block")

            if old_block is None or row["first_seen_block"] < old_block:
                by_address[address] = row

    rows = sorted(
        by_address.values(),
        key=lambda row: (
            row.get("first_seen_block", 0),
            row.get("address", ""),
        ),
    )

    OUTPUT.write_text(json.dumps(rows[-10000:], indent=2) + "\n")

    state_out = {
        "last_block": head,
        "scan_start": start,
        "scan_end": head,
        "mode": mode,
        "zero_mint_logs": len(mint_logs),
        "factory_logs": len(factory_logs),
        "uniswap_v4_initialize_logs": len(init_logs),
        "new_candidates": new_count,
        "stored_candidates": len(rows),
        "polluted_rows_removed": removed_polluted,
        "runtime_seconds": round(time.time() - started, 2),
        "updated_at_unix": int(time.time()),
        "rpc": RPC,
    }

    STATE.write_text(json.dumps(state_out, indent=2) + "\n")

    print(f"Removed polluted old rows: {removed_polluted}")
    print(f"New candidates: {new_count}")
    print(f"Stored candidates: {len(rows)}")
    print(f"Runtime: {state_out['runtime_seconds']} seconds")


if __name__ == "__main__":
    main()
