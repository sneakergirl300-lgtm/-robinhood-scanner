#!/usr/bin/env python3
"""Fast Robinhood Chain smoke-test collector. No API keys required."""

import json
import time
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"
STATE = Path("state.json")
OUT = Path("candidates.json")

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "0" * 64

IGNORE = {
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73",  # WETH
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168",  # USDG
}

def rpc(method, params):
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }).encode()

    req = urllib.request.Request(
        RPC,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "rh-candidate-collector/1.1"
        }
    )

    with urllib.request.urlopen(req, timeout=15) as r:
        obj = json.load(r)

    if "error" in obj:
        raise RuntimeError(obj["error"])

    return obj["result"]

def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default

def main():
    head = int(rpc("eth_blockNumber", []), 16)

    # Tiny test window so the workflow finishes quickly.
    # After this succeeds, we will expand it safely.
    start = max(0, head - 100)

    logs = rpc("eth_getLogs", [{
        "fromBlock": hex(start),
        "toBlock": hex(head),
        "topics": [TRANSFER_TOPIC, ZERO_TOPIC]
    }])

    existing = load_json(OUT, [])
    known = {x.get("address", "").lower() for x in existing}
    added = 0

    for log in logs:
        token = log.get("address", "").lower()

        if not token or token in IGNORE or token in known:
            continue

        existing.append({
            "address": token,
            "first_seen_block": int(log["blockNumber"], 16),
            "source": "zero_mint",
            "tx": log.get("transactionHash"),
            "collected_at_unix": int(time.time())
        })

        known.add(token)
        added += 1

    OUT.write_text(json.dumps(existing[-10000:], indent=2) + "\n")

    STATE.write_text(json.dumps({
        "last_block": head,
        "scan_start": start,
        "scan_end": head,
        "updated_at_unix": int(time.time()),
        "rpc": RPC
    }, indent=2) + "\n")

    print(f"Scanned blocks {start}-{head}")
    print(f"New candidates: {added}")
    print(f"Stored candidates: {len(existing)}")

if __name__ == "__main__":
    main()
