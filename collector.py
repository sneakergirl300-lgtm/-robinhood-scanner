#!/usr/bin/env python3
"""Robinhood Chain candidate collector. No API keys required."""
import json, os, time, urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"
STATE = Path("state.json")
OUT = Path("candidates.json")

FACTORIES = [
    "0x5fcc1df0dc020cf454e742e9a8ae2554c37a452c", # hood.fun
    "0x62b33a039d289cbda50ebeb72fe4261449e61bcf", # LaunchHood
    "0xd4ccbfa37e2f35611b3042e4096ad7a3459bd007", # Virtuals
    "0x26605f322f7ff986f381bb9a6e3f5dab0beaeb09", # Flap
    "0x16cf6788b762ee8969744586ed16fc5705140dd7", # Klik
    "0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862", # Doppler Airlock
    "0x22e99278308b393ea1260859b181ad7e78f5eeed", # Doppler launcher
    "0x6e4910ea5a04376032f6564da9a9e4e88b7a87c1", # Ape.store
    "0xe8cc4431adf8b5a847c113ef0c6af9043219cb37", # Bags.fm
    "0xd3f2cc1731b7fd17f28798835c2e02f0a1839a94", # Clanker
    "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e", # Pons V2
    "0xe33e9e479df8802cb0866d5d05258bec4cf62948", # Pons router
    "0x0000ffffbe8efe702c8703ae3477ff5de3d319c0", # pools.trade
    "0x00004c4ccc709ef590f7c81102c0689f0263d4e9", # pools.trade old
    "0x77dc6f6361b7b99456fc3761ce5b7dda80d83f9d", # trench.today
    "0x8366a39cc670b4001a1121b8f6a443a643e40951", # Uniswap v4 PoolManager
]
IGNORE = {
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73", # WETH
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168", # USDG
} | set(FACTORIES)
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "0" * 64

def rpc(method, params):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    req = urllib.request.Request(RPC, data=body, headers={"Content-Type":"application/json","User-Agent":"rh-candidate-collector/1.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        obj = json.load(r)
    if "error" in obj:
        raise RuntimeError(obj["error"])
    return obj["result"]

def is_contract(addr):
    try:
        return rpc("eth_getCode", [addr, "latest"]) not in ("0x", "0x0", None)
    except Exception:
        return False

def words_to_addresses(hexdata):
    s = hexdata[2:] if hexdata.startswith("0x") else hexdata
    found = set()
    for i in range(0, len(s) - 63, 64):
        word = s[i:i+64]
        if word[:24] == "0" * 24:
            a = "0x" + word[24:]
            if a != "0x" + "0"*40:
                found.add(a.lower())
    return found

def load_json(path, default):
    try: return json.loads(path.read_text())
    except Exception: return default

def get_logs(frm, to, address=None, topics=None):
    f = {"fromBlock":hex(frm), "toBlock":hex(to)}
    if address: f["address"] = address
    if topics: f["topics"] = topics
    return rpc("eth_getLogs", [f])

def main():
    head = int(rpc("eth_blockNumber", []), 16)
    state = load_json(STATE, {})
    # Always overlap by 1200 blocks. First run scans only recent history to stay lightweight.
    last = int(state.get("last_block", max(0, head - 1200)))
    start = max(0, min(last - 1200, head - 1200))
    existing = load_json(OUT, [])
    known = {x.get("address", "").lower() for x in existing}
    additions = {}

    # Split into small ranges so a busy interval cannot truncate the whole scan.
    step = 250
    for a in range(start, head + 1, step):
        b = min(head, a + step - 1)
        # Cross-chain ERC-20 launch signal: mint from zero address.
        try:
            logs = get_logs(a, b, topics=[TRANSFER_TOPIC, ZERO_TOPIC])
            for log in logs:
                token = log.get("address", "").lower()
                if token and token not in IGNORE:
                    additions.setdefault(token, {"address":token,"first_seen_block":int(log["blockNumber"],16),"source":"zero_mint","tx":log.get("transactionHash")})
        except Exception as e:
            print("zero-mint range warning", a, b, e)

        # Known launch/factory/pool-manager activity. Extract address-shaped event words.
        for factory in FACTORIES:
            try:
                logs = get_logs(a, b, address=factory)
                for log in logs:
                    vals = set()
                    for t in log.get("topics", [])[1:]: vals |= words_to_addresses(t)
                    vals |= words_to_addresses(log.get("data", "0x"))
                    for token in vals:
                        if token not in IGNORE and token not in known and is_contract(token):
                            additions.setdefault(token, {"address":token,"first_seen_block":int(log["blockNumber"],16),"source":"factory_or_pool_event","factory":factory,"tx":log.get("transactionHash")})
            except Exception as e:
                print("factory range warning", factory, a, b, e)

    for token, row in sorted(additions.items(), key=lambda kv: kv[1]["first_seen_block"]):
        if token not in known:
            row["collected_at_unix"] = int(time.time())
            existing.append(row); known.add(token)

    OUT.write_text(json.dumps(existing[-10000:], indent=2) + "\n")
    STATE.write_text(json.dumps({"last_block":head,"updated_at_unix":int(time.time()),"rpc":RPC}, indent=2) + "\n")
    print(f"Scanned blocks {start}-{head}; new candidates: {len(additions)}; stored: {len(existing)}")

if __name__ == "__main__":
    main()
