#!/usr/bin/env python3
"""Bounded Part 2 observation engine. Missing evidence never authorizes alerts."""
import copy
import json
import signal
import time
import urllib.error
import urllib.request
from pathlib import Path
from decimal import Decimal, localcontext

from v4_events import normalize_address, decode_initialize, decode_swap

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
INITIALIZE = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
SWAP = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SELECTORS = {"decimals": "0x313ce567", "total_supply_raw": "0x18160ddd", "owner_raw": "0x8da5cb5b"}
MAX_SECONDS = 180
MAX_CALLS = 48
HOT_LIMIT = 8
CHUNK = 500
FRESH_SECONDS = 300
ACTIVE_SECONDS = 6 * 3600
CHECKS = ["creator_supply_control", "holder_concentration", "linked_ownership",
          "mint_admin_permissions", "sellability_tax", "liquidity_control", "creator_history"]


class Deferred(Exception):
    pass


class StopRun(Deferred):
    pass


def load(path, default):
    # Corruption is an error, not permission to discard cursors.
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else copy.deepcopy(default)


def save(path, data):
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    tmp.replace(p)


class RPC:
    def __init__(self, seconds=MAX_SECONDS, calls=MAX_CALLS):
        self.started = time.monotonic()
        self.deadline = self.started + min(MAX_SECONDS, max(1, seconds))
        self.limit = min(MAX_CALLS, max(1, calls))
        self.calls = 0
        self.next_call = self.started
        self.stopped = None

    def __call__(self, method, params):
        if self.stopped:
            raise StopRun(self.stopped)
        remaining = self.deadline - time.monotonic()
        delay = max(0, self.next_call - time.monotonic())
        if self.calls >= self.limit or remaining <= delay + 1:
            raise StopRun("RPC_OR_TIME_BUDGET")
        time.sleep(delay)
        self.calls += 1
        req = urllib.request.Request(RPC_URL, data=json.dumps({
            "jsonrpc": "2.0", "id": self.calls, "method": method, "params": params
        }).encode(), headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=min(4, self.deadline-time.monotonic())) as r:
                # Bound response size too; oversized data is unresolved.
                raw = r.read(4_000_001)
                if len(raw) > 4_000_000:
                    raise Deferred("RESPONSE_TOO_LARGE")
                obj = json.loads(raw)
            if "error" in obj:
                error = obj["error"]
                message = str(error).lower()
                if any(s in message for s in ("429", "rate limit", "too many requests", "limit exceeded")):
                    self.stopped = "RATE_LIMIT"
                    raise StopRun(self.stopped)
                raise Deferred("RPC_ERROR: " + str(error))
            if obj.get("result") is None:
                raise Deferred("RPC_NULL_RESULT")
            return obj["result"]
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                self.stopped = "RATE_LIMIT"
                raise StopRun(self.stopped) from exc
            raise Deferred("HTTP_" + str(exc.code)) from exc
        except (OSError, ValueError) as exc:
            raise Deferred(type(exc).__name__) from exc
        finally:
            self.next_call = time.monotonic() + 0.3


def fresh_state():
    return {"version": 3, "tokens": {}, "quotes": {}}


def ingest(state, candidates, now):
    """Local identity merge only. Never perform RPC over the candidate universe."""
    tokens = state["tokens"]
    for c in candidates:
        address = normalize_address(c.get("address"))
        if not address:
            continue
        first = int(c.get("first_seen_block") or 0)
        last = int(c.get("last_seen_block") or first)
        discovered = int(c.get("collected_at_unix") or 0)
        is_new = address not in tokens
        t = tokens.setdefault(address, {
            "address": address, "first_seen_block": first, "last_seen_block": last,
            "discovered_at": discovered, "queue_since": now,
            "next_due": now, "last_attempt": 0, "attempts": 0, "receipts": [],
            "pools": {}, "metadata": {}, "snapshots": [], "lane": "NEW",
            "status": "UNRESOLVED", "last_activity_at": 0,
        })
        if last > t["last_seen_block"]:
            t["last_seen_block"] = last
            # Only new on-chain evidence reactivates cold tokens, not overlap re-observation.
            if t["lane"] == "COLD":
                t.update(lane="FOLLOWUP", queue_since=now, next_due=now)
        t["classification"] = c.get("classification", "UNRESOLVED")
        t["sources"] = c.get("sources") or [c.get("source")]
        t["discovery_receipts"] = list(dict.fromkeys(
            o["tx"] for o in sorted(c.get("observations") or [],
                                    key=lambda o: int(o.get("block") or 0), reverse=True)
            if o.get("tx") and o.get("source") == "uniswap_v4_initialize"))[:4]
        if c.get("tx") and c["tx"] not in t["discovery_receipts"]:
            t["discovery_receipts"].append(c["tx"])
        # Old bootstrap entries stay in the universe but do not flood the hot queue.
        if is_new and discovered and now-discovered > ACTIVE_SECONDS:
            t["lane"] = "COLD"
        if now-max(t["last_activity_at"], t["queue_since"]) > ACTIVE_SECONDS:
            t["lane"] = "COLD"


def select_hot(state, now, limit=HOT_LIMIT):
    due = [t for t in state["tokens"].values()
           if t["lane"] != "COLD" and t["status"] != "REJECT" and t["next_due"] <= now]
    # Reserve half for first observations, half for follow-up; oldest attempt wins.
    new = sorted((t for t in due if not t["attempts"]),
                 key=lambda t: (-t["last_seen_block"], t["address"]))
    old = sorted((t for t in due if t["attempts"]),
                 key=lambda t: (t["last_attempt"], t["next_due"], t["address"]))
    selected = []
    for i in range(limit):
        preferred = new if i % 2 == 0 else old
        other = old if i % 2 == 0 else new
        if preferred or other:
            selected.append((preferred or other).pop(0))
    return selected


def merge_gaps(ranges):
    out = []
    for a, b in sorted(ranges):
        if a > b:
            continue
        if out and a <= out[-1][1]+1:
            out[-1][1] = max(b, out[-1][1])
        else:
            out.append([a, b])
    return out


def sample_stream(rpc, stream, base_filter, head, first, repair=False):
    """One bounded query. Commit cursor/gaps only on a successful complete response."""
    cursor = stream.get("cursor", max(0, first-1))
    gaps = copy.deepcopy(stream.get("gaps", []))
    if repair and gaps:
        start, end = gaps[0][0], min(gaps[0][1], gaps[0][0]+CHUNK-1)
    else:
        if cursor >= head:
            return None
        start, end = max(cursor+1, head-CHUNK+1), head
        if start > cursor+1:
            gaps.append([cursor+1, start-1])
    filt = dict(base_filter, fromBlock=hex(start), toBlock=hex(end))
    logs = rpc("eth_getLogs", [filt])
    if not isinstance(logs, list) or any(l.get("removed") for l in logs):
        raise Deferred("INVALID_OR_REMOVED_LOGS")
    # A provider can silently truncate without signalling; this remains a provider limitation.
    if repair and stream.get("gaps"):
        gaps[0][0] = end+1
    else:
        stream["cursor"] = end
    stream["gaps"] = merge_gaps(gaps)
    return {"from_block": start, "to_block": end, "logs": logs, "repair": repair}


def structural_gate(token, risks):
    flags = list(risks.get(token["address"], {}).get("flags", []))
    m = token.get("metadata", {})
    if m.get("owner_share") is not None and Decimal(m["owner_share"]) >= Decimal("0.20"):
        flags.append("OWNER_CONTROLS_AT_LEAST_20_PERCENT")
    if flags:
        return {"status": "REJECT", "flags": sorted(set(flags)), "unresolved": CHECKS,
                "risk_source": risks.get(token["address"], {}).get("source")}
    # No implemented adapter can yet certify the complete gate. Never infer PASS from silence.
    return {"status": "UNRESOLVED", "flags": [], "unresolved": CHECKS}


def executed_quote(pool, swap, address, decimals, quote_decimals):
    if decimals is None or quote_decimals is None:
        return None
    if pool["currency0"] == address and pool["currency1"] == USDG:
        token, quote = swap["amount0_raw"], swap["amount1_raw"]
    elif pool["currency1"] == address and pool["currency0"] == USDG:
        token, quote = swap["amount1_raw"], swap["amount0_raw"]
    else:
        return None
    if token * quote >= 0:
        return None
    with localcontext() as ctx:
        ctx.prec = 60
        return str(Decimal(abs(quote)) / Decimal(abs(token)) * (Decimal(10) ** (decimals-quote_decimals)))


def observe_step(token, state, rpc, head, now):
    """One resumable unit per turn; round robin prevents metadata monopolizing a run."""
    pending = [tx for tx in token["discovery_receipts"] if tx not in token["receipts"]]
    if pending:
        tx = pending[0]
        receipt = rpc("eth_getTransactionReceipt", [tx])
        for log in receipt.get("logs", []):
            if log.get("address", "").lower() != POOL_MANAGER or (log.get("topics") or [None])[0] != INITIALIZE:
                continue
            pool = decode_initialize(log)
            if pool and token["address"] in {pool["currency0"], pool["currency1"]}:
                token["pools"][pool["pool_id"]] = pool
        if normalize_address(receipt.get("contractAddress")) == token["address"]:
            token["deployment_block"] = int(receipt["blockNumber"], 16)
            token["creator"] = normalize_address(receipt.get("from"))
        token["receipts"].append(tx)
        return

    # Fairly cycle fresh swaps, metadata, transfers, and gap repair.
    phase = token.get("phase", 0) % 4
    token["phase"] = phase+1
    m = token["metadata"]
    if phase == 1:
        if "decimals" in m and "decimals" not in state["quotes"]:
            raw = rpc("eth_call", [{"to": USDG, "data": SELECTORS["decimals"]}, hex(head)])
            if not isinstance(raw, str) or len(raw) != 66 or int(raw,16)>36:
                raise Deferred("INVALID_QUOTE_DECIMALS")
            state["quotes"]["decimals"] = int(raw,16)
            return
        missing = [k for k in SELECTORS if m.get("retry_after", {}).get(k, 0) <= now and
                   (k not in m or (k == "total_supply_raw" and now-m.get("supply_at", 0) > FRESH_SECONDS))]
        if missing:
            key = missing[0]
            m.setdefault("retry_after", {})[key] = now+3600
            raw = rpc("eth_call", [{"to": token["address"], "data": SELECTORS[key]}, hex(head)])
            if not isinstance(raw, str) or len(raw) != 66:
                raise Deferred("INVALID_ERC20_RETURN")
            value = int(raw, 16)
            if key == "decimals" and value > 36:
                raise Deferred("INVALID_DECIMALS")
            m[key] = value
            m["retry_after"][key] = 0
            if key == "total_supply_raw":
                m["supply_at"], m["supply_block"] = now, head
            return
        owner = m.get("owner_raw", 0)
        if owner and owner < 2**160 and m.get("total_supply_raw") and now-m.get("owner_at", 0) > FRESH_SECONDS:
            raw = rpc("eth_call", [{"to": token["address"], "data": "0x70a08231"+f"{owner:064x}"}, hex(head)])
            if not isinstance(raw, str) or len(raw) != 66:
                raise Deferred("INVALID_BALANCE_RETURN")
            m["owner_share"] = str(Decimal(int(raw,16))/Decimal(m["total_supply_raw"]))
            m["owner_at"] = now
            return
        if "decimals" not in state["quotes"]:
            raw = rpc("eth_call", [{"to": USDG, "data": SELECTORS["decimals"]}, hex(head)])
            if not isinstance(raw, str) or len(raw) != 66 or int(raw,16)>36:
                raise Deferred("INVALID_QUOTE_DECIMALS")
            state["quotes"]["decimals"] = int(raw,16)
        return

    pools = sorted(token["pools"])
    use_swaps = phase in (0, 3) and pools
    if use_swaps:
        # Rotate pools rather than permanently truncating pool coverage.
        idx = token.get("pool_turn", 0)
        pool_id = pools[idx % len(pools)]
        token["pool_turn"] = idx+1
        stream = token.setdefault("streams", {}).setdefault(pool_id, {})
        filt = {"address": POOL_MANAGER, "topics": [SWAP, pool_id]}
        first = token["pools"][pool_id]["initialize_block"]
    else:
        stream = token.setdefault("streams", {}).setdefault("transfers", {})
        filt = {"address": token["address"], "topics": [TRANSFER]}
        first = token["first_seen_block"]
    staged = copy.deepcopy(stream)
    sample = sample_stream(rpc, staged, filt, head, first, repair=(phase == 3 and bool(stream.get("gaps"))))
    if sample is None:
        return
    # Decode before committing; malformed logs keep the previous cursor intact.
    row = {k:v for k,v in sample.items() if k != "logs"}
    row.update(observed_at=now, kind="swaps" if use_swaps else "transfers")
    if use_swaps:
        decoded = [decode_swap(l) for l in sample["logs"]]
        if any(s is None or s["pool_id"] != pool_id for s in decoded):
            raise Deferred("INVALID_SWAP_LOG")
        row.update(pool_id=pool_id, event_count=len(decoded))
        if decoded:
            swap = max(decoded, key=lambda s:(s["block"], s["log_index"]))
            # Keep latest even when replaying old gap activity.
            old = token.get("latest_swaps", {}).get(pool_id)
            if old is None or (swap["block"], swap["log_index"]) > (old["block"], old["log_index"]):
                block = rpc("eth_getBlockByNumber", [hex(swap["block"]), False])
                swap["timestamp"] = int(block["timestamp"], 16)
                token.setdefault("latest_swaps", {})[pool_id] = swap
            row["buy_events"] = sum((s["amount0_raw"] if token["pools"][pool_id]["currency0"] == token["address"] else s["amount1_raw"]) > 0 for s in decoded)
            row["sell_events"] = sum((s["amount0_raw"] if token["pools"][pool_id]["currency0"] == token["address"] else s["amount1_raw"]) < 0 for s in decoded)
    else:
        logs = sample["logs"]
        if any(len(l.get("topics", [])) != 3 or len(l.get("data", "")) != 66 for l in logs):
            raise Deferred("INVALID_TRANSFER_LOG")
        row["event_count"] = len(logs)
        row["distinct_transfer_recipients"] = len({l["topics"][2] for l in logs if int(l["topics"][2],16)})
    stream.clear()
    stream.update(staged)
    if row["event_count"] and not row["repair"]:
        token["last_activity_at"] = now
        token["lane"] = "FOLLOWUP"
    # Keep launch/early observations plus a rolling tail, all labelled by actual ranges.
    token["snapshots"].append(row)
    if len(token["snapshots"]) > 64:
        token["snapshots"] = token["snapshots"][:16] + token["snapshots"][-48:]


def report_token(t, state, now, risks):
    gate = structural_gate(t, risks)
    quotes = []
    for pool_id, swap in t.get("latest_swaps", {}).items():
        age = now-swap["timestamp"]
        quote = executed_quote(t["pools"][pool_id], swap, t["address"],
                               t["metadata"].get("decimals"), state["quotes"].get("decimals"))
        if quote and 0 <= age <= FRESH_SECONDS:
            quotes.append({"pool_id":pool_id, "price_usdg":quote, "age_seconds":age,
                           "swap_tx":swap["tx"], "swap_block":swap["block"]})
    quotes.sort(key=lambda q:q["age_seconds"])
    conflict = len(quotes)>1 and max(Decimal(q["price_usdg"]) for q in quotes) / min(Decimal(q["price_usdg"]) for q in quotes) > Decimal("1.20")
    return {"address":t["address"], "status":gate["status"], "structural_gate":gate,
            "lane":t["lane"], "last_attempt":t["last_attempt"], "next_due":t["next_due"],
            "executed_price":quotes[0] if quotes and not conflict else None,
            "valuation_conflict":conflict, "valuation_sources":quotes,
            "market_cap_status":"MC UNRESOLVED", "market_cap_usd":None,
            "valuation_reason":"Effective supply and USD conversion are not verified; USDG is a quote unit, not a verified USD peg. Executed trade price does not prove current size-dependent exit price.",
            "holders":None, "independent_buyers":None, "liquidity_usd":None,
            "acceleration":"UNRESOLVED", "actionable_alert":None,
            "streams":t.get("streams", {}), "recent_observations":t["snapshots"][-4:],
            "last_error":t.get("last_error")}


def run(candidates, state, collector, risks, rpc, now=None, checkpoint=None):
    now = int(time.time()) if now is None else now
    if state.get("version") != 3:
        raise ValueError("Unsupported analysis state; archive legacy state explicitly before migration")
    ingest(state, candidates, now)
    # Risk updates also demote cold/unselected tokens immediately without RPC.
    for t in state["tokens"].values():
        t["status"] = structural_gate(t, risks)["status"]
    hot = select_hot(state, now)
    reason = "HOT_SET_VISITED"
    attempted = set()
    deferred = set()
    try:
        # Use finalized data to avoid treating an unconfirmed fork as persistent truth.
        block = rpc("eth_getBlockByNumber", ["finalized", False])
        head = int(block["number"], 16)
        if now-int(block["timestamp"],16) > FRESH_SECONDS:
            raise StopRun("FINALIZED_HEAD_STALE")
        # Six rounds, at most one small task per contract per round.
        for _ in range(6):
            for t in hot:
                if t["status"] == "REJECT" or t["address"] in deferred:
                    continue
                if t["address"] not in attempted:
                    t["last_attempt"] = now
                    t["attempts"] += 1
                    t["next_due"] = now+900
                    attempted.add(t["address"])
                try:
                    observe_step(t, state, rpc, head, int(time.time()) if now is None else now)
                    t["last_error"] = None
                    t["status"] = structural_gate(t, risks)["status"]
                except StopRun as exc:
                    t["last_error"] = str(exc)
                    raise
                except (Deferred, KeyError, ValueError, TypeError) as exc:
                    t["last_error"] = str(exc)
                    deferred.add(t["address"])
                if checkpoint:
                    checkpoint(state)
    except StopRun as exc:
        reason = str(exc)
    except (Deferred, KeyError, ValueError, TypeError) as exc:
        reason = "HEAD_UNRESOLVED: " + str(exc)
    finished = int(time.time())
    state["updated_at"] = finished
    output = {"version":3, "generated_at":finished, "stop_reason":reason,
              "rpc_calls":rpc.calls, "runtime_seconds":round(time.monotonic()-rpc.started, 3),
              "collector_coverage":collector.get("coverage_status", "UNRESOLVED"),
              "collector_updated_at":collector.get("updated_at_unix"),
              "collector_age_seconds":max(0, now-collector.get("updated_at_unix", now)),
              "universe_count":len(state["tokens"]), "selected":len(hot), "attempted":len(attempted),
              "due_remaining":sum(t["next_due"] <= now and t["lane"] != "COLD" and t["status"] != "REJECT" for t in state["tokens"].values()),
              "analysis_coverage":"PARTIAL", "alerts":[],
              "tokens":[report_token(t,state,max(now,finished),risks) for t in hot]}
    return output


def main():
    candidates = load("candidates.json", [])
    collector = load("state.json", {})
    state = load("analysis_state.json", fresh_state())
    if state.get("version") != 3:
        # Preserve old snapshots as evidence, but do not trust old global cursors/holder balances.
        save("analysis_state_legacy.json", state)
        state = fresh_state()
    risks = load("risk_evidence.json", {})
    rpc = RPC()
    def deadline(signum, frame):
        raise StopRun("WALL_CLOCK_DEADLINE")
    # Linux workflow hard wall clock including slow response streaming; leave time for final save.
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(MAX_SECONDS)
    try:
        output = run(candidates, state, collector, risks, rpc,
                     checkpoint=lambda s:save("analysis_state.json", s))
    finally:
        signal.alarm(0)
        save("analysis_state.json", state)
    save("latest_analysis.json", output)
    print(json.dumps({k:v for k,v in output.items() if k != "tokens"}))


if __name__ == "__main__":
    main()
