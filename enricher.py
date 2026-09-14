#!/usr/bin/env python3
"""Keyless, best-effort enrichment for the bounded Part 2 hot set.

This process never discovers contracts and never calls the Robinhood RPC. External
silence is represented as missing evidence; it never drops a candidate or implies
negative evidence.
"""
import copy
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEXSCREENER_TOKENS = "https://api.dexscreener.com/tokens/v1/{}/{}"
CHAIN_ID = os.getenv("DEXSCREENER_CHAIN_ID", "robinhood").lower()
MAX_SECONDS = 50
MAX_CALLS = 2
TIMEOUT_SECONDS = 25
WINDOWS = ("m5", "h1", "h6", "h24")


def load(path, default):
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else copy.deepcopy(default)


def save(path, data):
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    tmp.replace(p)


def address(value):
    if not isinstance(value, str):
        return None
    value = value.lower()
    if len(value) != 42 or not value.startswith("0x"):
        return None
    try:
        int(value[2:], 16)
    except ValueError:
        return None
    return value


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def nested_number(row, *keys):
    for key in keys:
        if not isinstance(row, dict):
            return None
        row = row.get(key)
    return number(row)


class DexScreener:
    """Small circuit-broken client. A failure only affects enrichment."""
    def __init__(self, seconds=MAX_SECONDS, calls=MAX_CALLS):
        self.started = time.monotonic()
        self.deadline = self.started + min(MAX_SECONDS, max(1, seconds))
        self.limit = min(MAX_CALLS, max(1, calls))
        self.calls = 0
        self.stopped = None

    def _get(self, url):
        if self.stopped:
            raise RuntimeError(self.stopped)
        if self.calls >= self.limit or time.monotonic() >= self.deadline:
            self.stopped = "EXTERNAL_BUDGET_EXHAUSTED"
            raise RuntimeError(self.stopped)
        self.calls += 1
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "robinhood-scanner-enricher/1.0",
        })
        timeout = max(1, min(TIMEOUT_SECONDS, self.deadline-time.monotonic()))
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read(2_000_001)
                if len(raw) > 2_000_000:
                    raise ValueError("DEXSCREENER_RESPONSE_TOO_LARGE")
                payload = json.loads(raw)
            if not isinstance(payload, list):
                raise ValueError("DEXSCREENER_INVALID_PAIRS")
            return payload
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                self.stopped = "DEXSCREENER_RATE_LIMIT"
            raise RuntimeError(self.stopped or "DEXSCREENER_HTTP_" + str(exc.code)) from exc
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("DEXSCREENER_" + type(exc).__name__) from exc

    def lookup_many(self, token_addresses, chain_id=CHAIN_ID):
        """The documented endpoint accepts up to 30 comma-separated contracts."""
        if len(token_addresses) > 30:
            raise ValueError("DEXSCREENER_BATCH_TOO_LARGE")
        joined = ",".join(urllib.parse.quote(a, safe="") for a in token_addresses)
        url = DEXSCREENER_TOKENS.format(urllib.parse.quote(chain_id, safe=""), joined)
        rows = self._get(url)
        result = {a: [] for a in token_addresses}
        for row in rows:
            base = address((row.get("baseToken") or {}).get("address")) if isinstance(row, dict) else None
            quote = address((row.get("quoteToken") or {}).get("address")) if isinstance(row, dict) else None
            if base in result:
                result[base].append(row)
            if quote in result and quote != base:
                result[quote].append(row)
        return result


def exact_pairs(rows, token_address, chain_id=CHAIN_ID):
    """Search is transport only: accept data solely after exact identity checks."""
    token_address = address(token_address)
    base, quote = [], []
    for row in rows:
        if not isinstance(row, dict) or str(row.get("chainId", "")).lower() != chain_id:
            continue
        base_address = address((row.get("baseToken") or {}).get("address"))
        quote_address = address((row.get("quoteToken") or {}).get("address"))
        if base_address == token_address:
            base.append(row)
        elif quote_address == token_address:
            quote.append(row)
    return base, quote


def primary_pair(pairs):
    def rank(row):
        return (nested_number(row, "liquidity", "usd") or -1,
                nested_number(row, "volume", "h1") or -1,
                nested_number(row, "volume", "h24") or -1,
                number(row.get("pairCreatedAt")) or -1)
    return max(pairs, key=rank) if pairs else None


def totals(pairs, section, field=None):
    result = {}
    for window in WINDOWS:
        values = []
        for pair in pairs:
            row = pair.get(section) or {}
            value = (row.get(window) or {}).get(field) if field else row.get(window)
            value = number(value)
            if value is not None:
                values.append(value)
        result[window] = sum(values) if values else None
    return result


def confidence(value, basis, level="MEDIUM"):
    return {"value": value, "confidence": level if value is not None else "MISSING",
            "basis": basis if value is not None else "source_returned_no_value"}


def prior_snapshot(previous, token_address):
    for packet in previous.get("candidates", []) if isinstance(previous, dict) else []:
        if address(packet.get("contract")) == token_address:
            return ((packet.get("enrichment") or {}).get("dexscreener") or {}).get("snapshot")
    return None


def delta(current, previous):
    if current is None or previous is None:
        return None
    return current - previous


def dexscreener_evidence(rows, token_address, now, previous=None, chain_id=CHAIN_ID):
    pairs, quote_matches = exact_pairs(rows, token_address, chain_id)
    primary = primary_pair(pairs)
    if not primary:
        return {
            "status": "NO_EXACT_BASE_PAIR",
            "source": {"name": "DexScreener", "endpoint": "chain-scoped exact-contract batch",
                       "observed_at": now, "confidence": "MISSING"},
            "exact_base_pair_count": 0, "exact_quote_pair_count": len(quote_matches),
            "snapshot": None,
        }

    buys, sells = totals(pairs, "txns", "buys"), totals(pairs, "txns", "sells")
    volume = totals(pairs, "volume")
    liquidity_total = sum(v for v in (nested_number(p, "liquidity", "usd") for p in pairs)
                          if v is not None)
    liquidity_total = liquidity_total if any(nested_number(p, "liquidity", "usd") is not None for p in pairs) else None
    market_cap = number(primary.get("marketCap"))
    fdv = number(primary.get("fdv"))
    valuation = market_cap if market_cap is not None else fdv
    valuation_basis = "marketCap" if market_cap is not None else "fdv_fallback"
    created_ms = number(primary.get("pairCreatedAt"))
    snapshot = {
        "observed_at": now,
        "primary_pair": primary.get("pairAddress"),
        "price_usd": primary.get("priceUsd"),
        "market_cap_usd": market_cap,
        "fdv_usd": fdv,
        "liquidity_total_usd": liquidity_total,
        "volume": volume,
        "buys": buys,
        "sells": sells,
    }
    previous = previous if isinstance(previous, dict) else {}
    old_volume, old_buys = previous.get("volume") or {}, previous.get("buys") or {}
    changes = {
        "liquidity_usd": delta(liquidity_total, number(previous.get("liquidity_total_usd"))),
        "volume_m5": delta(volume["m5"], number(old_volume.get("m5"))),
        "buys_m5": delta(buys["m5"], number(old_buys.get("m5"))),
    }
    price_changes = primary.get("priceChange") if isinstance(primary.get("priceChange"), dict) else {}
    profile = primary.get("info") if isinstance(primary.get("info"), dict) else {}
    signals = []
    if buys["m5"] is not None and sells["m5"] is not None and buys["m5"] > sells["m5"]:
        signals.append("m5_buys_exceed_sells")
    if volume["m5"] is not None and volume["h1"] not in (None, 0) and volume["m5"] * 12 > volume["h1"]:
        signals.append("m5_volume_pace_above_h1_average")
    if changes["buys_m5"] is not None and changes["buys_m5"] > 0:
        signals.append("m5_buys_increased_since_previous_run")
    if changes["liquidity_usd"] is not None and changes["liquidity_usd"] >= 0:
        signals.append("indexed_liquidity_retained_since_previous_run")
    return {
        "status": "AVAILABLE",
        "source": {"name": "DexScreener", "endpoint": "chain-scoped exact-contract batch",
                   "observed_at": now, "confidence": "THIRD_PARTY_AGGREGATE",
                   "identity_match": "exact_base_token_and_chain"},
        "chain_id": primary.get("chainId"), "dex_id": primary.get("dexId"),
        "primary_pair": primary.get("pairAddress"), "pair_url": primary.get("url"),
        "exact_base_pair_count": len(pairs), "exact_quote_pair_count": len(quote_matches),
        "pair_created_at": int(created_ms/1000) if created_ms is not None else None,
        "pair_age_seconds": max(0, now-int(created_ms/1000)) if created_ms is not None else None,
        "fresh_market_cap": confidence(valuation, valuation_basis,
                                       "MEDIUM" if market_cap is not None else "LOW"),
        "fdv_usd": confidence(fdv, "DexScreener fdv"),
        "price_trajectory": {
            "price_usd": confidence(primary.get("priceUsd"), "primary exact-base pair"),
            "change_percent": {w: number(price_changes.get(w)) for w in WINDOWS},
            "previous_price_usd": previous.get("price_usd"),
        },
        "liquidity": {
            "primary_pair_usd": confidence(nested_number(primary, "liquidity", "usd"), "primary pair"),
            "all_exact_base_pairs_usd": confidence(liquidity_total, "sum of exact-base pairs"),
            "change_since_previous_run_usd": changes["liquidity_usd"],
        },
        "buyers_sellers": {"buys": buys, "sells": sells,
                           "meaning": "aggregated swap counts; not unique or independent wallets"},
        "volume_flow": {"volume_usd": volume,
                        "meaning": "aggregated indexed volume; not a quality decision by itself"},
        "social_profile": {"websites": profile.get("websites") or [],
                           "socials": profile.get("socials") or [],
                           "confidence": "SELF_REPORTED_PROFILE_ONLY"},
        "helper_context_signals": signals,
        "changes_since_previous_run": changes,
        "snapshot": snapshot,
    }


def packet(token, dex, now):
    token_address = address(token.get("address"))
    gate = token.get("structural_gate") if isinstance(token.get("structural_gate"), dict) else {}
    gate_status = gate.get("status") or token.get("status") or "UNRESOLVED"
    unresolved = list(gate.get("unresolved") or [])
    unresolved.extend(["holder_growth", "independent_repeat_buyers", "creator_linkage",
                       "liquidity_lock_or_removal_control", "sellability_and_tax",
                       "fomo_attention", "independent_social_attention"])
    if dex.get("status") != "AVAILABLE":
        unresolved.extend(["dexscreener_market_cap", "dexscreener_price",
                           "dexscreener_liquidity", "dexscreener_flow"])
    flags = list(gate.get("flags") or [])
    guard = ("STRUCTURAL_REJECT_BLOCKS_POSITIVE_PROMOTION" if gate_status == "REJECT" else
             "STRUCTURAL_UNRESOLVED_OBSERVATION_ONLY" if gate_status == "UNRESOLVED" else
             "STRUCTURAL_GATE_REPORTED_PASS")
    discovered = number(token.get("discovered_at"))
    return {
        "contract": token_address,
        "review_state": "NEEDS_MODEL_REVIEW",
        "promotion_guard": guard,
        "discovery": {
            "age_seconds": max(0, now-discovered) if discovered else None,
            "discovered_at": discovered,
            "classification": token.get("classification", "UNRESOLVED"),
            "sources": token.get("discovery_sources") or [],
            "first_seen_block": token.get("first_seen_block"),
            "last_seen_block": token.get("last_seen_block"),
        },
        "raw_chain_evidence": {
            "source": "analyzer.py bounded exact-contract observations",
            "lane": token.get("lane"), "last_attempt": token.get("last_attempt"),
            "executed_price": token.get("executed_price"),
            "valuation_conflict": token.get("valuation_conflict"),
            "valuation_sources": token.get("valuation_sources") or [],
            "recent_observations": token.get("recent_observations") or [],
            "streams": token.get("streams") or {}, "last_error": token.get("last_error"),
        },
        "enrichment": {"dexscreener": dex,
                       "fomo": {"status": "NOT_IMPLEMENTED", "evidence": None},
                       "public_scam_trackers": {"status": "NOT_IMPLEMENTED", "evidence": None}},
        "holder_growth": {"value": None, "confidence": "MISSING"},
        "concentration_creator_risk": {
            "structural_status": gate_status, "source": gate.get("risk_source"),
            "flags": flags, "unresolved": gate.get("unresolved") or [],
        },
        "scam_flags": flags,
        "unresolved_checks": sorted(set(unresolved)),
        "model_review": {
            "required": True, "state": "NEEDS_MODEL_REVIEW",
            "allowed_decisions": ["REJECT", "EMERGING", "SCOUT", "CONFIRMED", "COLD / NO ACTION"],
            "hard_rule": "REJECT must be returned when structural status is REJECT; UNRESOLVED is never clean.",
            "judgment_focus": ["buyer acceleration", "independent or repeat buyers when known",
                               "holder growth", "liquidity growth and retention",
                               "improving distribution", "buy-flow quality",
                               "participation growth versus valuation",
                               "consecutive strengthening observations"],
            "anti_signal": "Raw volume or transaction count must not dominate judgment.",
        },
    }


def run(analysis, previous, client, now=None):
    now = int(time.time()) if now is None else now
    packets = []
    errors = []
    tokens = analysis.get("tokens", [])
    valid_addresses = [address(t.get("address")) for t in tokens]
    valid_addresses = [a for a in valid_addresses if a]
    batch_error = None
    rows_by_address = {}
    if hasattr(client, "lookup_many") and valid_addresses:
        try:
            rows_by_address = client.lookup_many(valid_addresses)
        except (RuntimeError, ValueError) as exc:
            batch_error = str(exc)
    for token in tokens:
        token_address = address(token.get("address"))
        if not token_address:
            errors.append({"contract": token.get("address"), "error": "INVALID_CONTRACT_ADDRESS"})
            continue
        try:
            if batch_error:
                raise RuntimeError(batch_error)
            rows = (rows_by_address.get(token_address, []) if hasattr(client, "lookup_many")
                    else client.lookup(token_address))
            dex = dexscreener_evidence(rows, token_address, now,
                                       prior_snapshot(previous, token_address))
        except RuntimeError as exc:
            dex = {"status": "UNAVAILABLE", "error": str(exc),
                   "source": {"name": "DexScreener", "endpoint": "chain-scoped exact-contract batch",
                              "observed_at": now, "confidence": "MISSING"}, "snapshot": None}
            errors.append({"contract": token_address, "error": str(exc)})
        packets.append(packet(token, dex, now))
    return {
        "version": 1, "generated_at": now, "state": "NEEDS_MODEL_REVIEW",
        "pipeline": "DISCOVERY -> FAST_OBSERVATION -> ENRICHMENT -> STRUCTURAL_GATE -> MODEL_REVIEW",
        "input": {"latest_analysis_generated_at": analysis.get("generated_at"),
                  "analysis_stop_reason": analysis.get("stop_reason"),
                  "analysis_coverage": analysis.get("analysis_coverage"),
                  "candidate_count": len(analysis.get("tokens", []))},
        "policy": {"enrichment_is_discovery_gate": False,
                   "missing_enrichment_is_negative_evidence": False,
                   "structural_reject_blocks_positive_promotion": True,
                   "structural_unresolved_is_clean": False},
        "external_calls": getattr(client, "calls", None), "enrichment_errors": errors,
        "candidates": packets,
    }


def main():
    analysis = load("latest_analysis.json", {"tokens": []})
    previous = load("decision_candidates.json", {})
    client = DexScreener()
    output = run(analysis, previous, client)
    save("decision_candidates.json", output)
    print(json.dumps({"state": output["state"], "candidates": len(output["candidates"]),
                      "external_calls": output["external_calls"],
                      "enrichment_errors": len(output["enrichment_errors"])}))


if __name__ == "__main__":
    main()
