import unittest

import enricher as e

TOKEN = "0x" + "1" * 40
OTHER = "0x" + "2" * 40
NOW = 2_000_000_000


def analysis(status="UNRESOLVED"):
    return {"generated_at": NOW-2, "analysis_coverage": "PARTIAL", "tokens": [{
        "address": TOKEN, "status": status,
        "structural_gate": {"status": status, "flags": ["KNOWN_BAD"] if status == "REJECT" else [],
                            "unresolved": ["holder_concentration"]},
        "classification": "NEW_LAUNCH", "discovery_sources": ["uniswap_v4_initialize"],
        "discovered_at": NOW-60, "first_seen_block": 10, "last_seen_block": 12,
        "recent_observations": [{"kind": "swaps", "event_count": 2}],
    }]}


def pair(base=TOKEN, chain="robinhood", liquidity=1000):
    return {"chainId": chain, "dexId": "uniswap", "pairAddress": "0xpair", "url": "https://example/pair",
            "baseToken": {"address": base}, "quoteToken": {"address": OTHER},
            "priceUsd": "0.01", "marketCap": 10000, "fdv": 12000,
            "liquidity": {"usd": liquidity}, "pairCreatedAt": (NOW-30)*1000,
            "txns": {"m5": {"buys": 4, "sells": 1}, "h1": {"buys": 6, "sells": 2}},
            "volume": {"m5": 500, "h1": 700}, "priceChange": {"m5": 5, "h1": 20}}


class Client:
    def __init__(self, rows=None, error=None): self.rows=rows or []; self.error=error; self.calls=0
    def lookup(self, token):
        self.calls += 1
        if self.error: raise RuntimeError(self.error)
        return self.rows


class EnricherTests(unittest.TestCase):
    def test_exact_contract_and_chain_filter(self):
        base, quote = e.exact_pairs([pair(), pair(base=OTHER), pair(chain="ethereum"),
                                     dict(pair(base=OTHER), quoteToken={"address": TOKEN})], TOKEN)
        self.assertEqual(len(base), 1); self.assertEqual(len(quote), 1)

    def test_missing_external_data_never_removes_candidate(self):
        out=e.run(analysis(), {}, Client(error="offline"), now=NOW)
        self.assertEqual(len(out["candidates"]), 1)
        self.assertEqual(out["candidates"][0]["review_state"], "NEEDS_MODEL_REVIEW")
        self.assertFalse(out["policy"]["missing_enrichment_is_negative_evidence"])

    def test_structural_reject_is_hard_guard(self):
        packet=e.run(analysis("REJECT"), {}, Client([pair()]), now=NOW)["candidates"][0]
        self.assertEqual(packet["promotion_guard"], "STRUCTURAL_REJECT_BLOCKS_POSITIVE_PROMOTION")
        self.assertIn("KNOWN_BAD", packet["scam_flags"])

    def test_unresolved_is_not_clean_and_metrics_are_context(self):
        packet=e.run(analysis(), {}, Client([pair()]), now=NOW)["candidates"][0]
        dex=packet["enrichment"]["dexscreener"]
        self.assertEqual(packet["promotion_guard"], "STRUCTURAL_UNRESOLVED_OBSERVATION_ONLY")
        self.assertEqual(dex["fresh_market_cap"]["value"], 10000)
        self.assertIn("m5_buys_exceed_sells", dex["helper_context_signals"])
        self.assertIn("not unique", dex["buyers_sellers"]["meaning"])

    def test_previous_observation_delta(self):
        first=e.run(analysis(), {}, Client([pair(liquidity=1000)]), now=NOW-15)
        second=e.run(analysis(), first, Client([pair(liquidity=1250)]), now=NOW)
        dex=second["candidates"][0]["enrichment"]["dexscreener"]
        self.assertEqual(dex["liquidity"]["change_since_previous_run_usd"], 250)
        self.assertIn("indexed_liquidity_retained_since_previous_run", dex["helper_context_signals"])


if __name__ == "__main__": unittest.main()
