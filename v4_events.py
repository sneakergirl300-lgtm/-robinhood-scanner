"""Pure v4 event decoders, retained from the original analyzer."""

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

def signed_word(word, bits=256):
    value = int(word, 16)
    if value >= 1 << (bits - 1):
        value -= 1 << bits
    return value

def word_chunks(data):
    raw = data[2:] if data.startswith("0x") else data
    return [raw[i:i + 64] for i in range(0, len(raw), 64) if len(raw[i:i + 64]) == 64]

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
