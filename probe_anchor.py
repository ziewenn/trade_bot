"""
POLYMARKET ANCHOR PRICE DISCOVERY SCRIPT
=========================================
This script probes every known Polymarket API endpoint to find
where the "Price to Beat" (anchor price) is stored for BTC 5-minute
Up/Down markets.

INSTRUCTIONS FOR CLAUDE CODE:
1. pip install requests websockets --break-system-packages
2. python probe_anchor.py
3. Read ALL output carefully — the anchor price field will be in one of these responses
4. Look for any field containing a BTC dollar value (e.g. 87000-88000 range currently)
5. Report back which endpoint and which field contains the anchor price

This script requires NO API keys — all endpoints are public/read-only.
"""

import requests
import json
import time
import sys
from datetime import datetime, timezone

# ============================================================
# HELPERS
# ============================================================

def pretty_print(label: str, data):
    """Print a labeled JSON response with formatting."""
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    if data is None:
        print("  [No data / request failed]")
        return
    if isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2, default=str))
    else:
        print(str(data)[:5000])
    print()


def safe_get(url: str, params: dict = None, timeout: int = 10) -> dict | list | None:
    """Make a GET request and return parsed JSON, or None on failure."""
    try:
        print(f"  → GET {url}")
        if params:
            print(f"    params: {params}")
        resp = requests.get(url, params=params, timeout=timeout)
        print(f"    status: {resp.status_code}")
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"    body: {resp.text[:500]}")
            return None
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def get_current_window_ts() -> int:
    """Get the Unix timestamp of the current 5-min window start."""
    now = int(time.time())
    return now - (now % 300)


def get_previous_window_ts() -> int:
    """Get the Unix timestamp of the previous (just-ended) 5-min window."""
    return get_current_window_ts() - 300


def get_next_window_ts() -> int:
    """Get the Unix timestamp of the next 5-min window."""
    return get_current_window_ts() + 300


# ============================================================
# PROBE 1: GAMMA API — Events endpoint with slug
# ============================================================

def probe_gamma_events():
    """
    Fetch the current BTC 5-min market from Gamma API.
    This is the primary market discovery endpoint.
    We try current, previous, and next windows.
    """
    print("\n" + "#"*80)
    print("# PROBE 1: GAMMA API — Events by slug")
    print("#"*80)

    base_url = "https://gamma-api.polymarket.com/events"

    for label, ts in [
        ("PREVIOUS window", get_previous_window_ts()),
        ("CURRENT window", get_current_window_ts()),
        ("NEXT window", get_next_window_ts()),
    ]:
        slug = f"btc-updown-5m-{ts}"
        print(f"\n  Trying {label}: slug={slug} (ts={ts})")
        print(f"  Human time: {datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()}")

        data = safe_get(base_url, params={"slug": slug})

        if data:
            pretty_print(f"GAMMA EVENT — {label} — slug={slug}", data)

            # If it's a list, dig into the first event
            if isinstance(data, list) and len(data) > 0:
                event = data[0]
                print(f"\n  >>> EVENT-LEVEL FIELDS (looking for anchor price):")
                for key, val in event.items():
                    if key != "markets":  # Print everything except nested markets
                        val_str = str(val)[:200]
                        print(f"      {key}: {val_str}")

                # Dig into markets
                markets = event.get("markets", [])
                if markets:
                    market = markets[0]
                    print(f"\n  >>> MARKET-LEVEL FIELDS ({len(markets)} markets):")
                    for key, val in market.items():
                        val_str = str(val)[:300]
                        print(f"      {key}: {val_str}")

                    # Specifically check fields that might contain anchor price
                    print(f"\n  >>> CANDIDATE ANCHOR FIELDS:")
                    candidates = [
                        "groupItemThreshold", "startPrice", "anchorPrice",
                        "priceToBeat", "referencePrice", "strike",
                        "templateVariables", "description", "question",
                        "customDescription", "metadata", "xAxisValue",
                        "yAxisValue", "startDateIso", "endDateIso",
                    ]
                    for field in candidates:
                        if field in market:
                            print(f"      *** {field}: {market[field]}")
                        elif field in event:
                            print(f"      *** (event-level) {field}: {event[field]}")

                    # Also look for ANY field containing a number in BTC price range
                    print(f"\n  >>> FIELDS WITH NUMERIC VALUES IN BTC RANGE (50000-120000):")
                    for key, val in market.items():
                        try:
                            num = float(val) if val else 0
                            if 50000 <= num <= 120000:
                                print(f"      *** POSSIBLE ANCHOR: {key} = {val}")
                        except (ValueError, TypeError):
                            pass
                    for key, val in event.items():
                        if key == "markets":
                            continue
                        try:
                            num = float(val) if val else 0
                            if 50000 <= num <= 120000:
                                print(f"      *** POSSIBLE ANCHOR (event): {key} = {val}")
                        except (ValueError, TypeError):
                            pass

            return data  # Return the first successful response for later use
    return None


# ============================================================
# PROBE 2: GAMMA API — Markets endpoint with slug
# ============================================================

def probe_gamma_markets():
    """
    Try the /markets endpoint instead of /events.
    Some fields may differ.
    """
    print("\n" + "#"*80)
    print("# PROBE 2: GAMMA API — Markets by slug")
    print("#"*80)

    base_url = "https://gamma-api.polymarket.com/markets"
    ts = get_current_window_ts()
    slug = f"btc-updown-5m-{ts}"

    # Try multiple slug formats
    for s in [slug, f"will-btc-go-up-5m-{ts}"]:
        data = safe_get(base_url, params={"slug": s})
        if data:
            pretty_print(f"GAMMA MARKETS — slug={s}", data)
            return data

    # Also try without slug, using tag filter
    data = safe_get(base_url, params={
        "tag": "btc-5min",
        "active": "true",
        "closed": "false",
        "limit": "3"
    })
    if data:
        pretty_print("GAMMA MARKETS — tag=btc-5min, active", data)

    return None


# ============================================================
# PROBE 3: CLOB API — crypto-outcomes endpoint (UNDOCUMENTED)
# ============================================================

def probe_clob_crypto_outcomes():
    """
    This is the key endpoint we're hunting.
    The polymarket-apis package documents a method called
    get_crypto_outcomes(slugs=...) that wraps this endpoint.
    """
    print("\n" + "#"*80)
    print("# PROBE 3: CLOB API — crypto-outcomes (UNDOCUMENTED)")
    print("#"*80)

    base_url = "https://clob.polymarket.com"

    ts_current = get_current_window_ts()
    ts_prev = get_previous_window_ts()

    # Try various URL patterns for the crypto-outcomes endpoint
    attempts = [
        # Pattern 1: query parameter
        (f"{base_url}/crypto-outcomes", {"slugs": f"btc-updown-5m-{ts_current}"}),
        # Pattern 2: comma-separated slugs
        (f"{base_url}/crypto-outcomes", {"slugs": f"btc-updown-5m-{ts_current},btc-updown-5m-{ts_prev}"}),
        # Pattern 3: slug (singular)
        (f"{base_url}/crypto-outcomes", {"slug": f"btc-updown-5m-{ts_current}"}),
        # Pattern 4: different base path
        (f"{base_url}/crypto/outcomes", {"slugs": f"btc-updown-5m-{ts_current}"}),
        # Pattern 5: no query params, slug in path
        (f"{base_url}/crypto-outcomes/btc-updown-5m-{ts_current}", None),
        # Pattern 6: try with previous window (resolved market might have more data)
        (f"{base_url}/crypto-outcomes", {"slugs": f"btc-updown-5m-{ts_prev}"}),
    ]

    for url, params in attempts:
        data = safe_get(url, params=params)
        if data:
            pretty_print(f"CLOB CRYPTO-OUTCOMES — SUCCESS!", data)

            # Deep inspect for price fields
            if isinstance(data, dict):
                print(f"\n  >>> ALL FIELDS IN RESPONSE:")
                def print_nested(d, prefix=""):
                    for k, v in d.items():
                        if isinstance(v, dict):
                            print(f"      {prefix}{k}: (dict)")
                            print_nested(v, prefix + "  ")
                        elif isinstance(v, list) and v and isinstance(v[0], dict):
                            print(f"      {prefix}{k}: (list of {len(v)} dicts)")
                            if v:
                                print_nested(v[0], prefix + "  [0].")
                        else:
                            val_str = str(v)[:200]
                            print(f"      {prefix}{k}: {val_str}")
                            # Check for BTC price range
                            try:
                                num = float(v) if v else 0
                                if 50000 <= num <= 120000:
                                    print(f"      *** ^^^ POSSIBLE ANCHOR PRICE ^^^")
                            except (ValueError, TypeError):
                                pass
                print_nested(data)

            elif isinstance(data, list):
                print(f"\n  >>> Response is a list with {len(data)} items")
                for i, item in enumerate(data[:3]):
                    print(f"\n  Item [{i}]:")
                    if isinstance(item, dict):
                        for k, v in item.items():
                            val_str = str(v)[:200]
                            print(f"      {k}: {val_str}")
                            try:
                                num = float(v) if v else 0
                                if 50000 <= num <= 120000:
                                    print(f"      *** ^^^ POSSIBLE ANCHOR PRICE ^^^")
                            except (ValueError, TypeError):
                                pass

            return data

    print("\n  All crypto-outcomes attempts failed. Endpoint may not exist or requires auth.")
    return None


# ============================================================
# PROBE 4: CLOB API — Standard market endpoint
# ============================================================

def probe_clob_markets(gamma_data=None):
    """
    Fetch market data from the CLOB API using condition_id from Gamma.
    """
    print("\n" + "#"*80)
    print("# PROBE 4: CLOB API — Standard /markets endpoint")
    print("#"*80)

    # Extract condition_id from gamma data
    condition_id = None
    if gamma_data:
        events = gamma_data if isinstance(gamma_data, list) else [gamma_data]
        for event in events:
            markets = event.get("markets", [])
            if markets:
                condition_id = markets[0].get("conditionId")
                break

    if condition_id:
        data = safe_get(f"https://clob.polymarket.com/markets/{condition_id}")
        if data:
            pretty_print(f"CLOB MARKET — condition_id={condition_id}", data)

            # Check all fields
            if isinstance(data, dict):
                print(f"\n  >>> CLOB MARKET FIELDS:")
                for k, v in data.items():
                    val_str = str(v)[:300]
                    print(f"      {k}: {val_str}")
                    try:
                        num = float(v) if v else 0
                        if 50000 <= num <= 120000:
                            print(f"      *** ^^^ POSSIBLE ANCHOR PRICE ^^^")
                    except (ValueError, TypeError):
                        pass

                # Check nested tokens
                tokens = data.get("tokens", [])
                if tokens:
                    print(f"\n  >>> TOKENS:")
                    for t in tokens:
                        print(f"      {json.dumps(t, indent=6)}")

            return data
    else:
        print("  No condition_id found from Gamma data. Skipping.")

    return None


# ============================================================
# PROBE 5: RTDS WebSocket — Quick check for Chainlink price
# ============================================================

def probe_rtds_websocket():
    """
    Connect to the Polymarket RTDS WebSocket briefly to get
    the current Chainlink BTC/USD price. This tells us what
    value range to look for in the anchor price fields.

    Uses synchronous approach since this is a probe script.
    """
    print("\n" + "#"*80)
    print("# PROBE 5: RTDS WebSocket — Chainlink BTC/USD current price")
    print("#"*80)

    try:
        import asyncio
        import websockets

        async def get_chainlink_price():
            url = "wss://ws-live-data.polymarket.com"
            print(f"  → Connecting to {url}...")

            async with websockets.connect(url) as ws:
                # Subscribe to Chainlink BTC/USD
                sub_msg = json.dumps({
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": json.dumps({"symbol": "btc/usd"})
                        },
                        {
                            "topic": "crypto_prices",
                            "type": "*",
                            "filters": json.dumps({"symbol": "btcusdt"})
                        }
                    ]
                })
                await ws.send(sub_msg)
                print("  → Subscribed to crypto_prices_chainlink + crypto_prices")
                print("  → Waiting for messages (max 15 seconds)...\n")

                chainlink_price = None
                binance_price = None
                start = time.time()

                while time.time() - start < 15:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(msg)
                        topic = data.get("topic", "")
                        payload = data.get("payload", {})

                        if topic == "crypto_prices_chainlink" and payload.get("symbol") == "btc/usd":
                            chainlink_price = float(payload.get("value", 0))
                            print(f"  CHAINLINK BTC/USD: ${chainlink_price:,.2f}  (ts: {payload.get('timestamp')})")
                            pretty_print("FULL CHAINLINK MESSAGE", data)

                        elif topic == "crypto_prices" and payload.get("symbol") == "btcusdt":
                            binance_price = float(payload.get("value", 0))
                            print(f"  BINANCE BTCUSDT:   ${binance_price:,.2f}  (ts: {payload.get('timestamp')})")

                        # If we got both, we're done
                        if chainlink_price and binance_price:
                            break

                    except asyncio.TimeoutError:
                        await ws.ping()  # Keep alive
                        continue

                if chainlink_price:
                    print(f"\n  >>> CURRENT BTC PRICE RANGE TO LOOK FOR: ${chainlink_price:,.2f}")
                    print(f"  >>> Any anchor price field should be close to this value.")
                    return chainlink_price
                else:
                    print("  No Chainlink price received within timeout.")
                    return None

        return asyncio.run(get_chainlink_price())

    except ImportError:
        print("  websockets not installed. Run: pip install websockets")
        return None
    except Exception as e:
        print(f"  WebSocket error: {e}")
        return None


# ============================================================
# PROBE 6: Polymarket website scrape — event page
# ============================================================

def probe_polymarket_event_page():
    """
    Fetch the actual Polymarket event page.
    The page may contain embedded JSON with the anchor price.
    """
    print("\n" + "#"*80)
    print("# PROBE 6: Polymarket Event Page — embedded data")
    print("#"*80)

    ts = get_current_window_ts()
    url = f"https://polymarket.com/event/btc-updown-5m-{ts}"
    print(f"  → Fetching {url}")

    try:
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (compatible; probe/1.0)"
        })
        print(f"    status: {resp.status_code}")

        if resp.status_code == 200:
            body = resp.text

            # Look for JSON embedded in script tags
            import re
            # Find __NEXT_DATA__ or similar
            matches = re.findall(r'<script[^>]*>.*?window\.__NEXT_DATA__\s*=\s*({.*?})\s*</script>', body, re.DOTALL)
            if matches:
                print(f"  Found __NEXT_DATA__ ({len(matches[0])} chars)")
                try:
                    next_data = json.loads(matches[0])
                    pretty_print("__NEXT_DATA__ (truncated)", str(next_data)[:3000])
                except:
                    print("  Could not parse __NEXT_DATA__")

            # Search for any occurrence of "priceToBeat" or similar
            for keyword in ["priceToBeat", "anchorPrice", "startPrice", "referencePrice",
                           "groupItemThreshold", "price_to_beat", "anchor_price"]:
                idx = body.find(keyword)
                if idx >= 0:
                    snippet = body[max(0,idx-50):idx+200]
                    print(f"\n  >>> FOUND '{keyword}' in page HTML!")
                    print(f"      Context: ...{snippet}...")

    except Exception as e:
        print(f"    ERROR: {e}")


# ============================================================
# PROBE 7: Gamma API — Series endpoint
# ============================================================

def probe_gamma_series():
    """
    Check if the series endpoint has anchor info.
    Series ID 10192 = btc-up-or-down-15m (from the BTC15mAssistant).
    We need to find the 5-min equivalent.
    """
    print("\n" + "#"*80)
    print("# PROBE 7: GAMMA API — Series endpoint")
    print("#"*80)

    # Try to find 5-min series
    for slug in ["btc-up-or-down-5m", "btc-updown-5m", "btc-5min"]:
        data = safe_get("https://gamma-api.polymarket.com/series", params={"slug": slug})
        if data:
            pretty_print(f"GAMMA SERIES — slug={slug}", data)
            return data

    # Also try listing series
    data = safe_get("https://gamma-api.polymarket.com/series", params={"limit": "5"})
    if data:
        pretty_print("GAMMA SERIES — first 5", data)

    return None


# ============================================================
# PROBE 8: Gamma API — events with different filters
# ============================================================

def probe_gamma_tag_search():
    """
    Search for BTC 5-min markets using tag/keyword filters.
    """
    print("\n" + "#"*80)
    print("# PROBE 8: GAMMA API — Search/filter for BTC 5-min")
    print("#"*80)

    # Try search endpoint
    data = safe_get("https://gamma-api.polymarket.com/events", params={
        "tag": "btc-5min",
        "active": "true",
        "closed": "false",
        "limit": "2",
    })
    if data:
        pretty_print("GAMMA EVENTS — tag=btc-5min", data)

    # Also try with different tag
    data = safe_get("https://gamma-api.polymarket.com/events", params={
        "tag": "crypto-5m",
        "active": "true",
        "limit": "2",
    })
    if data:
        pretty_print("GAMMA EVENTS — tag=crypto-5m", data)

    return data


# ============================================================
# MAIN
# ============================================================

def main():
    print("="*80)
    print("  POLYMARKET ANCHOR PRICE DISCOVERY SCRIPT")
    print("  Running at:", datetime.now(timezone.utc).isoformat())
    print("="*80)

    now = int(time.time())
    current_window = get_current_window_ts()
    previous_window = get_previous_window_ts()
    next_window = get_next_window_ts()

    print(f"\n  Current time (UTC):     {datetime.fromtimestamp(now, tz=timezone.utc).isoformat()}")
    print(f"  Current window start:   {current_window} ({datetime.fromtimestamp(current_window, tz=timezone.utc).isoformat()})")
    print(f"  Previous window start:  {previous_window}")
    print(f"  Next window start:      {next_window}")
    print(f"  Current slug:           btc-updown-5m-{current_window}")

    # Run all probes
    # Probe 5 first to know what BTC price range to look for
    btc_price = probe_rtds_websocket()

    # Then probe all REST endpoints
    gamma_data = probe_gamma_events()
    probe_gamma_markets()
    probe_clob_crypto_outcomes()
    probe_clob_markets(gamma_data)
    probe_gamma_series()
    probe_gamma_tag_search()
    probe_polymarket_event_page()

    # Summary
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    if btc_price:
        print(f"  Current BTC price (Chainlink): ${btc_price:,.2f}")
        print(f"  Look for any field in the above responses containing a value")
        print(f"  close to ${btc_price:,.2f} — that's the anchor price field.")
    print(f"\n  The anchor price for each 5-min window should be the BTC price")
    print(f"  at the exact moment the window opened (the 5-min boundary).")
    print(f"  It may differ slightly from the current price if BTC has moved.")
    print(f"\n  KEY ENDPOINTS TO CHECK:")
    print(f"  1. Gamma events response — look for groupItemThreshold, templateVariables")
    print(f"  2. CLOB crypto-outcomes — if it returned data, anchor is likely there")
    print(f"  3. CLOB market response — check all numeric fields")
    print(f"\n  Once you find the field, update the anchor-price-guide.md accordingly.")


if __name__ == "__main__":
    main()
