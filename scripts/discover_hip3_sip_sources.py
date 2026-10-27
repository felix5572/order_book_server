"""Find which xyz (HIP-3) perps are US stocks quoted on the SIP: the HIP-3 local marks' sources (design 000535).

Writes `server/data/hip3_sip_sources.json`, which the server embeds at build time: regenerate and rebuild for a newly
listed coin or a delisted stock.

For every xyz perp it reads Alpaca's latest SIP quote of the coin's name (`xyz:TSLA` -> `TSLA`) and keeps it only when
the quote's midpoint is within the tolerance of HL's stored oracle: that catches another listing under the same ticker
and the names that are no US stock (Korean and Japanese shares, commodities, FX, indices: no SIP quote). Every rejected
candidate is reported with its reason and ratio; nothing is chosen silently. Run during US trading hours (the oracle
moves off the stock's last quote when its market is closed), with Alpaca credentials in the environment
(APCA_API_KEY_ID / APCA_API_SECRET_KEY, as the trading system's research tools):
    python scripts/discover_hip3_sip_sources.py --sources server/data/hip3_sip_sources.json --report report.json
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from pathlib import Path

HL_INFO = "https://api.hyperliquid.xyz/info"
ALPACA_LATEST_QUOTES = "https://data.alpaca.markets/v2/stocks/quotes/latest"
DEX = "xyz"
# Alpaca's latest-quotes request takes a symbol list; this many per request.
SYMBOLS_PER_REQUEST = 100


def post(url, payload):
    request = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json", "User-Agent": "hip3-sip-sources"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def xyz_oracles():
    """{coin: stored oracle} of the dex's listed perps (`metaAndAssetCtxs`)."""
    meta, contexts = post(HL_INFO, {"type": "metaAndAssetCtxs", "dex": DEX})
    return {asset["name"]: Decimal(context["oraclePx"]) for asset, context in zip(meta["universe"], contexts, strict=True)
            if not asset.get("isDelisted")}


INVALID_SYMBOL = "invalid symbol: "


def latest_quotes(symbols):
    """({symbol: Alpaca's latest SIP quote}, symbols Alpaca refuses as invalid). A symbol Alpaca does not list is absent
    from the answer; one it calls invalid (`400 invalid symbol: JP225`, 10-27) fails the whole request, so it is set
    aside and the request repeated. Any other error stops with its body."""
    headers = {"APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"],
               "User-Agent": "hip3-sip-sources"}
    quotes, invalid = {}, set()
    for start in range(0, len(symbols), SYMBOLS_PER_REQUEST):
        batch = symbols[start:start + SYMBOLS_PER_REQUEST]
        while batch:
            url = f"{ALPACA_LATEST_QUOTES}?{urllib.parse.urlencode({'feed': 'sip', 'symbols': ','.join(batch)})}"
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
                    quotes.update(json.load(response)["quotes"])
                break
            except urllib.error.HTTPError as error:
                body = error.read().decode()
                refused = body.partition(INVALID_SYMBOL)[2].partition('"')[0]
                if error.code != 400 or refused not in batch:
                    raise SystemExit(f"alpaca latest quotes {batch}: HTTP {error.code} {body[:500]!r}") from error
                invalid.add(refused)
                batch = [symbol for symbol in batch if symbol != refused]
    return quotes, invalid


def judge(oracle, quote, tolerance):
    """(accepted, reason, ratio of the SIP midpoint to the oracle)."""
    if quote is None:
        return False, "noSipQuote", None
    bid, ask = Decimal(str(quote["bp"])), Decimal(str(quote["ap"]))
    if bid <= 0 or ask <= 0 or ask < bid:
        return False, "noTwoSidedQuote", None
    ratio = (bid + ask) / 2 / oracle
    if abs(ratio - 1) > tolerance:
        return False, "priceMismatch", ratio
    return True, "accepted", ratio


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--tolerance", type=Decimal, default=Decimal("0.01"), help="largest |SIP mid / oracle - 1|")
    args = parser.parse_args()
    oracles = xyz_oracles()
    symbols = sorted(coin.removeprefix(f"{DEX}:") for coin in oracles)
    quotes, invalid = latest_quotes(symbols)
    coins, report = {}, []
    for coin, oracle in sorted(oracles.items()):
        symbol = coin.removeprefix(f"{DEX}:")
        accepted, reason, ratio = (False, "invalidSymbol", None) if symbol in invalid else judge(oracle, quotes.get(symbol), args.tolerance)
        report.append({"coin": coin, "symbol": symbol, "reason": reason, "oraclePx": str(oracle),
                       "ratio": None if ratio is None else f"{ratio:.6f}", "quote": quotes.get(symbol)})
        if accepted:
            coins[coin] = {"symbol": symbol}
    generated = {"generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "feed": "sip",
                 "tolerance": str(args.tolerance), "coins": coins}
    args.sources.write_text(json.dumps(generated, indent=1, sort_keys=True) + "\n")
    args.report.write_text(json.dumps(report, indent=1) + "\n")
    by_reason = {}
    for row in report:
        by_reason.setdefault(row["reason"], []).append(row["coin"])
    for reason, names in sorted(by_reason.items()):
        print(f"{reason}: {len(names)}" + ("" if reason == "accepted" else f" {names}"))


if __name__ == "__main__":
    main()
