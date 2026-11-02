"""Find which HIP-3 perps Binance lists as a TradFi perpetual: the HIP-3 external quotes' sources (design
hyperliquid-trade 000664), the one authority of the name mapping.

Writes `server/data/hip3_external_sources.json`, which the server embeds at build time: regenerate, review the diff
coin by coin (underlying, listing venue, share class, contract unit and multiplier) and rebuild for a newly listed coin.

A candidate is a Binance USDT TradFi perpetual (`contractType == TRADIFI_PERPETUAL`, trading) whose `baseAsset` equals
the coin's name exactly (`xyz:TSLA` -> `TSLA`; a prefix never matches: `ANT` is not `ANTHROPIC`), or one written in
`ALIASES` with its reason. Every candidate, an alias too, is kept only when Binance's bookTicker midpoint × the
multiplier is within the tolerance of HL's markPx; every rejected coin is reported with its reason and ratio; nothing is
chosen silently. Binance's futures API is region-restricted: run it from the host that serves the quotes (bm):
    python scripts/discover_hip3_external_sources.py --sources server/data/hip3_external_sources.json --report report.json
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

HL_INFO = "https://api.hyperliquid.xyz/info"
BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_TICKERS = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
VENUE = "binance_perp"
TRADFI = "TRADIFI_PERPETUAL"
# {coin: (Binance symbol, multiplier, why)}: a coin whose Binance base asset is another name for the same underlying.
# Each one passes the same price check as an exact match (11-02 09:1xZ, bm: all within 36 bp at multiplier 1). Left
# out: xyz:KR200 / KODEX200 (an ETF of KOSPI 200, another unit), xyz:SP500 / SPY and xyz:XYZ100 / QQQ (ETFs, not the
# index).
ALIASES: dict[str, tuple[str, str, str]] = {
    "xyz:GOLD": ("XAUUSDT", "1", "gold per troy ounce, ISO 4217 XAU"),
    "xyz:SILVER": ("XAGUSDT", "1", "silver per troy ounce, ISO 4217 XAG"),
    "xyz:PLATINUM": ("XPTUSDT", "1", "platinum per troy ounce, ISO 4217 XPT"),
    "xyz:PALLADIUM": ("XPDUSDT", "1", "palladium per troy ounce, ISO 4217 XPD"),
    "xyz:BRENTOIL": ("BZUSDT", "1", "Brent crude oil per barrel, its futures' code BZ"),
    "xyz:SMSN": ("SAMSUNGUSDT", "1", "Samsung Electronics common share (KRX 005930), in USD"),
    "xyz:SKHX": ("SKHYNIXUSDT", "1", "SK hynix share (KRX 000660), in USD"),
}


def get(url):
    request = urllib.request.Request(url, headers={"User-Agent": "hip3-external-sources"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def post(url, payload):
    request = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json", "User-Agent": "hip3-external-sources"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def hl_marks(dex):
    """{coin: markPx} of the dex's listed perps (`metaAndAssetCtxs`)."""
    meta, contexts = post(HL_INFO, {"type": "metaAndAssetCtxs", "dex": dex})
    return {asset["name"]: Decimal(context["markPx"]) for asset, context in zip(meta["universe"], contexts, strict=True)
            if not asset.get("isDelisted")}


def tradfi_perps():
    """{symbol: (baseAsset, underlyingType)} of Binance's trading USDT TradFi perpetuals."""
    symbols = get(BINANCE_EXCHANGE_INFO)["symbols"]
    return {row["symbol"]: (row["baseAsset"], row["underlyingType"]) for row in symbols
            if row["contractType"] == TRADFI and row["status"] == "TRADING" and row["quoteAsset"] == "USDT"}


def book_tickers():
    """{symbol: (bid, ask)} of every Binance perp."""
    return {row["symbol"]: (Decimal(row["bidPrice"]), Decimal(row["askPrice"])) for row in get(BINANCE_BOOK_TICKERS)}


def candidate(coin, dex, by_base):
    """(symbol, multiplier, how) of the coin's candidate; None without one."""
    if coin in ALIASES:
        symbol, multiplier, why = ALIASES[coin]
        return symbol, Decimal(multiplier), f"alias: {why}"
    symbol = by_base.get(coin.removeprefix(f"{dex}:"))
    return None if symbol is None else (symbol, Decimal(1), "exact")


def judge(mark, quote, multiplier, tolerance):
    """(accepted, reason, ratio of Binance's midpoint × multiplier to HL's markPx)."""
    if quote is None:
        return False, "noBookTicker", None
    bid, ask = quote
    if bid <= 0 or ask <= 0 or ask < bid:
        return False, "noTwoSidedQuote", None
    ratio = (bid + ask) / 2 * multiplier / mark
    if abs(ratio - 1) > tolerance:
        return False, "priceMismatch", ratio
    return True, "accepted", ratio


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--dex", default="xyz")
    parser.add_argument("--tolerance", type=Decimal, default=Decimal("0.01"), help="largest |Binance mid × multiplier / markPx - 1|")
    args = parser.parse_args()
    marks = hl_marks(args.dex)
    perps = tradfi_perps()
    by_base = {base: symbol for symbol, (base, _) in perps.items()}
    quotes = book_tickers()
    coins, report = {}, []
    for coin, mark in sorted(marks.items()):
        found = candidate(coin, args.dex, by_base)
        if found is None:
            report.append({"coin": coin, "reason": "noTradfiPerp", "markPx": str(mark)})
            continue
        symbol, multiplier, how = found
        if symbol not in perps:
            raise SystemExit(f"{coin}: alias symbol {symbol} is not a trading Binance USDT TradFi perpetual")
        accepted, reason, ratio = judge(mark, quotes.get(symbol), multiplier, args.tolerance)
        report.append({"coin": coin, "symbol": symbol, "pxMultiplier": str(multiplier), "match": how,
                       "underlyingType": perps[symbol][1], "reason": reason, "markPx": str(mark),
                       "ratio": None if ratio is None else f"{ratio:.6f}"})
        if accepted:
            coins[coin] = {"symbol": symbol, "pxMultiplier": str(multiplier)}
    generated = {"generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "venue": VENUE,
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
