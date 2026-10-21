"""Find each HL main-dex perp's spot and perp listings on the local mark venues (public data only).

Writes `server/data/mark_sources.json`, which the server embeds at build time (design 000454): regenerate
and rebuild for a newly listed coin or a delisted market.

For every listed, tradable USDT market of a venue it reads the current best bid and ask, and keeps a
listing for an HL coin only when its mid times the price multiplier is within the tolerance of HL's
own price (the stored oracle for spot, the mark for perps). That catches another asset under the same
ticker and lot multipliers (HL kPEPE is 1,000 PEPE; Binance 1000PEPEUSDT already is). Every rejected
candidate is reported with its ratio; nothing is chosen silently.

Run where every venue answers (bm: Binance and Bybit refuse some regions):
    python scripts/discover_mark_sources.py --sources server/data/mark_sources.json --report report.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

import websockets

SPOT_VENUES = ("binance", "okx", "bybit", "kraken", "kucoin", "gate", "mexc")
PERP_VENUES = ("binance", "okx", "bybit", "gate", "mexc")
HL_INFO = "https://api.hyperliquid.xyz/info"
# Not priced from these venues yet (user 10-21: later). Their perps are USDC-quoted, the venues' quotes USDT
# (bm 10-21: their spot medians stood 6.8 / 8.4 bp above HL's oracle, the other coins' median -0.8 bp).
EXCLUDED = {"HYPE": "usdcQuotedPerp", "PURR": "usdcQuotedPerp"}


def get(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json", "User-Agent": "hl-mark-prices"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def quotes(rows):
    """{base: (symbol, bid, ask)} of rows (base, symbol, bid, ask); a side without a price drops the row."""
    out = {}
    for base, symbol, bid, ask in rows:
        if bid in (None, "", "0") or ask in (None, "", "0"):
            continue
        if base in out:
            raise ValueError(f"two markets for base {base}: {out[base][0]} and {symbol}")
        out[base] = (symbol, Decimal(str(bid)), Decimal(str(ask)))
    return out


def binance_spot():
    listed = {s["symbol"]: s["baseAsset"] for s in get("https://api.binance.com/api/v3/exchangeInfo")["symbols"]
              if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"}
    tickers = get("https://api.binance.com/api/v3/ticker/bookTicker")
    return quotes((listed[t["symbol"]], t["symbol"], t["bidPrice"], t["askPrice"]) for t in tickers if t["symbol"] in listed)


def binance_perp():
    listed = {s["symbol"]: s["baseAsset"] for s in get("https://fapi.binance.com/fapi/v1/exchangeInfo")["symbols"]
              if s["status"] == "TRADING" and s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT"}
    tickers = get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
    return quotes((listed[t["symbol"]], t["symbol"], t["bidPrice"], t["askPrice"]) for t in tickers if t["symbol"] in listed)


def okx(inst_type, suffix):
    listed = {i["instId"]: i["instId"].removesuffix(suffix)
              for i in get(f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}")["data"]
              if i["state"] == "live" and i["instId"].endswith(suffix)}
    tickers = get(f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}")["data"]
    return quotes((listed[t["instId"]], t["instId"], t["bidPx"], t["askPx"]) for t in tickers if t["instId"] in listed)


def bybit(category, contract_type):
    rows = get(f"https://api.bybit.com/v5/market/instruments-info?category={category}&limit=1000")["result"]["list"]
    listed = {r["symbol"]: r["baseCoin"] for r in rows
              if r["status"] == "Trading" and r["quoteCoin"] == "USDT" and (contract_type is None or r["contractType"] == contract_type)}
    tickers = get(f"https://api.bybit.com/v5/market/tickers?category={category}")["result"]["list"]
    return quotes((listed[t["symbol"]], t["symbol"], t["bid1Price"], t["ask1Price"]) for t in tickers if t["symbol"] in listed)


def kucoin_spot():
    listed = {s["symbol"]: s["baseCurrency"] for s in get("https://api.kucoin.com/api/v2/symbols")["data"]
              if s["enableTrading"] and s["quoteCurrency"] == "USDT"}
    tickers = get("https://api.kucoin.com/api/v1/market/allTickers")["data"]["ticker"]
    return quotes((listed[t["symbol"]], t["symbol"], t["buy"], t["sell"]) for t in tickers if t["symbol"] in listed)


def gate_spot():
    listed = {p["id"]: p["base"] for p in get("https://api.gateio.ws/api/v4/spot/currency_pairs")
              if p["trade_status"] == "tradable" and p["quote"] == "USDT"}
    tickers = get("https://api.gateio.ws/api/v4/spot/tickers")
    return quotes((listed[t["currency_pair"]], t["currency_pair"], t["highest_bid"], t["lowest_ask"]) for t in tickers if t["currency_pair"] in listed)


def gate_perp():
    listed = {c["name"]: c["name"].removesuffix("_USDT") for c in get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
              if not c["in_delisting"] and c["name"].endswith("_USDT")}
    tickers = get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
    return quotes((listed[t["contract"]], t["contract"], t["highest_bid"], t["lowest_ask"]) for t in tickers if t["contract"] in listed)


def mexc_spot():
    listed = {s["symbol"]: s["baseAsset"] for s in get("https://api.mexc.com/api/v3/exchangeInfo")["symbols"]
              if s["status"] == "1" and s["isSpotTradingAllowed"] and s["quoteAsset"] == "USDT"}
    tickers = get("https://api.mexc.com/api/v3/ticker/bookTicker")
    return quotes((listed[t["symbol"]], t["symbol"], t["bidPrice"], t["askPrice"]) for t in tickers if t["symbol"] in listed)


def mexc_perp():
    listed = {c["symbol"]: c["baseCoin"] for c in get("https://contract.mexc.com/api/v1/contract/detail")["data"]
              if c["state"] == 0 and c["quoteCoin"] == "USDT" and c["settleCoin"] == "USDT"}
    tickers = get("https://contract.mexc.com/api/v1/contract/ticker")["data"]
    return quotes((listed[t["symbol"]], t["symbol"], t["bid1"], t["ask1"]) for t in tickers if t["symbol"] in listed)


async def kraken_spot_async():
    """v2 names (`BTC/USDT`, not REST's `XBT/USDT`): the instrument list, then one ticker snapshot."""
    async with websockets.connect("wss://ws.kraken.com/v2", max_size=2**24) as ws:
        await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "instrument", "snapshot": True}}))
        while True:
            message = json.loads(await ws.recv())
            if message.get("channel") == "instrument" and message.get("type") == "snapshot":
                break
        symbols = {p["symbol"]: p["base"] for p in message["data"]["pairs"] if p["quote"] == "USDT" and p["status"] == "online"}
        await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "ticker", "symbol": sorted(symbols), "snapshot": True}}))
        seen = {}
        deadline = time.monotonic() + 20
        while len(seen) < len(symbols) and time.monotonic() < deadline:
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            if message.get("channel") == "ticker":
                for row in message["data"]:
                    seen[row["symbol"]] = (symbols[row["symbol"]], row["symbol"], row["bid"], row["ask"])
        return quotes(seen.values())


def kraken_spot():
    return asyncio.run(kraken_spot_async())


SPOT = {"binance": binance_spot, "okx": lambda: okx("SPOT", "-USDT"), "bybit": lambda: bybit("spot", None),
        "kraken": kraken_spot, "kucoin": kucoin_spot, "gate": gate_spot, "mexc": mexc_spot}
PERP = {"binance": binance_perp, "okx": lambda: okx("SWAP", "-USDT-SWAP"), "bybit": lambda: bybit("linear", "LinearPerpetual"),
        "gate": gate_perp, "mexc": mexc_perp}


def candidates(coin):
    """(venue base, multiplier): HL price = multiplier × venue price. HL's `k` coins are 1,000 units."""
    if coin.startswith("k") and coin[1:].isupper():
        base = coin[1:]
        return [(base, Decimal(1000)), (f"1000{base}", Decimal(1))]
    return [(coin, Decimal(1)), (f"1000{coin}", Decimal("0.001")), (f"1000000{coin}", Decimal("0.000001"))]


def match(coin, venue_quotes, reference, tolerance):
    """The listing whose scaled mid is within `tolerance` of `reference`, and every candidate seen."""
    seen = []
    for base, multiplier in candidates(coin):
        if base not in venue_quotes:
            continue
        symbol, bid, ask = venue_quotes[base]
        ratio = (bid + ask) / 2 * multiplier / reference
        seen.append({"symbol": symbol, "multiplier": str(multiplier), "ratio": float(ratio)})
        if abs(ratio - 1) <= tolerance:
            return {"symbol": symbol, "pxMultiplier": str(multiplier), "ratio": float(ratio)}, seen
    return None, seen


def hl_universe():
    meta, contexts = get(HL_INFO, {"type": "metaAndAssetCtxs"})
    coins = {}
    for asset, context in zip(meta["universe"], contexts):
        if asset.get("isDelisted"):
            continue
        coins[asset["name"]] = {"szDecimals": asset["szDecimals"], "oraclePx": Decimal(context["oraclePx"]), "markPx": Decimal(context["markPx"])}
    return coins


def discover(tolerance):
    hl = hl_universe()
    spot = {venue: fetch() for venue, fetch in SPOT.items()}
    perp = {venue: fetch() for venue, fetch in PERP.items()}
    sources, report = {}, {}
    for coin, prices in sorted(hl.items()):
        if coin in EXCLUDED:
            report[coin] = {"excluded": EXCLUDED[coin]}
            continue
        entry, notes = {"spot": {}, "perp": {}}, {"spot": {}, "perp": {}}
        ratios = {"spot": {}, "perp": {}}
        for kind, venues, reference in (("spot", spot, prices["oraclePx"]), ("perp", perp, prices["markPx"])):
            for venue, venue_quotes in venues.items():
                listing, seen = match(coin, venue_quotes, reference, tolerance)
                if listing is not None:
                    ratios[kind][venue] = listing.pop("ratio")
                    entry[kind][venue] = listing
                elif seen:
                    notes[kind][venue] = {"rejected": seen}
        # the reference needs the oracle (spot) and the external price (perps): design 000454 r2 revision 2
        if not entry["spot"] or not entry["perp"]:
            report[coin] = {"excluded": "noSpotListing" if not entry["spot"] else "noPerpListing", "rejected": notes}
            continue
        sources[coin] = entry
        # the scaled mid over HL's price at the read: a check of the mapping, not of the oracle rule
        report[coin] = {"spot": sorted(entry["spot"]), "perp": sorted(entry["perp"]), "ratios": ratios, "rejected": notes}
    return sources, report


def main():
    parser = argparse.ArgumentParser(description="Discover per-coin mark sources from public venue listings")
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--tolerance", type=float, default=0.03)
    args = parser.parse_args()
    sources, report = discover(Decimal(str(args.tolerance)))
    generated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    header = {"generatedAt": generated, "tolerance": args.tolerance, "spotVenues": SPOT_VENUES, "perpVenues": PERP_VENUES}
    args.sources.write_text(json.dumps({**header, "coins": sources}, indent=1, sort_keys=True) + "\n")
    args.report.write_text(json.dumps({**header, "coins": report}, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
