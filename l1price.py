#!/usr/bin/env python3
"""
l1price - a tiny public price API for GenesisL1 (L1).

Serves the live L1/USD price, 24h volume, and 24h change. Primary source is the
Osmosis indexer (Numia public API), which returns price + volume + change in one
call; the Osmosis SQS router is used as a price-only fallback. Zero dependencies
- Python 3 standard library only.

Endpoints:
  GET /price       -> JSON {symbol,name,usd,change_24h_pct,vol_24h_usd,liquidity_usd,source,updated,age_seconds,stale}
  GET /price.txt   -> "L1 $0.039672  24h +8.0%  vol $6,109"
  GET /health      -> "ok"
  GET /            -> short help

Run:
  python3 l1price.py                 # listens on 0.0.0.0:8585
  PORT=9000 python3 l1price.py       # custom port

CORS is open so browsers / dashboards / Streamlit apps can call it directly.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --- GenesisL1 (L1) on Osmosis ------------------------------------------------
L1_DENOM   = "ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4"
USDC_DENOM = "ibc/498A0751C798A0D9A389AA3691123DADA57DAA4FE165D5C75894505B876BA6E4"  # Noble USDC
NUMIA_URL  = "https://public-osmosis-api.numia.xyz/tokens/v2/L1"   # price + volume + change
SQS_URL    = "https://sqs.osmosis.zone/tokens/prices?base=" + L1_DENOM  # price-only fallback
SUPPLY_URL = "https://api.genesisl1.org/cosmos/bank/v1beta1/supply/by_denom?denom=el1"  # total supply (for mcap)

CACHE_TTL = int(os.environ.get("CACHE_TTL", "30"))
PORT      = int(os.environ.get("PORT", "8585"))
TIMEOUT   = 12

_cache = {"data": None, "ts": 0.0, "ok": False}


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "l1price/2.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _supply_mcap(usd):
    """Best-effort: total L1 supply (from the chain) and market cap = supply x price."""
    try:
        r = _get(SUPPLY_URL)
        supply = int(r["amount"]["amount"]) / 1e18
        return round(supply, 2), round(supply * usd, 2)
    except Exception:
        return None, None


def _fetch():
    """Primary: Numia (price + volume + 24h change). Fallback: SQS (price only)."""
    try:
        arr = _get(NUMIA_URL)
        t = arr[0] if isinstance(arr, list) else arr
        usd = _num(t.get("price"))
        if usd is None:
            raise ValueError("no price in Numia response")
        chg = _num(t.get("price_24h_change"))
        vol = _num(t.get("volume_24h"))
        liq = _num(t.get("liquidity"))
        result = {
            "usd": usd,
            "change_24h_pct": round(chg, 2) if chg is not None else None,
            "vol_24h_usd": round(vol, 2) if vol is not None else None,
            "liquidity_usd": round(liq, 2) if liq is not None else None,
            "source": "osmosis-numia",
        }
    except Exception:
        data = _get(SQS_URL)
        quotes = data.get(L1_DENOM) or {}
        raw = quotes.get(USDC_DENOM) or (next(iter(quotes.values())) if quotes else None)
        if raw is None:
            raise ValueError("no price from Numia or SQS")
        result = {"usd": float(raw), "change_24h_pct": None, "vol_24h_usd": None,
                  "liquidity_usd": None, "source": "osmosis-sqs"}
    result["supply"], result["mcap_usd"] = _supply_mcap(result["usd"])
    return result


def get_data():
    """Return cache, refreshing if stale. Serves last-good on fetch error."""
    now = time.time()
    if _cache["ok"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache
    try:
        _cache.update(data=_fetch(), ts=now, ok=True)
    except Exception:
        if _cache["data"] is None:
            raise
    return _cache


def _payload():
    c = get_data()
    d = c["data"]
    age = round(time.time() - c["ts"], 1)
    return {
        "symbol": "L1",
        "name": "GenesisL1",
        "usd": round(d["usd"], 8),
        "change_24h_pct": d["change_24h_pct"],
        "vol_24h_usd": d["vol_24h_usd"],
        "mcap_usd": d.get("mcap_usd"),
        "supply": d.get("supply"),
        "liquidity_usd": d["liquidity_usd"],
        "source": d["source"],
        "updated": datetime.fromtimestamp(c["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_seconds": age,
        "stale": age > CACHE_TTL,
    }


def _money(v):
    if v >= 1e6:
        return "${:.2f}M".format(v / 1e6)
    if v >= 1e3:
        return "${:,.0f}".format(v)
    return "${:.2f}".format(v)


def _ticker(p):
    s = "L1 ${:.6f}".format(p["usd"])
    if p.get("change_24h_pct") is not None:
        s += "  24h {:+.1f}%".format(p["change_24h_pct"])
    if p.get("vol_24h_usd") is not None:
        s += "  vol " + _money(p["vol_24h_usd"])
    if p.get("mcap_usd") is not None:
        s += "  mcap " + _money(p["mcap_usd"])
    return s


class Handler(BaseHTTPRequestHandler):
    server_version = "l1price/2.0"

    def _send(self, code, body, ctype="application/json"):
        body = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=15")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/")
        try:
            if path in ("", "/"):
                self._send(200,
                    "l1price - GenesisL1 (L1) price API\n"
                    "  GET /price      JSON price + 24h volume + 24h change\n"
                    "  GET /price.txt  one-line ticker\n"
                    "  GET /health     ok\n",
                    "text/plain")
            elif path == "/price":
                self._send(200, json.dumps(_payload(), indent=2))
            elif path == "/price.txt":
                self._send(200, _ticker(_payload()), "text/plain")
            elif path == "/health":
                self._send(200, "ok", "text/plain")
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:
            self._send(503, json.dumps({"error": "price unavailable", "detail": str(e)}))

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print("l1price listening on 0.0.0.0:{}  (cache {}s)".format(PORT, CACHE_TTL))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
