#!/usr/bin/env python3
"""
l1price - a tiny public price API for GenesisL1 (L1).

Serves the live L1/USD price, sourced from the Osmosis SQS router
(which internally routes across L1's liquidity pools and returns a
USDC-denominated price). Zero dependencies - Python 3 standard library only.

Endpoints:
  GET /price       -> JSON  {"symbol","usd","source","quote","updated","age_seconds","stale"}
  GET /price.txt   -> "L1 $0.040113"   (one-line ticker)
  GET /health      -> "ok"
  GET /            -> short help

Run:
  python3 l1price.py                 # listens on 0.0.0.0:8585
  PORT=9000 python3 l1price.py       # custom port

CORS is open (Access-Control-Allow-Origin: *) so browsers / dashboards /
Streamlit apps can call it directly.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --- GenesisL1 (L1) on Osmosis ------------------------------------------------
L1_DENOM   = "ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4"
USDC_DENOM = "ibc/498A0751C798A0D9A389AA3691123DADA57DAA4FE165D5C75894505B876BA6E4"  # Noble USDC on Osmosis
SQS_URL    = "https://sqs.osmosis.zone/tokens/prices?base=" + L1_DENOM

CACHE_TTL = int(os.environ.get("CACHE_TTL", "30"))   # seconds
PORT      = int(os.environ.get("PORT", "8585"))
TIMEOUT   = 10

_cache = {"usd": None, "ts": 0.0, "ok": False, "quote": "USDC"}


def _fetch_price():
    """Hit Osmosis SQS and return (usd_float, quote_label)."""
    req = urllib.request.Request(SQS_URL, headers={"User-Agent": "l1price/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        data = json.load(r)
    quotes = data.get(L1_DENOM) or {}
    raw = quotes.get(USDC_DENOM)
    quote = "USDC"
    if raw is None and quotes:                  # fallback: take whatever quote exists
        quote, raw = next(iter(quotes.items()))
    if raw is None:
        raise ValueError("no price found in SQS response")
    return float(raw), quote


def get_price():
    """Return cache dict, refreshing if stale. Serves last-good on fetch error."""
    now = time.time()
    if _cache["ok"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache
    try:
        usd, quote = _fetch_price()
        _cache.update(usd=usd, ts=now, ok=True, quote=quote)
    except Exception as e:
        # keep serving the last good value if we have one; otherwise re-raise
        if _cache["usd"] is None:
            raise
        _cache["last_error"] = str(e)
    return _cache


def _payload():
    c = get_price()
    age = round(time.time() - c["ts"], 1)
    return {
        "symbol": "L1",
        "name": "GenesisL1",
        "usd": round(c["usd"], 8),
        "source": "osmosis-sqs",
        "quote": c["quote"],
        "base_denom": L1_DENOM,
        "updated": datetime.fromtimestamp(c["ts"], timezone.utc)
                            .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_seconds": age,
        "stale": age > CACHE_TTL,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "l1price/1.0"

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
                    "  GET /price      JSON price\n"
                    "  GET /price.txt  one-line ticker\n"
                    "  GET /health     ok\n",
                    "text/plain")
            elif path == "/price":
                self._send(200, json.dumps(_payload(), indent=2))
            elif path == "/price.txt":
                p = _payload()
                self._send(200, "L1 ${:.6f}".format(p["usd"]), "text/plain")
            elif path == "/health":
                self._send(200, "ok", "text/plain")
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:
            self._send(503, json.dumps({"error": "price unavailable", "detail": str(e)}))

    def log_message(self, *args):
        pass  # quiet; rely on systemd/journal if needed


if __name__ == "__main__":
    print("l1price listening on 0.0.0.0:{}  (cache {}s)".format(PORT, CACHE_TTL))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
