#!/usr/bin/env python3
"""
l1price - a tiny public price + liquidity API for GenesisL1 (L1).

Serves the live L1/USD price, 24h volume, and 24h change, PLUS a keyless
per-pool liquidity map (/pools) for every L1 Osmosis pool. Zero dependencies -
Python 3 standard library only.

Endpoints:
  GET /price       -> JSON {symbol,name,usd,change_24h_pct,vol_24h_usd,liquidity_usd,...}
  GET /price.txt   -> "L1 $0.039672  24h +8.0%  vol $6,109"
  GET /pools       -> JSON {pools:[{id,pair,fee_pct,liquidity_usd,vol_24h_usd,apr_pct,warming_up}],...}
  GET /health      -> "ok"
  GET /            -> short help

Data sources (all public, no API key):
  - price/volume : Osmosis indexer (Numia public) + SQS fallback
  - pool liquidity/fee/pair : Osmosis SQS  (liquidity_cap matches app.osmosis.zone)
  - pool 24h volume : the chain's per-pool CUMULATIVE volume (poolmanager
    total_volume, normalized in uosmo by Osmosis), snapshotted on a background
    thread and diffed over 24h. APR = fee x vol_24h x 365 / liquidity.
    Per-pool volume needs ~24h to warm up (it must bank a baseline snapshot).

Run:
  python3 l1price.py                 # listens on 0.0.0.0:8585
  PORT=9000 python3 l1price.py       # custom port
  POOLS_STATE=/data/pools.json ...   # persist snapshots across restarts (recommended)

CORS is open so browsers / dashboards can call it directly.
"""
import json
import os
import threading
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

# --- L1 liquidity pools (keyless) --------------------------------------------
# Known L1 pool IDs on Osmosis (the dApp's >$ filter hides dust; add IDs here).
POOL_IDS = [732, 2894, 3456, 2849, 2850, 3435, 2947, 3349, 2837]
SQS_POOLS_URL  = "https://sqs.osmosis.zone/pools?IDs=" + ",".join(map(str, POOL_IDS))
LCD_VOL_URL    = "https://lcd.osmosis.zone/osmosis/poolmanager/v1beta1/pools/{}/total_volume"
OSMO_DENOM     = "uosmo"
OSMO_PRICE_URL = "https://sqs.osmosis.zone/tokens/prices?base=uosmo"
# counter-asset denom -> symbol (for pair labels)
SYM = {
    "uosmo": "OSMO",
    "ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2": "ATOM",
    "ibc/498A0751C798A0D9A389AA3691123DADA57DAA4FE165D5C75894505B876BA6E4": "USDC",
    "ibc/E6931F78057F7CC5DA0FD6CEF82FF39373A6E0452BF1FD76910B93292CF356C1": "CRO",
    "ibc/1480B8FD20AD5FCAE81EA87584D269547DD4D436843C1D20F15E00EB64743EF4": "AKT",
    "ibc/D79E7D83AB399BFFF93433E54FAA480C191248FC556924A2A8351AE2638B3877": "TIA",
    "factory/osmo1k6c8jln7ejuqwtqmay3yvzrg3kueaczl96pk067ldg8u835w0yhsw27twm/alloyed/allETH": "ETH",
}

SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1800"))   # 30 min
POOLS_TTL         = int(os.environ.get("POOLS_TTL", "60"))
POOLS_STATE       = os.environ.get("POOLS_STATE", "pools_snapshots.json")
RING_MAX_AGE      = 26 * 3600

_ring = []                      # [{"ts": float, "vols": {pid: uosmo_amount_int}}]
_ring_lock = threading.Lock()
_pools_cache = {"data": None, "ts": 0.0, "ok": False}


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "l1price/3.0"})
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


# --- pools: snapshots + 24h volume -------------------------------------------

def _load_ring():
    global _ring
    try:
        with open(POOLS_STATE) as f:
            _ring = json.load(f)
    except Exception:
        _ring = []


def _save_ring():
    try:
        tmp = POOLS_STATE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_ring, f)
        os.replace(tmp, POOLS_STATE)
    except Exception:
        pass


def _fetch_cumulative_vols():
    """Cumulative volume (uosmo) per pool from the chain's poolmanager."""
    out = {}
    for pid in POOL_IDS:
        try:
            r = _get(LCD_VOL_URL.format(pid))
            amt = sum(int(v.get("amount", 0)) for v in (r.get("volume") or [])
                      if v.get("denom") == OSMO_DENOM)
            out[pid] = amt
        except Exception:
            continue
    return out


def _snapshot_pools():
    vols = _fetch_cumulative_vols()
    if not vols:
        return
    now = time.time()
    with _ring_lock:
        _ring.append({"ts": now, "vols": vols})
        cutoff = now - RING_MAX_AGE
        while len(_ring) > 2 and _ring[0]["ts"] < cutoff:
            _ring.pop(0)
        _save_ring()


def _snapshot_loop():
    while True:
        try:
            _snapshot_pools()
        except Exception:
            pass
        time.sleep(SNAPSHOT_INTERVAL)


def _osmo_price():
    try:
        d = _get(OSMO_PRICE_URL)
        q = d.get(OSMO_DENOM) or {}
        v = q.get(USDC_DENOM) or next(iter(q.values()), None)
        return float(v) if v is not None else None
    except Exception:
        return None


def _sqs_pools():
    """Current liquidity, fee, pair per pool from SQS (keyed by pool id)."""
    out = {}
    try:
        arr = _get(SQS_POOLS_URL)
    except Exception:
        return out
    for p in arr:
        pid = (p.get("chain_model") or {}).get("id")
        bals = p.get("balances") or []
        other = next((b for b in bals if b.get("denom") != L1_DENOM), None)
        if other:
            sym = SYM.get(other["denom"]) or (other["denom"][4:8] + "…" if other["denom"].startswith("ibc/") else other["denom"][:6].upper())
        else:
            sym = "?"
        out[pid] = {
            "pair": "L1/" + sym,
            "fee_pct": float(p.get("spread_factor") or 0) * 100,
            "liquidity_usd": float(p.get("liquidity_cap") or 0),
        }
    return out


def _vol_24h_osmo(pid):
    """24h cumulative-volume delta (uosmo) for a pool; returns (value|None, warming_up)."""
    with _ring_lock:
        if not _ring:
            return None, True
        latest = _ring[-1]
        target = latest["ts"] - 24 * 3600
        baseline = _ring[0]
        for e in _ring:
            if e["ts"] <= target + SNAPSHOT_INTERVAL:
                baseline = e
        age = latest["ts"] - baseline["ts"]
        cur = latest["vols"].get(pid)
        old = baseline["vols"].get(pid)
        warming = age < 23 * 3600
        if warming or cur is None or old is None:
            return None, warming
        return max(0, cur - old), False


def _pools_data():
    pools = _sqs_pools()
    osmo = _osmo_price()
    out = []
    for pid in POOL_IDS:
        info = pools.get(pid)
        if not info:
            continue
        vol_osmo, warming = _vol_24h_osmo(pid)
        vol_usd = (vol_osmo / 1e6 * osmo) if (vol_osmo is not None and osmo) else None
        liq = info["liquidity_usd"]
        apr = (info["fee_pct"] / 100 * vol_usd * 365 / liq * 100) if (vol_usd and liq) else None
        out.append({
            "id": pid,
            "pair": info["pair"],
            "fee_pct": round(info["fee_pct"], 3),
            "liquidity_usd": round(liq, 2),
            "vol_24h_usd": round(vol_usd, 2) if vol_usd is not None else None,
            "apr_pct": round(apr, 1) if apr is not None else None,
            "warming_up": warming,
        })
    out.sort(key=lambda x: x["liquidity_usd"], reverse=True)
    return out


def _pools_payload():
    now = time.time()
    if _pools_cache["ok"] and (now - _pools_cache["ts"]) < POOLS_TTL:
        pools = _pools_cache["data"]
    else:
        pools = _pools_data()
        _pools_cache.update(data=pools, ts=now, ok=True)
    with _ring_lock:
        hrs = round((_ring[-1]["ts"] - _ring[0]["ts"]) / 3600, 1) if len(_ring) > 1 else 0.0
    return {
        "pools": pools,
        "total_liquidity_usd": round(sum(p["liquidity_usd"] for p in pools), 2),
        "total_vol_24h_usd": round(sum((p["vol_24h_usd"] or 0) for p in pools), 2),
        "hours_of_history": hrs,
        "warming_up": (any(p["warming_up"] for p in pools) if pools else True),
        "updated": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    server_version = "l1price/3.0"

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
                    "l1price - GenesisL1 (L1) price + liquidity API\n"
                    "  GET /price      JSON price + 24h volume + 24h change\n"
                    "  GET /price.txt  one-line ticker\n"
                    "  GET /pools      JSON per-pool liquidity + 24h volume + APR\n"
                    "  GET /health     ok\n",
                    "text/plain")
            elif path == "/price":
                self._send(200, json.dumps(_payload(), indent=2))
            elif path == "/price.txt":
                self._send(200, _ticker(_payload()), "text/plain")
            elif path == "/pools":
                self._send(200, json.dumps(_pools_payload(), indent=2))
            elif path == "/health":
                self._send(200, "ok", "text/plain")
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:
            self._send(503, json.dumps({"error": "unavailable", "detail": str(e)}))

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    _load_ring()
    try:
        _snapshot_pools()   # bank an immediate baseline
    except Exception:
        pass
    threading.Thread(target=_snapshot_loop, daemon=True).start()
    print("l1price listening on 0.0.0.0:{}  (cache {}s, snapshot {}s)".format(
        PORT, CACHE_TTL, SNAPSHOT_INTERVAL))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
