#!/usr/bin/env python3
"""
l1price - a tiny public price + liquidity API for GenesisL1 (L1).

Serves the live L1/USD price, 24h volume, and 24h change, PLUS a keyless
per-pool liquidity map (/pools) for every L1 Osmosis pool. Zero dependencies -
Python 3 standard library only.

As of v4.0 the headline price is CROSS-VENUE: the L1/USD price is a
liquidity-weighted blend of Osmosis (Numia/SQS) and Base (Uniswap wL1/USDC via
DexScreener), so the deepest market dominates. A 2M-L1 Base pool therefore
outweighs the thin Osmosis pools, matching the L1 Liquidity Map dApp.

Endpoints:
  GET /price       -> JSON {symbol,name,usd,change_24h_pct,vol_24h_usd,liquidity_usd,venues,...}
  GET /price.txt   -> "L1 $0.039672  24h +8.0%  vol $6,109"
  GET /pools       -> JSON {pools:[{id,pair,fee_pct,liquidity_usd,vol_24h_usd,apr_pct,warming_up}],...}
  GET /health      -> "ok"
  GET /            -> short help

Data sources (all public, no API key):
  - Osmosis price/volume : Osmosis indexer (Numia public) + SQS fallback
  - Base price/liquidity  : the Uniswap v3 pool contract, read over a public Base RPC
  - Base 24h volume       : Swap events scanned incrementally on the snapshot thread
  - headline usd : liquidity-weighted blend of the venues above
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

# --- GenesisL1 (L1) on Base ---------------------------------------------------
# Wrapped L1 on Base (address from M's explorer sidebar -> basescan). DexScreener
# indexes every Base DEX, keyless + CORS-open; returns {"pairs":null} if no pool.
WL1_BASE       = "0xe6522a891702cd2e8cc2a5182638c9da1dd44b22"
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/" + WL1_BASE

# ON-CHAIN Base read. Added 2026-08-10 after the community reported the published price
# tracking Osmosis only.
#
# DexScreener indexes NOTHING for wL1 — verified on /latest/dex/tokens and /tokens/v1,
# Base and Ethereum, all empty. The token address was never wrong; DexScreener simply does
# not carry this token while Dextools does. So _fetch_base() returned None, `venues.base`
# was null, and the liquidity-weighted blend collapsed to Osmosis alone. Osmosis held
# ~$11k of liquidity against Base's ~$148k, so the headline was being set by the venue
# with ~7% of the depth, and read ~6% low.
#
# Reading the pool contract removes the indexer from the loop. It cannot be de-listed,
# rate-limited or silently changed, which is also the posture Mike wants for GenesisL1
# infrastructure: "not rely on any 3rd party".
WL1_POOL  = os.environ.get("WL1_POOL", "0xde9ba4084bf0733a9d69e42e71e5fd140179df71")
BASE_RPCS = [u for u in os.environ.get(
    "BASE_RPCS",
    "https://mainnet.base.org,https://base-rpc.publicnode.com,"
    "https://base.llamarpc.com,https://1rpc.io/base").split(",") if u.strip()]
# BASE 24h VOLUME, by scanning Swap events.
#
# The first cut reported Base volume as None and labelled the total "Osmosis only". That
# was needless: this service ALREADY banks periodic snapshots and diffs them over 24h for
# the Osmosis pools (_ring / _snapshot_loop / _vol_24h_osmo). The same machinery works
# here, so the honest answer to "can we do cross-venue volume" was yes.
#
# Method: on each snapshot tick, fetch Swap logs emitted since the last scan — ~900 Base
# blocks at 30-minute intervals, small enough for any public RPC — and bank the USD amount
# as one bucket. 24h volume is the sum of the buckets inside the window.
#
# Swap events give the EXACT traded amount, so this is not an estimate. The alternative,
# diffing feeGrowthGlobal0X128, would have to back out volume through in-range liquidity
# that moves during the window; it is an approximation where this is exact, and no cheaper.
#
# CONFIRMATION LAG: the scan stops short of the chain head. A reorg that reverts a swap we
# had already banked would silently inflate volume forever, since buckets are never revised.
BASE_SWAP_TOPIC   = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
BASE_VOL_CHUNK    = int(os.environ.get("BASE_VOL_CHUNK", "3000"))    # blocks per getLogs
BASE_VOL_LAG      = int(os.environ.get("BASE_VOL_LAG", "12"))        # reorg safety margin
BASE_VOL_BACKFILL = os.environ.get("BASE_VOL_BACKFILL", "1") != "0"
_base_vol = {"buckets": [], "last_block": 0}   # [{"ts","from","to","usd"}]
_base_vol_lock = threading.Lock()

_SEL = {"slot0": "3850c7bd", "token0": "0dfe1681", "token1": "d21220a7",
        "decimals": "313ce567", "balanceOf": "70a08231"}

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
    req = urllib.request.Request(url, headers={"User-Agent": "l1price/4.0"})
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


def _fetch_osmosis():
    """Osmosis venue. Primary: Numia (price + volume + 24h change). Fallback: SQS (price only)."""
    try:
        arr = _get(NUMIA_URL)
        t = arr[0] if isinstance(arr, list) else arr
        usd = _num(t.get("price"))
        if usd is None:
            raise ValueError("no price in Numia response")
        chg = _num(t.get("price_24h_change"))
        vol = _num(t.get("volume_24h"))
        liq = _num(t.get("liquidity"))
        return {
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
        return {"usd": float(raw), "change_24h_pct": None, "vol_24h_usd": None,
                "liquidity_usd": None, "source": "osmosis-sqs"}


def _rpc_raw(method, params, timeout=None):
    """One JSON-RPC call against the first Base endpoint that answers."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": method, "params": params}).encode()
    for url in BASE_RPCS:
        try:
            req = urllib.request.Request(url, data=body, headers={
                "Content-Type": "application/json", "User-Agent": "l1price/1.1"})
            with urllib.request.urlopen(req, timeout=timeout or TIMEOUT) as r:
                out = json.loads(r.read())
            if "error" in out:
                continue
            res = out.get("result")
            if res is not None and res != "0x":
                return res
        except Exception:
            continue
    return None


def _rpc(to, data):
    """One eth_call. Returns hex result or None."""
    return _rpc_raw("eth_call", [{"to": to, "data": data}, "latest"])


def _hexint(h, start=0, width=64):
    return int(h[2:][start:start + width], 16) if h and len(h[2:]) >= start + width else 0


def _fetch_base_onchain():
    """Base venue read straight from the Uniswap v3 pool. No indexer.

    Returns {"usd","liquidity_usd","vol_24h_usd","source","pool"} or None.

    vol_24h_usd comes from the Swap-event ring banked by the snapshot thread, NOT from a
    scan on the request path — a price endpoint must not depend on a log query. It is None
    while the ring is still warming, so an incomplete window is visibly absent rather than
    quietly understating the cross-venue total.
    """
    t0 = _rpc(WL1_POOL, "0x" + _SEL["token0"])
    t1 = _rpc(WL1_POOL, "0x" + _SEL["token1"])
    if not t0 or not t1:
        return None
    a0, a1 = "0x" + t0[-40:], "0x" + t1[-40:]
    # Do not assume the ordering — Uniswap sorts tokens by address, and a redeploy of
    # either side would flip it. Getting this backwards inverts the price silently.
    if a0.lower() == WL1_BASE:
        wl1, quote, wl1_is_t0 = a0, a1, True
    elif a1.lower() == WL1_BASE:
        wl1, quote, wl1_is_t0 = a1, a0, False
    else:
        return None
    d0 = _hexint(_rpc(a0, "0x" + _SEL["decimals"]) or "") or 18
    d1 = _hexint(_rpc(a1, "0x" + _SEL["decimals"]) or "") or 18

    s0 = _rpc(WL1_POOL, "0x" + _SEL["slot0"])
    if not s0:
        return None
    sq = _hexint(s0, 0, 64)
    if not sq:
        return None
    t1_per_t0 = (sq / 2 ** 96) ** 2 * (10 ** d0) / (10 ** d1)
    if not t1_per_t0:
        return None
    usd = t1_per_t0 if wl1_is_t0 else 1.0 / t1_per_t0     # quote side is a USD stable

    pad = WL1_POOL.lower().replace("0x", "").rjust(64, "0")
    bal_wl1 = _hexint(_rpc(wl1, "0x" + _SEL["balanceOf"] + pad) or "")
    bal_q = _hexint(_rpc(quote, "0x" + _SEL["balanceOf"] + pad) or "")
    dq = d1 if wl1_is_t0 else d0
    dw = d0 if wl1_is_t0 else d1
    liq = (bal_wl1 / 10 ** dw) * usd + (bal_q / 10 ** dq)

    vol, warming = _vol_24h_base()
    return {"usd": usd, "liquidity_usd": round(liq, 2),
            "vol_24h_usd": None if warming else vol,
            "vol_warming_up": warming,
            "source": "base-uniswap-v3-onchain", "pool": WL1_POOL,
            # the scanner needs to know which leg is the USD side; work it out here where
            # token order has already been resolved, rather than assuming it twice
            "_quote_is_token0": not wl1_is_t0, "_quote_decimals": dq}


def _sint256(word):
    """32-byte hex word -> signed int. Swap amounts are int256: negative means the pool
    paid that token out. Reading them unsigned turns a sell into ~1.16e77 of volume."""
    v = int(word, 16)
    return v - (1 << 256) if v >= (1 << 255) else v


def _scan_base_swaps(quote_is_token0, quote_decimals):
    """Bank Swap volume since the last scan. Called from the snapshot thread only."""
    head_hex = _rpc_raw("eth_blockNumber", [])
    if not head_hex:
        print("[basevol] no RPC answered eth_blockNumber", flush=True)
        return
    head = int(head_hex, 16) - BASE_VOL_LAG
    with _base_vol_lock:
        start = _base_vol["last_block"]
    if not start:
        # First run. Backfill 24h so the figure is usable immediately instead of after a
        # day of warm-up — ~43k blocks at 2s, which is 15 chunked requests, once.
        start = head - (86400 // 2) if BASE_VOL_BACKFILL else head
    if head <= start:
        return

    frm = start + 1
    while frm <= head:
        to = min(frm + BASE_VOL_CHUNK - 1, head)
        logs = _rpc_raw("eth_getLogs", [{"address": WL1_POOL,
                                         "fromBlock": hex(frm), "toBlock": hex(to),
                                         "topics": [BASE_SWAP_TOPIC]}], timeout=30)
        if logs is None:
            # Do NOT advance last_block past a window we failed to read, or that volume is
            # lost silently. Stop here and retry the same range on the next tick.
            print("[basevol] getLogs failed for blocks {}-{} — will retry".format(frm, to),
                  flush=True)
            return
        usd = 0.0
        for lg in logs:
            d = (lg.get("data") or "")[2:]
            if len(d) < 128:
                continue
            a0, a1 = _sint256(d[0:64]), _sint256(d[64:128])
            amt = a0 if quote_is_token0 else a1
            usd += abs(amt) / (10 ** quote_decimals)
        # Stamp the bucket with the BLOCK time, not the scan time. On a backfill every
        # bucket would otherwise be stamped "now", so a window covering the last 24h would
        # report zero coverage and sit in warming for a full day — defeating the backfill
        # entirely. One extra call per chunk.
        bts = None
        blk = _rpc_raw("eth_getBlockByNumber", [hex(to), False])
        if isinstance(blk, dict) and blk.get("timestamp"):
            try:
                bts = int(blk["timestamp"], 16)
            except Exception:
                bts = None
        # ts  = end of the bucket    (used to place it in the 24h window)
        # ts0 = start of the bucket  (used to measure how much history we cover)
        # Both are needed. Measuring coverage from `ts` alone makes the oldest bucket look
        # one chunk-length newer than it is — with 3000-block chunks that is ~100 minutes,
        # enough to keep a full 24h backfill permanently just under the threshold and stuck
        # in warming. Base blocks are ~2s.
        end_ts = bts or time.time()
        with _base_vol_lock:
            _base_vol["buckets"].append({"ts": end_ts,
                                         "ts0": end_ts - (to - frm) * 2,
                                         "from": frm, "to": to, "usd": round(usd, 2)})
            _base_vol["last_block"] = to
            cutoff = time.time() - RING_MAX_AGE
            _base_vol["buckets"] = [b for b in _base_vol["buckets"] if b["ts"] >= cutoff]
        print("[basevol] banked blocks {}-{}: ${:,.2f} from {} swaps".format(
            frm, to, usd, len(logs)), flush=True)
        frm = to + 1


def _vol_24h_base():
    """(usd, warming). warming=True while the banked window is still under 24h."""
    with _base_vol_lock:
        if not _base_vol["buckets"]:
            return None, True
        now = time.time()
        window = [b for b in _base_vol["buckets"] if b["ts"] >= now - 86400]
        oldest = min(b.get("ts0", b["ts"]) for b in _base_vol["buckets"])
        covered = now - oldest
        # 23h of coverage is close enough to call the figure a 24h volume; demanding the
        # full 86400 means one slow chunk keeps it warming for another half hour.
        return round(sum(b["usd"] for b in window), 2), covered < 82800


def _fetch_base_dexscreener():
    """Legacy DexScreener path, kept as a fallback only.

    It returned nothing for a month without anyone noticing, which is the real lesson
    here: a venue that vanishes must be visible in the response, not merely absent.
    """
    try:
        d = _get(DEXSCREENER_URL)
    except Exception:
        return None
    pairs = [p for p in (d.get("pairs") or []) if p.get("chainId") == "base"]
    if not pairs:
        return None
    liq_sum = vol_sum = 0.0
    pw_sum = pl_sum = 0.0   # price*liq accumulator, and liq of priced pools
    for p in pairs:
        liq = _num((p.get("liquidity") or {}).get("usd")) or 0.0
        vol = _num((p.get("volume") or {}).get("h24")) or 0.0
        # DexScreener priceUsd is the price of the pair's BASE token; only trust it
        # for wL1's price when wL1 IS the base token (else it's the other asset's price).
        wl1_is_base = ((p.get("baseToken") or {}).get("address") or "").lower() == WL1_BASE
        price = _num(p.get("priceUsd")) if wl1_is_base else None
        liq_sum += liq
        vol_sum += vol
        if price is not None and liq > 0:
            pw_sum += price * liq
            pl_sum += liq
    return {
        "usd": (pw_sum / pl_sum) if pl_sum > 0 else None,
        "liquidity_usd": round(liq_sum, 2),
        "vol_24h_usd": round(vol_sum, 2),
        "source": "base-dexscreener",
    }


def _fetch_base():
    """On-chain first, DexScreener only as a fallback."""
    try:
        b = _fetch_base_onchain()
    except Exception:
        b = None
    if b and b.get("usd"):
        return b
    try:
        return _fetch_base_dexscreener()
    except Exception:
        return None


def _blend(osmo, base):
    """Liquidity-weight each venue's own USD price by that venue's liquidity."""
    parts = []
    if osmo.get("usd") is not None and (osmo.get("liquidity_usd") or 0) > 0:
        parts.append((osmo["usd"], osmo["liquidity_usd"]))
    if base and base.get("usd") is not None and (base.get("liquidity_usd") or 0) > 0:
        parts.append((base["usd"], base["liquidity_usd"]))
    if parts:
        lsum = sum(l for _, l in parts)
        return sum(p * l for p, l in parts) / lsum if lsum > 0 else osmo.get("usd")
    # No liquidity figures on EITHER venue to weight by (rare: Osmosis on SQS-fallback
    # AND Base reporting no liquidity) -> prefer the historically-trusted Osmosis price.
    if osmo.get("usd") is not None:
        return osmo["usd"]
    return base.get("usd") if base else None


def _fetch():
    """Cross-venue: blend Osmosis + Base into one liquidity-weighted L1/USD price."""
    osmo = _fetch_osmosis()               # raises if even Osmosis is unreachable
    base = _fetch_base()                  # None if no Base pool / DexScreener down

    usd = _blend(osmo, base)
    if usd is None:
        raise ValueError("no usable price from any venue")

    def _sum(*xs):
        vals = [x for x in xs if x is not None]
        return round(sum(vals), 2) if vals else None

    total_liq = _sum(osmo.get("liquidity_usd"), base.get("liquidity_usd") if base else None)
    # vol_24h_complete is the honest flag: Base volume is banked by the snapshot thread
    # and is None until the ring covers 24h, so for the first day after a fresh deploy this
    # total is Osmosis-only. Better to say so than to let a partial figure pass as
    # cross-venue — which is the mistake the price itself was making.
    total_vol = _sum(osmo.get("vol_24h_usd"), base.get("vol_24h_usd") if base else None)
    vol_complete = bool(base) and base.get("vol_24h_usd") is not None

    if base:
        source = "blended:osmosis+base"
    else:
        source = osmo["source"]

    # A MISSING VENUE MUST BE LOUD. The Base leg was returning None for weeks and the API
    # kept serving a confident Osmosis-only number with no hint anything was wrong; it took
    # a community member noticing a price mismatch. Absence of a field is not a signal
    # anyone reads, so say it outright.
    degraded = base is None
    venue_note = None if base else ("Base venue unavailable - headline is Osmosis-only "
                                    "and reflects the shallower market")

    result = {
        "usd": usd,
        # 24h change stays Osmosis-derived (Numia) — a true cross-venue change needs
        # 24h-ago prices per venue, which neither source exposes. Flagged, not blended.
        "change_24h_pct": osmo.get("change_24h_pct"),
        "vol_24h_usd": total_vol,
        "vol_24h_complete": vol_complete,
        "liquidity_usd": total_liq,
        "source": source,
        "degraded": degraded,
        "note": venue_note,
        "venues": {
            "osmosis": {"usd": osmo.get("usd"), "liquidity_usd": osmo.get("liquidity_usd"),
                        "vol_24h_usd": osmo.get("vol_24h_usd"), "source": osmo.get("source")},
            # explicit key list: `base` carries _quote_* internals for the scanner that
            # have no business in a public response
            "base": ({"usd": base.get("usd"), "liquidity_usd": base.get("liquidity_usd"),
                      "vol_24h_usd": base.get("vol_24h_usd"),
                      "vol_warming_up": base.get("vol_warming_up"),
                      "source": base.get("source"), "pool": base.get("pool")}
                     if base else None),
        },
    }
    result["supply"], result["mcap_usd"] = _supply_mcap(usd)
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
        # _payload() WHITELISTS keys, so anything added to _fetch() must be added here too
        # or it is silently dropped — which is exactly how these two got lost on the first
        # pass. Health flags that never reach the response are worse than none, because
        # they read as reassurance in the source.
        "vol_24h_complete": d.get("vol_24h_complete"),
        "mcap_usd": d.get("mcap_usd"),
        "supply": d.get("supply"),
        "liquidity_usd": d["liquidity_usd"],
        "source": d["source"],
        "degraded": d.get("degraded", False),
        "note": d.get("note"),
        "venues": d.get("venues"),
        "updated": datetime.fromtimestamp(c["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_seconds": age,
        "stale": age > CACHE_TTL,
    }


# --- pools: snapshots + 24h volume -------------------------------------------

BASE_VOL_STATE = os.environ.get("BASE_VOL_STATE", POOLS_STATE + ".basevol")


def _load_ring():
    global _ring, _base_vol
    try:
        with open(POOLS_STATE) as f:
            _ring = json.load(f)
    except Exception:
        _ring = []
    # Separate file on purpose: the two rings have different shapes and lifetimes, and
    # sharing one would mean a schema change to either could corrupt the other.
    try:
        with open(BASE_VOL_STATE) as f:
            d = json.load(f)
        if isinstance(d, dict) and "buckets" in d:
            _base_vol.update(d)
    except Exception:
        pass


def _save_ring():
    try:
        tmp = POOLS_STATE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_ring, f)
        os.replace(tmp, POOLS_STATE)
    except Exception:
        pass


def _save_base_vol():
    try:
        tmp = BASE_VOL_STATE + ".tmp"
        with _base_vol_lock:
            data = {"buckets": list(_base_vol["buckets"]),
                    "last_block": _base_vol["last_block"]}
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, BASE_VOL_STATE)   # atomic: a half-written ring would lose a day
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
            out[str(pid)] = amt   # str keys: survive the JSON persist round-trip (JSON keys are strings)
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


def _snapshot_base_volume():
    """Ask the venue reader which leg is the USD side, then scan for new swaps."""
    b = _fetch_base_onchain()
    if not b:
        return
    _scan_base_swaps(b.get("_quote_is_token0", True), b.get("_quote_decimals", 6))
    _save_base_vol()


def _snapshot_loop():
    while True:
        for job in (_snapshot_pools, _snapshot_base_volume):
            try:
                job()
            except Exception as e:
                # REPORT IT. The first version swallowed this with a bare `pass`, which is
                # precisely the failure mode that let the Base venue disappear unnoticed
                # for weeks. A background job that fails silently is a bug generator.
                print("[snapshot] {} failed: {}: {}".format(
                    getattr(job, "__name__", job), type(e).__name__, e), flush=True)
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
        cur = latest["vols"].get(str(pid))   # str key — consistent in-memory and after JSON reload
        old = baseline["vols"].get(str(pid))
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
    server_version = "l1price/4.0"

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
                    "  GET /price      JSON price (cross-venue) + 24h volume + 24h change\n"
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
