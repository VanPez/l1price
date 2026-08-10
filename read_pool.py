#!/usr/bin/env python3
"""
read_pool.py — read an L1 DEX pool straight off the chain. No indexer, no API key.

    python3 read_pool.py                        # the known wL1/? pool on Base
    python3 read_pool.py 0xPOOL                  # any pool, Base
    python3 read_pool.py 0xPOOL ethereum         # any pool, another chain
    python3 read_pool.py --debug                 # show why an RPC failed

WHY
---
l1price.py reads the Base venue from DexScreener, which indexes NOTHING for wL1
(verified 2026-08-10: /latest/dex/tokens and /tokens/v1, Base and Ethereum, all empty).
The token address was never wrong — DexScreener just does not carry this token, while
Dextools does. So `venues.base` came back null and the headline price silently fell back
to Osmosis alone. That is what the community spotted.

Reading the pool contract removes the indexer from the loop entirely, which is both more
reliable and the posture Mike wants: "not rely on any 3rd party".

WHAT IT READS
-------------
Uniswap v3   slot0().sqrtPriceX96  -> exact spot price
Uniswap v2 / Aerodrome  getReserves() -> reserve ratio
plus token0/token1, their decimals and symbols, and the pool's actual wL1 balance.

The balance matters: a pool can exist, quote a price, and hold nothing. A price from an
empty pool is noise, and blending it into the headline would be worse than omitting it.

NOTE ON THE EARLIER VERSION: find_pools.py swallowed every exception and printed only
"no RPC answered", which was useless. Errors are reported per endpoint here, and requests
carry a User-Agent because several public RPCs reject urllib's default outright.
"""
from __future__ import annotations
import json, sys, urllib.request, urllib.error

L1 = "0xE6522A891702Cd2E8CC2A5182638c9DA1DD44B22".lower()
POOL_DEFAULT = "0xde9ba4084bf0733a9d69e42e71e5fd140179df71"   # from Dextools, via Ivan

RPCS = {
    "base": ["https://mainnet.base.org",
             "https://base.llamarpc.com",
             "https://base-rpc.publicnode.com",
             "https://1rpc.io/base",
             "https://base.drpc.org"],
    "ethereum": ["https://eth.llamarpc.com",
                 "https://ethereum-rpc.publicnode.com",
                 "https://1rpc.io/eth",
                 "https://eth.drpc.org"],
}
SEL = {"slot0": "3850c7bd", "getReserves": "0902f1ac", "token0": "0dfe1681",
       "token1": "d21220a7", "decimals": "313ce567", "symbol": "95d89b41",
       "balanceOf": "70a08231", "liquidity": "1a686502"}
DEBUG = "--debug" in sys.argv
UA = {"Content-Type": "application/json",
      "User-Agent": "l1price/1.0 (+github.com/VanPez/l1price)"}


def call(rpcs, to, data):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                       "params": [{"to": to, "data": data}, "latest"]}).encode()
    for url in rpcs:
        try:
            req = urllib.request.Request(url, data=body, headers=UA)
            with urllib.request.urlopen(req, timeout=20) as r:
                out = json.loads(r.read())
            if "error" in out:
                if DEBUG:
                    print(f"    [{url}] rpc error: {out['error'].get('message')}")
                continue
            res = out.get("result")
            if res and res != "0x":
                return res
            if DEBUG:
                print(f"    [{url}] empty result (no such function on this contract?)")
        except urllib.error.HTTPError as e:
            if DEBUG:
                print(f"    [{url}] HTTP {e.code}")
        except Exception as e:
            if DEBUG:
                print(f"    [{url}] {type(e).__name__}: {e}")
    return None


def probe(rpcs):
    """Is anything answering at all? Separates 'chain unreachable' from 'wrong contract'."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": "eth_blockNumber", "params": []}).encode()
    for url in rpcs:
        try:
            req = urllib.request.Request(url, data=body, headers=UA)
            with urllib.request.urlopen(req, timeout=15) as r:
                out = json.loads(r.read())
            if "result" in out:
                return url, int(out["result"], 16)
        except Exception as e:
            if DEBUG:
                print(f"    [{url}] {type(e).__name__}: {e}")
    return None, None


def as_addr(h):  return "0x" + h[-40:] if h and len(h) >= 42 else None
def as_int(h, s=0, w=64): return int(h[2:][s:s + w], 16) if h and len(h[2:]) >= s + w else 0


def sym(rpcs, t):
    r = call(rpcs, t, "0x" + SEL["symbol"])
    if not r:
        return "?"
    try:
        ln = int(r[2 + 64:2 + 128], 16)
        return bytes.fromhex(r[2 + 128:2 + 128 + ln * 2]).decode(errors="replace")
    except Exception:
        return bytes.fromhex(r[2:66]).decode(errors="replace").rstrip("\x00") or "?"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    pool = (args[0] if args else POOL_DEFAULT).lower()
    chain = args[1].lower() if len(args) > 1 else "base"
    rpcs = RPCS.get(chain)
    if not rpcs:
        sys.exit(f"unknown chain {chain}; known: {', '.join(RPCS)}")

    print(f"pool  {pool}\nchain {chain}")
    url, blk = probe(rpcs)
    if not url:
        print("\nNo RPC answered. Re-run with --debug to see why (HTTP 403 usually means\n"
              "the endpoint rejects unknown clients; a timeout usually means DNS/firewall).")
        return 1
    print(f"rpc   {url}  (block {blk:,})\n")

    t0 = as_addr(call(rpcs, pool, "0x" + SEL["token0"]) or "")
    t1 = as_addr(call(rpcs, pool, "0x" + SEL["token1"]) or "")
    if not t0 or not t1:
        print("This address does not answer token0()/token1() — not a Uniswap-style pool,\n"
              "or wrong chain. Check the Dextools URL: it names the chain explicitly.")
        return 1
    d0 = as_int(call(rpcs, t0, "0x" + SEL["decimals"]) or "0x0") or 18
    d1 = as_int(call(rpcs, t1, "0x" + SEL["decimals"]) or "0x0") or 18
    s0, s1 = sym(rpcs, t0), sym(rpcs, t1)
    l1_is_t0 = t0.lower() == L1
    if not l1_is_t0 and t1.lower() != L1:
        print(f"!! neither token is wL1 ({L1})")
    print(f"token0 {s0:<8} {t0}  decimals {d0}")
    print(f"token1 {s1:<8} {t1}  decimals {d1}")
    print(f"wL1 is token{0 if l1_is_t0 else 1}\n")

    price = None
    s = call(rpcs, pool, "0x" + SEL["slot0"])
    if s:
        sq = as_int(s, 0, 64)
        if sq:
            raw = (sq / 2 ** 96) ** 2 * (10 ** d0) / (10 ** d1)   # token1 per token0
            price = raw if l1_is_t0 else (1 / raw if raw else None)
            print(f"kind   Uniswap v3 (slot0)")
    if price is None:
        r = call(rpcs, pool, "0x" + SEL["getReserves"])
        if r:
            r0, r1 = as_int(r, 0, 64) / 10 ** d0, as_int(r, 64, 64) / 10 ** d1
            if r0 and r1:
                price = (r1 / r0) if l1_is_t0 else (r0 / r1)
                print(f"kind   Uniswap v2 / Aerodrome (getReserves)")
                print(f"reserves {r0:,.4f} {s0}  ·  {r1:,.4f} {s1}")
    if price is None:
        print("Could not read a price: neither slot0() nor getReserves() answered.")
        return 1

    quote = s1 if l1_is_t0 else s0
    print(f"\nPRICE  1 wL1 = {price:.12g} {quote}")

    bal = call(rpcs, L1, "0x" + SEL["balanceOf"] + pool.replace("0x", "").rjust(64, "0"))
    if bal:
        print(f"POOL HOLDS {as_int(bal) / 10**18:,.2f} wL1")
        print("\n(An empty pool still quotes a price. Check this number before trusting it —")
        print(" blending a dead pool into the headline would be worse than omitting it.)")
    if quote.upper() not in ("USDC", "USDT", "DAI"):
        print(f"\nNOTE: priced in {quote}, not USD. Still needs {quote}/USD to be a dollar price.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
