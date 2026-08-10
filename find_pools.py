#!/usr/bin/env python3
"""
find_pools.py — locate the wL1 Uniswap pools by asking the factory contracts directly.

    python3 find_pools.py
    python3 find_pools.py --debug     # show why an endpoint failed

If you already know the pool address, use read_pool.py instead — this file is for
finding it in the first place, e.g. after a pool migration.

WHY
---
l1price.py reads the Base venue from DexScreener, which returns NOTHING for the wL1
token (verified 2026-08-10 on both /latest/dex/tokens and /tokens/v1, Base and Ethereum).
The address is correct — DexScreener simply does not index this token, while Dextools
does. So the headline price silently degrades to Osmosis alone, which is what the
community spotted.

The fix is to stop asking an indexer and read the chain. Uniswap pool addresses are not
arbitrary: the factory records every pool it created, so `getPool(tokenA, tokenB, fee)`
returns the address or zero. This sweeps the plausible combinations and reports what
actually exists, so we do not have to guess which pool Dextools is showing.

No dependencies, no API key, no third-party indexer — same posture as the rest of
l1price, and the reason Mike likes this stack: "not rely on any 3rd party".

WHAT IT DOES
------------
For Ethereum and Base, for each quote token (WETH/USDC/USDT) and each Uniswap v3 fee
tier (0.01/0.05/0.3/1%), plus Uniswap v2 pairs:
  1. ask the factory whether a pool exists
  2. if it does, read slot0 (v3) or getReserves (v2) and both token decimals
  3. print the pool address, the pair, the price, and how much wL1 is actually in it

The last number is the one that matters. A pool can exist and hold nothing.
"""
from __future__ import annotations
import json, sys, urllib.request, urllib.error

# Run with --debug to see WHY an endpoint failed. The first version of this file caught
# every exception and printed only "no RPC answered", which told the user nothing and sent
# them looking at their own network when the real cause was a missing User-Agent: several
# public RPCs sit behind Cloudflare and reject urllib's default outright.
DEBUG = "--debug" in sys.argv
HEADERS = {"Content-Type": "application/json",
           "User-Agent": "l1price/1.0 (+github.com/VanPez/l1price)"}

L1 = "0xE6522A891702Cd2E8CC2A5182638c9DA1DD44B22".lower()

CHAINS = {
    "ethereum": {
        "rpc": ["https://eth.llamarpc.com", "https://ethereum-rpc.publicnode.com",
                "https://1rpc.io/eth", "https://eth.drpc.org"],
        "v3_factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "v2_factory": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "quotes": {
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        },
    },
    "base": {
        "rpc": ["https://mainnet.base.org", "https://base-rpc.publicnode.com",
                "https://base.llamarpc.com", "https://1rpc.io/base"],
        "v3_factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "v2_factory": "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6",
        "quotes": {
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        },
    },
}
FEES = [100, 500, 3000, 10000]          # 0.01% 0.05% 0.3% 1%

# function selectors (first 4 bytes of keccak256 of the signature)
SEL = {
    "getPool":     "1698ee82",   # getPool(address,address,uint24)
    "getPair":     "e6a43905",   # getPair(address,address)
    "slot0":       "3850c7bd",
    "getReserves": "0902f1ac",
    "token0":      "0dfe1681",
    "token1":      "d21220a7",
    "decimals":    "313ce567",
    "symbol":      "95d89b41",
    "balanceOf":   "70a08231",
}
ZERO = "0x" + "0" * 40


def _addr32(a: str) -> str:
    return a.lower().replace("0x", "").rjust(64, "0")


def _u32(n: int) -> str:
    return format(n, "064x")


def call(rpcs, to: str, data: str):
    """eth_call against the first RPC that answers. Returns hex string or None."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                       "params": [{"to": to, "data": data}, "latest"]}).encode()
    for url in rpcs:
        try:
            req = urllib.request.Request(url, data=body, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as r:
                out = json.loads(r.read())
            if "result" in out and out["result"] not in (None, "0x"):
                return out["result"]
            if "error" in out and DEBUG:
                print(f"    [{url}] rpc error: {out['error'].get('message')}")
        except urllib.error.HTTPError as e:
            if DEBUG:
                print(f"    [{url}] HTTP {e.code} — endpoint rejected the request")
        except Exception as e:
            if DEBUG:
                print(f"    [{url}] {type(e).__name__}: {e}")
    return None


def as_addr(hexstr):
    return "0x" + hexstr[-40:] if hexstr and len(hexstr) >= 42 else None


def as_int(hexstr, start=0, width=64):
    seg = hexstr[2:][start:start + width]
    return int(seg, 16) if seg else 0


def decimals(rpcs, token):
    r = call(rpcs, token, "0x" + SEL["decimals"])
    return as_int(r) if r else 18


def symbol(rpcs, token):
    r = call(rpcs, token, "0x" + SEL["symbol"])
    if not r:
        return "?"
    try:                                   # dynamic string: offset, length, bytes
        ln = int(r[2 + 64:2 + 128], 16)
        return bytes.fromhex(r[2 + 128:2 + 128 + ln * 2]).decode(errors="replace")
    except Exception:
        return bytes.fromhex(r[2:66]).decode(errors="replace").rstrip("\x00") or "?"


def v3_price(rpcs, pool, quote_sym):
    """token1-per-token0 from sqrtPriceX96, then oriented so it reads as L1 priced in quote."""
    s = call(rpcs, pool, "0x" + SEL["slot0"])
    if not s:
        return None
    sqrt_p = as_int(s, 0, 64)
    if sqrt_p == 0:
        return None
    t0 = as_addr(call(rpcs, pool, "0x" + SEL["token0"]) or "")
    t1 = as_addr(call(rpcs, pool, "0x" + SEL["token1"]) or "")
    if not t0 or not t1:
        return None
    d0, d1 = decimals(rpcs, t0), decimals(rpcs, t1)
    raw = (sqrt_p / (2 ** 96)) ** 2                 # token1 per token0, raw units
    price = raw * (10 ** d0) / (10 ** d1)           # decimal-adjusted
    l1_is_t0 = t0.lower() == L1
    l1_in_quote = price if l1_is_t0 else (1 / price if price else None)
    # how much L1 the pool actually holds
    bal = call(rpcs, L1, "0x" + SEL["balanceOf"] + _addr32(pool))
    held = as_int(bal) / (10 ** 18) if bal else None
    return {"price": l1_in_quote, "l1_is_token0": l1_is_t0, "l1_held": held,
            "token0": t0, "token1": t1, "quote": quote_sym}


def v2_price(rpcs, pair, quote_sym):
    r = call(rpcs, pair, "0x" + SEL["getReserves"])
    if not r:
        return None
    r0, r1 = as_int(r, 0, 64), as_int(r, 64, 64)
    t0 = as_addr(call(rpcs, pair, "0x" + SEL["token0"]) or "")
    t1 = as_addr(call(rpcs, pair, "0x" + SEL["token1"]) or "")
    if not t0 or not t1 or r0 == 0 or r1 == 0:
        return None
    d0, d1 = decimals(rpcs, t0), decimals(rpcs, t1)
    a0, a1 = r0 / 10 ** d0, r1 / 10 ** d1
    l1_is_t0 = t0.lower() == L1
    price = (a1 / a0) if l1_is_t0 else (a0 / a1)
    held = a0 if l1_is_t0 else a1
    return {"price": price, "l1_is_token0": l1_is_t0, "l1_held": held,
            "token0": t0, "token1": t1, "quote": quote_sym}


def main():
    print(f"wL1 = {L1}\n")
    found = []
    for chain, c in CHAINS.items():
        rpcs = c["rpc"]
        print(f"=== {chain} ===")
        alive = call(rpcs, c["v3_factory"], "0x" + SEL["getPool"]
                     + _addr32(L1) + _addr32(list(c['quotes'].values())[0]) + _u32(3000))
        if alive is None:
            print("  ! no RPC answered. Re-run with --debug to see why.\n")
            continue
        for qsym, q in c["quotes"].items():
            for fee in FEES:
                data = "0x" + SEL["getPool"] + _addr32(L1) + _addr32(q) + _u32(fee)
                pool = as_addr(call(rpcs, c["v3_factory"], data) or "")
                if pool and pool != ZERO:
                    info = v3_price(rpcs, pool, qsym)
                    held = f"{info['l1_held']:,.0f}" if info and info.get('l1_held') is not None else "?"
                    px = f"{info['price']:.10g}" if info and info.get('price') else "?"
                    print(f"  v3 {fee/10000:>5.2f}%  L1/{qsym:<4} {pool}  "
                          f"price {px} {qsym}  ·  holds {held} L1")
                    found.append((chain, "v3", fee, qsym, pool, info))
            data = "0x" + SEL["getPair"] + _addr32(L1) + _addr32(q)
            pair = as_addr(call(rpcs, c["v2_factory"], data) or "")
            if pair and pair != ZERO:
                info = v2_price(rpcs, pair, qsym)
                held = f"{info['l1_held']:,.0f}" if info and info.get('l1_held') is not None else "?"
                px = f"{info['price']:.10g}" if info and info.get('price') else "?"
                print(f"  v2         L1/{qsym:<4} {pair}  price {px} {qsym}  ·  holds {held} L1")
                found.append((chain, "v2", 0, qsym, pair, info))
        print()

    if not found:
        print("No Uniswap pool found for this token on either chain.")
        print("If Dextools shows one, it is on another chain or another DEX")
        print("(Aerodrome, PancakeSwap, SushiSwap...). Send me the Dextools URL —")
        print("the chain and pool address are both in it.")
    else:
        print(f"{len(found)} pool(s) found. The one to use is whichever holds the most L1.")
        print("Note the price above is in the QUOTE token, not USD — for a WETH pair")
        print("it still needs multiplying by ETH/USD.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
