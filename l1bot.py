#!/usr/bin/env python3
"""
l1bot - GenesisL1 (L1) price bot for Telegram.

Keeps a single auto-updating PINNED message with the live L1/USD price (edits it
every UPDATE_INTERVAL seconds, so no spam) and answers the /price command on
demand. Pulls from the l1price API. Zero dependencies - Python 3 stdlib only.
The bot only polls Telegram outbound, so it needs no inbound firewall ports.

Env (put secrets in an env file, never in code):
  TG_BOT_TOKEN   - bot token from @BotFather              (required)
  TG_CHAT        - target chat: @genesisL1price or numeric id (required)
  PRICE_URL      - l1price endpoint (default http://localhost:8585/price)
  UPDATE_INTERVAL- seconds between pinned-message edits  (default 60)
  STATE_FILE     - where to remember the pinned msg id   (default /data/l1bot_state.json)
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

TOKEN     = os.environ["TG_BOT_TOKEN"]
CHAT      = os.environ["TG_CHAT"]
PRICE_URL = os.environ.get("PRICE_URL", "http://localhost:8585/price")
INTERVAL  = int(os.environ.get("UPDATE_INTERVAL", "60"))
STATE_FILE = os.environ.get("STATE_FILE", "/data/l1bot_state.json")
API = "https://api.telegram.org/bot" + TOKEN

# l1price refreshes its cache synchronously, and the Base RPC leg alone can take 4-9 s, so
# the one request after each expiry can wait well past 10 s. Give it room, retry once, and
# fall back to the last good payload (clearly marked) instead of "unavailable".
PRICE_TIMEOUT = 25      # seconds per attempt
STALE_WARN    = 120     # warn when the API itself serves data older than this (seconds)
MAX_LAST_GOOD = 900     # reuse our own last good fetch for at most this long (seconds)
_last_good = {"d": None, "at": 0.0}


def tg(method, **params):
    data = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}).encode()
    req = urllib.request.Request(API + "/" + method, data=data)
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def _money(v):
    if v >= 1e6:
        return "${:.2f}M".format(v / 1e6)
    if v >= 1e3:
        return "${:,.0f}".format(v)
    return "${:.2f}".format(v)


def _source_label(src):
    """Friendly label for the l1price `source` field. Auto-adapts: normally the
    cross-venue blend; degrades to a single venue if the other is unreachable."""
    return {
        "blended:osmosis+base": "Osmosis + Base (liquidity-weighted)",
        "osmosis-numia": "Osmosis (USDC)",
        "osmosis-sqs": "Osmosis (USDC)",
        "base-dexscreener": "Base (USDC)",          # legacy, DexScreener no longer
                                                    # indexes wL1 — kept for old caches
        "base-uniswap-v3-onchain": "Base (USDC, on-chain)",
    }.get(src, src or "—")


def fetch_price():
    """GET the l1price payload, one retry. Remembers the last good result. The URL is
    public, so logging the failure is safe (the empty log hid this once)."""
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(PRICE_URL, timeout=PRICE_TIMEOUT) as r:
                d = json.load(r)
            float(d["usd"])
            _last_good.update(d=d, at=time.time())
            return d
        except Exception as e:
            print("price fetch attempt {} failed: {}: {}".format(attempt, type(e).__name__, e),
                  flush=True)
            if attempt == 2:
                raise
            time.sleep(1)


def _data_time(d):
    """'HH:MM UTC' of the price data itself (API `updated`), not of this message."""
    try:
        return datetime.strptime(d["updated"], "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M UTC")
    except Exception:
        return datetime.now(timezone.utc).strftime("%H:%M UTC")


def price_text():
    try:
        warn = None
        try:
            d = fetch_price()
        except Exception:
            if _last_good["d"] is None or time.time() - _last_good["at"] > MAX_LAST_GOOD:
                raise
            d = _last_good["d"]
            warn = "live refresh failed — this price may be out of date"
        age = d.get("age_seconds")
        if warn is None and age is not None and age > STALE_WARN:
            warn = "price is {} min old (the price service could not refresh)".format(int(age // 60))
        usd = float(d["usd"])
        chg = d.get("change_24h_pct")
        vol = d.get("vol_24h_usd")
        mcap = d.get("mcap_usd")
        now = _data_time(d)
        price_line = "Price: ${:.6f}".format(usd)
        if chg is not None:
            price_line += "  (24h {:+.1f}%)".format(chg)
        lines = ["\U0001F4B2 GenesisL1 (L1)", "", price_line]
        if vol is not None:
            # say so when the total is one venue only, rather than letting a partial
            # figure read as cross-venue
            lines.append("24h Volume: " + _money(vol)
                         + ("" if d.get("vol_24h_complete") else "  (Osmosis only)"))
        if mcap is not None:
            lines.append("Market Cap: " + _money(mcap))
        lines += ["Source: " + _source_label(d.get("source")), "Updated: " + now]
        # SURFACE THE DEGRADED FLAG. The API has carried it since 2026-08-10, but the bot
        # is what people actually read — and a health signal nobody sees is not a health
        # signal. The original bug (Base venue silently absent for weeks, headline set by
        # a venue holding ~5% of the depth) was invisible precisely here.
        if d.get("degraded"):
            note = d.get("note") or "one venue unavailable — price reflects a single market"
            lines += ["", "\u26A0\uFE0F " + note]
        if warn:
            lines += ["", "\u26A0\uFE0F " + warn]
        lines += ["", "via github.com/VanPez/l1price"]
        return "\n".join(lines)
    except Exception:
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        return "⚠️ L1 price temporarily unavailable ({})".format(now)


def load_state():
    try:
        return json.load(open(STATE_FILE))
    except Exception:
        return {}


def save_state(s):
    d = os.path.dirname(STATE_FILE)
    if d:
        os.makedirs(d, exist_ok=True)
    json.dump(s, open(STATE_FILE, "w"))


def main():
    state = load_state()
    pinned = state.get("pinned_message_id")
    offset = state.get("offset", 0)
    last_edit = 0.0

    while True:
        # 0) create + pin the message (retries every loop until the bot is in
        #    the group as admin, so deploy order doesn't matter)
        if not pinned:
            try:
                res = tg("sendMessage", chat_id=CHAT, text=price_text())
                if res.get("ok"):
                    pinned = res["result"]["message_id"]
                    try:
                        tg("pinChatMessage", chat_id=CHAT, message_id=pinned,
                           disable_notification="true")
                    except Exception:
                        pass
                    state["pinned_message_id"] = pinned
                    save_state(state)
            except Exception:
                pass

        # 1) refresh the pinned message on schedule
        if time.time() - last_edit >= INTERVAL and pinned:
            try:
                tg("editMessageText", chat_id=CHAT, message_id=pinned, text=price_text())
            except Exception:
                pass
            last_edit = time.time()

        # 2) answer /price commands (long-poll)
        try:
            upd = tg("getUpdates", offset=offset + 1, timeout=20)
            for u in upd.get("result", []):
                offset = u["update_id"]
                msg = u.get("message") or u.get("channel_post") or {}
                txt = msg.get("text", "") or ""
                if txt.split("@")[0] == "/price":
                    tg("sendMessage", chat_id=msg["chat"]["id"], text=price_text(),
                       reply_to_message_id=msg.get("message_id"))
            state["offset"] = offset
            save_state(state)
        except Exception:
            time.sleep(3)


if __name__ == "__main__":
    main()
