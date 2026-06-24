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


def price_text():
    try:
        with urllib.request.urlopen(PRICE_URL, timeout=10) as r:
            d = json.load(r)
        usd = float(d["usd"])
        chg = d.get("change_24h_pct")
        vol = d.get("vol_24h_usd")
        mcap = d.get("mcap_usd")
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        price_line = "Price: ${:.6f}".format(usd)
        if chg is not None:
            price_line += "  (24h {:+.1f}%)".format(chg)
        lines = ["\U0001F4B2 GenesisL1 (L1)", "", price_line]
        if vol is not None:
            lines.append("24h Volume: " + _money(vol))
        if mcap is not None:
            lines.append("Market Cap: " + _money(mcap))
        lines += ["Source: Osmosis (USDC)", "Updated: " + now, "",
                  "via github.com/VanPez/l1price"]
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
