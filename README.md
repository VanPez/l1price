# l1price — a public price API for GenesisL1 (L1)

A tiny, zero-dependency HTTP service that serves the live **L1/USD price, 24h
volume, 24h change, and market cap**, so the GenesisL1 community can stop
reinventing price-fetching: point any bot, dashboard, or widget at one URL.

**Live instance:** `http://46.224.42.12:8585/price` · `http://46.224.42.12:8585/price.txt`

## Where the data comes from

L1 trades on **two venues**, and the headline price is a **liquidity-weighted blend** of
both, so the deeper market dominates. All sources are public and need no API key.

**Osmosis** — IBC denom
`ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4`, channel-253.
Price, 24h volume and 24h change come from the **Numia** public indexer
(`public-osmosis-api.numia.xyz`), which powers app.osmosis.zone, falling back to the
Osmosis **SQS router** (`sqs.osmosis.zone`) for price alone.

**Base** — wrapped L1 (`wL1`, `0xE6522A891702Cd2E8CC2A5182638c9DA1DD44B22`) against USDC on
Uniswap v3, read **directly from the pool contract** over a public Base RPC: `slot0()` for
the spot price and `balanceOf()` on both sides for the depth. No indexer sits in the path.

> **Why on-chain and not an indexer.** This originally used DexScreener, which stopped
> returning anything for wL1 — the token simply is not in their index, though Dextools
> carries it. The Base leg silently returned nothing, the blend collapsed to Osmosis alone,
> and the published price tracked a venue holding ~5% of the liquidity for weeks before a
> community member noticed the mismatch. Reading the pool cannot be de-listed or
> rate-limited, and matches the GenesisL1 posture of not depending on third parties.
>
> Both are overridable without a redeploy: `WL1_POOL` and `BASE_RPCS`.

**Market cap** — total L1 supply from the GenesisL1 chain REST (`api.genesisl1.org`) ×
blended price. L1 is fully circulating, so total supply is the circulating supply and the
mcap is honest.

### Health fields, and why they exist

The failure above was invisible because a missing venue looked identical to a working one.
So the response says so outright:

| field | meaning |
|---|---|
| `degraded` | `true` when a venue is unavailable and the headline rests on one market |
| `note` | plain-English explanation when degraded |
| `vol_24h_complete` | `false` when the volume total is not cross-venue |
| `venues.*` | each venue's own price, depth and source — always inspect this before quoting a number |

`vol_24h_complete` is normally **false**: the on-chain Base read gives price and depth but
not 24h volume, which would need a log scan over ~43k blocks — out of scope for a 30-second
endpoint. The total is Osmosis-only and says so rather than passing as cross-venue.

## Endpoints

| Route        | Returns |
|--------------|---------|
| `GET /price`     | full JSON (below) |
| `GET /price.txt` | one-line ticker, e.g. `L1 $0.039602  24h +0.1%  vol $5,004  mcap $1.83M` |
| `GET /health`    | `ok` |
| `GET /`          | short help |

```json
{
  "symbol": "L1",
  "name": "GenesisL1",
  "usd": 0.10992173,
  "change_24h_pct": -5.85,
  "vol_24h_usd": 1223.77,
  "vol_24h_complete": false,
  "mcap_usd": 5140451.81,
  "supply": 46764657.1,
  "liquidity_usd": 210211.0,
  "source": "blended:osmosis+base",
  "degraded": false,
  "note": null,
  "venues": {
    "osmosis": { "usd": 0.10403084, "liquidity_usd": 11197.26,
                 "vol_24h_usd": 1223.77, "source": "osmosis-numia" },
    "base":    { "usd": 0.11025317, "liquidity_usd": 199013.74, "vol_24h_usd": null,
                 "source": "base-uniswap-v3-onchain",
                 "pool": "0xde9ba4084bf0733a9d69e42e71e5fd140179df71" }
  },
  "updated": "2026-08-10T14:27:26Z",
  "age_seconds": 2.1,
  "stale": false
}
```

- Responses are cached ~30s (configurable) so you can hammer it safely.
- `Access-Control-Allow-Origin: *` is set, so browser/JS/Streamlit apps can call it directly.
- If a venue is briefly unreachable it serves the last good price and never crashes —
  but sets `degraded` so you can tell.

## Run it

Requires only Python 3 (no `pip install` needed).

```bash
python3 l1price.py            # listens on 0.0.0.0:8585
PORT=9000 python3 l1price.py  # custom port
curl localhost:8585/price
```

### As a systemd service

```bash
cp l1price.py ~/l1price.py
sudo cp l1price.service /etc/systemd/system/l1price.service
# edit User= and the path in the unit if your user isn't 'ivan'
sudo systemctl daemon-reload
sudo systemctl enable --now l1price
```

### As a Docker container

```bash
docker run -d --name l1price --restart unless-stopped \
  --log-driver json-file --log-opt max-size=10m --log-opt max-file=3 \
  -p 8585:8585 -v "$PWD/l1price.py:/app/l1price.py:ro" \
  python:3-slim python3 /app/l1price.py
```

## Consume it

```bash
curl -s http://YOUR-HOST:8585/price | jq '.usd, .vol_24h_usd, .mcap_usd'   # shell / bots
curl -s http://YOUR-HOST:8585/price.txt   # -> "L1 $0.109922  24h -5.9%  vol $1,224  mcap $5.14M"
```

```js
// browser / dashboard
const { usd } = await (await fetch("http://YOUR-HOST:8585/price")).json();
```

## Telegram bot (`l1bot.py`)

A companion bot that posts the live price into a Telegram group, keeps it
**pinned and auto-updating every minute** (edits one message, no spam), and
answers **/price**. Zero dependencies, outbound-only (no inbound ports).

```bash
TG_BOT_TOKEN=<token from @BotFather> \
TG_CHAT=@your_group \
PRICE_URL=http://YOUR-HOST:8585/price \
python3 l1bot.py
```

Add the bot to the group as an admin with **"Pin messages"**. It self-heals —
it retries until it's added, so start order doesn't matter. Live instance runs
in the `@genesisL1price` group.

## Roadmap

Same pattern, more endpoints off a full node: `/supply`, `/validators`,
`/apr` (inflation × bonded ratio), `/block`, `/upgrade` (halt height vs current) —
turning this into the public GenesisL1 stats API the ecosystem is missing. PRs welcome.

## License

MIT — free to use, fork, and self-host. Built for the GenesisL1 community.
