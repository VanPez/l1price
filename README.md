# l1price — a public price API for GenesisL1 (L1)

A tiny, zero-dependency HTTP service that serves the live **L1/USD price, 24h
volume, 24h change, and market cap**, so the GenesisL1 community can stop
reinventing price-fetching: point any bot, dashboard, or widget at one URL.

**Live instance:** `http://46.224.42.12:8585/price` · `http://46.224.42.12:8585/price.txt`

## Where the data comes from

L1 trades on **Osmosis** (IBC denom
`ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4`, channel-253).
All sources are public and need no API key:

- **Price + 24h volume + 24h change** — the Osmosis indexer (**Numia** public API,
  `public-osmosis-api.numia.xyz`), which powers app.osmosis.zone. Falls back to the
  Osmosis **SQS router** (`sqs.osmosis.zone`) for price if Numia is unreachable.
- **Market cap** — total L1 supply from the GenesisL1 chain REST
  (`api.genesisl1.org`) × price. Since L1 is fully circulating (nothing held back),
  total supply is effectively the circulating supply, so the mcap is honest.

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
  "usd": 0.03960195,
  "change_24h_pct": 0.14,
  "vol_24h_usd": 5004.26,
  "mcap_usd": 1827928.07,
  "supply": 46157530.13,
  "liquidity_usd": 7128.36,
  "source": "osmosis-numia",
  "updated": "2026-06-24T13:04:21Z",
  "age_seconds": 4.0,
  "stale": false
}
```

- Responses are cached ~30s (configurable) so you can hammer it safely.
- `Access-Control-Allow-Origin: *` is set, so browser/JS/Streamlit apps can call it directly.
- If Osmosis is briefly unreachable, it serves the last good price and never crashes.

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
curl -s http://YOUR-HOST:8585/price.txt   # -> "L1 $0.039602  24h +0.1%  vol $5,004  mcap $1.83M"
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
