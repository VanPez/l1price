# l1price — a public price API for GenesisL1 (L1)

A tiny, zero-dependency HTTP service that serves the live **L1/USD** price, so the
GenesisL1 community can stop reinventing price-fetching: point any bot, dashboard,
or widget at one URL.

**Live instance:** `http://46.224.42.12:8585/price` · `http://46.224.42.12:8585/price.txt`

## Where the price comes from

L1 trades on **Osmosis** (IBC denom
`ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4`, channel-253).
Instead of manually weighting pools and pulling CoinGecko quotes, this uses
**Osmosis's own SQS router** (`sqs.osmosis.zone/tokens/prices`), which already routes
across L1's liquidity and returns a **USDC-denominated** price in one call. That value
is effectively L1/USD, and it tracks LiveCoinWatch's L1 page within ~0.5%.

## Endpoints

| Route        | Returns |
|--------------|---------|
| `GET /price`     | full JSON (below) |
| `GET /price.txt` | one-line ticker, e.g. `L1 $0.040113` |
| `GET /health`    | `ok` |
| `GET /`          | short help |

```json
{
  "symbol": "L1",
  "name": "GenesisL1",
  "usd": 0.04011304,
  "source": "osmosis-sqs",
  "quote": "USDC",
  "base_denom": "ibc/F16FDC11A7662B86BC0B9CE61871CBACF7C20606F95E86260FD38915184B75B4",
  "updated": "2026-06-24T08:43:39Z",
  "age_seconds": 4.2,
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
curl -s http://YOUR-HOST:8585/price | jq .usd     # shell / bots
curl -s http://YOUR-HOST:8585/price.txt           # -> "L1 $0.040113"
```

```js
// browser / dashboard
const { usd } = await (await fetch("http://YOUR-HOST:8585/price")).json();
```

## Roadmap

Same pattern, more endpoints off a full node: `/supply`, `/validators`,
`/apr` (inflation × bonded ratio), `/block`, `/upgrade` (halt height vs current) —
turning this into the public GenesisL1 stats API the ecosystem is missing. PRs welcome.

## License

MIT — free to use, fork, and self-host. Built for the GenesisL1 community.
