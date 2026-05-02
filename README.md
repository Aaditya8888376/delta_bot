# delta_bot

Algorithmic trading bot scaffold for Delta Exchange using ccxt, with a basic trend-following strategy and backtesting pipeline.

> **Risk warning**: Trading is risky. This project is for educational purposes only and does not guarantee returns. Validate thoroughly in paper trading before any live deployment.

## Strategy implemented
- **SMA trend-following with ATR volatility filter**
- Enters long when fast SMA > slow SMA and volatility is above a minimum threshold
- Enters short when fast SMA < slow SMA (optional)
- Uses ATR-based stop distance for position sizing and risk control

## Step-by-step setup

### 1) Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Create configuration
```bash
python -m delta_bot.cli init-config --config config.json
```

### 3) Set API keys
Use environment variables referenced in `config.json`:
```bash
export DELTA_API_KEY="your_key"
export DELTA_API_SECRET="your_secret"
```

### 4) Confirm and set requirements in `config.json`
Update these fields before running anything:
- `market.type`: `spot` or `perp`
- `market.symbol`: instrument like `BTC/USDT`
- `market.timeframe`: e.g. `1h`
- `risk.capital_usd`: starting capital for backtests/paper
- `risk.max_drawdown_pct`, `risk.max_daily_loss_pct`: risk limits
- `strategy.*`: windows and ATR filter settings

### 5) Fetch historical data
```bash
python -m delta_bot.cli fetch-data --config config.json
```
Data is saved under `data/`.

### 6) Run backtest
```bash
python -m delta_bot.cli backtest --config config.json
```
Results are written to `results/`.

### 7) Paper trade (recommended first)
```bash
python -m delta_bot.cli trade --config config.json
```
Logs and simulated trades are written to `logs/` and `state/`.

### 8) Live trading (only after validation)
```bash
python -m delta_bot.cli trade --config config.json --live
```

### 9) Health check
```bash
python -m delta_bot.cli health --config config.json
```

## Deployment runbook (VPS)
1. Provision a Linux VPS and install Python 3.10+.
2. Clone this repo and set up a venv.
3. Store API keys as environment variables (avoid committing secrets).
4. Run `fetch-data` and `backtest` to validate parameters.
5. Start paper trading for several days.
6. Move to live mode with conservative risk limits.
7. Use a process manager (systemd, supervisor, or tmux) for auto-restart.
8. Monitor `logs/bot.log` and `logs/trades.jsonl` daily.

## Notes
- `--live` sends real orders. Default is paper trading.
- Funding history may be unavailable depending on exchange support in ccxt.
- Adjust fees/slippage in `config.json` to match your account tier.
