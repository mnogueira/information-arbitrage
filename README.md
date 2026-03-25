# information-arbitrage

Central intelligence for latency-sensitive news trading across Interactive Brokers and MetaTrader 5.

## What it does

- Ingests institutional IB news, RSS headlines, and Polymarket probability shifts
- Scores each event for multi-instrument trade signals with OpenAI or a deterministic fallback
- Routes paper orders to IB or MT5 based on an instrument registry
- Stores headlines, scores, trades, and executions in DuckDB for replay and analysis

## Quick start

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
python scripts\run_all.py --duration 60
```

## Runtime notes

- `OPENAI_API_KEY` is read from process environment and, on Windows, from the user environment registry.
- IB is configured for a dual-port setup: data on `4001`, paper execution on `4002`.
- Futures execution requires contract months to be configured before live paper routing.
- If a broker API is unavailable, the system falls back to simulated paper receipts instead of placing orders blindly.
