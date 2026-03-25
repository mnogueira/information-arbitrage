# Setup

## Local bootstrap

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
```

## Important environment variables

- `OPENAI_API_KEY`: required for model scoring. On Windows the app also checks the user environment registry.
- `OPENAI_MODEL`: defaults to `gpt-5-mini`.
- `IB_CONTRACT_MONTHS`: JSON mapping for futures, for example `{"CL":"202606","GC":"202608"}`.
- `MT5_PASSWORD`: required for live MT5 API initialization.
- `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`: optional trade alerts.

## Runtime commands

```powershell
python scripts\run_collector.py --duration 60
python scripts\run_trader.py --duration 60 --simulate-only
python scripts\run_all.py --duration 60 --simulate-only
python scripts\backtest_signals.py --limit 250
```

## GitHub

The intended GitHub command is:

```powershell
gh repo create information-arbitrage --public --source=. --remote=origin
```

If network or authentication is unavailable, initialize locally first and rerun the command later.
