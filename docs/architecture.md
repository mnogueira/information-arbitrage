# Architecture

The platform is split into five runtime layers:

1. Collectors ingest institutional IB news, RSS feeds, and Polymarket probability shifts.
2. DuckDB acts as the durable handoff point between ingestion and trading.
3. The scoring engine converts each headline into structured, multi-instrument trade ideas.
4. The strategy engine filters signals, sizes positions, and expands cross-market or pair opportunities.
5. The execution layer routes paper orders to IB or MT5, with simulated fallbacks when a broker is unavailable.

## Data flow

`collector -> headlines table -> scorer -> strategy -> trade_decisions table -> execution -> executions table`

## Broker model

- `IBDataClient` connects to port `4001` for news and data access.
- `IBExecClient` connects to port `4002` for paper execution.
- `MT5Client` connects to the configured terminal path for B3 paper workflows.

## Safety defaults

- Orders remain paper-only.
- If a required broker dependency, credential, or contract month is missing, the system records a simulated execution receipt instead of forcing a broker submission.
- Duplicate headlines are ignored using a normalized headline hash.
