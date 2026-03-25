from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from information_arbitrage.models import ExecutionReport, HeadlineEvent, ScoredHeadline, TradeDecision


class MarketStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        self._conn = duckdb.connect(self.db_path)
        self.initialize()

    def initialize(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS headlines (
                    id VARCHAR PRIMARY KEY,
                    dedupe_key VARCHAR UNIQUE,
                    source VARCHAR NOT NULL,
                    source_kind VARCHAR NOT NULL,
                    provider VARCHAR,
                    text VARCHAR NOT NULL,
                    url VARCHAR,
                    article_id VARCHAR,
                    body VARCHAR,
                    symbols_json VARCHAR,
                    metadata_json VARCHAR,
                    published_at TIMESTAMP NOT NULL,
                    status VARCHAR NOT NULL DEFAULT 'pending',
                    claimed_by VARCHAR,
                    claimed_at TIMESTAMP,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scores (
                    headline_id VARCHAR PRIMARY KEY,
                    model VARCHAR NOT NULL,
                    scored_at TIMESTAMP NOT NULL,
                    escalation_score DOUBLE NOT NULL,
                    category VARCHAR NOT NULL,
                    urgency VARCHAR NOT NULL,
                    time_horizon VARCHAR NOT NULL,
                    instruments_json VARCHAR NOT NULL,
                    metadata_json VARCHAR NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_decisions (
                    id VARCHAR PRIMARY KEY,
                    headline_id VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    broker VARCHAR NOT NULL,
                    direction VARCHAR NOT NULL,
                    quantity DOUBLE NOT NULL,
                    confidence DOUBLE NOT NULL,
                    urgency VARCHAR NOT NULL,
                    time_horizon VARCHAR NOT NULL,
                    strategy_name VARCHAR NOT NULL,
                    reason VARCHAR NOT NULL,
                    metadata_json VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS executions (
                    decision_id VARCHAR PRIMARY KEY,
                    symbol VARCHAR NOT NULL,
                    broker VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    broker_order_id VARCHAR,
                    filled_quantity DOUBLE NOT NULL,
                    average_fill_price DOUBLE,
                    executed_at TIMESTAMP NOT NULL,
                    metadata_json VARCHAR NOT NULL
                )
                """
            )

    def insert_headline(self, headline: HeadlineEvent) -> bool:
        payload = headline.model_dump(mode="json")
        with self._lock:
            existing = self._conn.execute(
                "SELECT 1 FROM headlines WHERE dedupe_key = ? LIMIT 1",
                [headline.dedupe_key],
            ).fetchone()
            if existing:
                return False
            self._conn.execute(
                """
                INSERT INTO headlines (
                    id, dedupe_key, source, source_kind, provider, text, url, article_id,
                    body, symbols_json, metadata_json, published_at, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                [
                    headline.id,
                    headline.dedupe_key,
                    headline.source,
                    headline.source_kind,
                    headline.provider,
                    headline.text,
                    headline.url,
                    headline.article_id,
                    headline.body,
                    json.dumps(payload["symbols"]),
                    json.dumps(payload["metadata"]),
                    headline.published_at,
                ],
            )
        return True

    def claim_pending_headlines(self, consumer_id: str, limit: int) -> list[HeadlineEvent]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id
                FROM headlines
                WHERE status = 'pending'
                ORDER BY published_at ASC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            ids = [row[0] for row in rows]
            if not ids:
                return []

            claimed_at = datetime.now(UTC)
            placeholders = ", ".join(["?"] * len(ids))
            self._conn.execute(
                f"""
                UPDATE headlines
                SET status = 'processing', claimed_by = ?, claimed_at = ?
                WHERE id IN ({placeholders})
                """,
                [consumer_id, claimed_at, *ids],
            )

            records = self._conn.execute(
                f"""
                SELECT id, source, source_kind, text, published_at, url, provider,
                       article_id, body, symbols_json, metadata_json, dedupe_key
                FROM headlines
                WHERE id IN ({placeholders})
                ORDER BY published_at ASC
                """,
                ids,
            ).fetchall()

        return [self._row_to_headline(row) for row in records]

    def mark_headline_failed(self, headline_id: str, error_message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE headlines
                SET status = 'failed',
                    processed_at = ?,
                    metadata_json = ?
                WHERE id = ?
                """,
                [datetime.now(UTC), json.dumps({"error": error_message}), headline_id],
            )

    def mark_headline_processed(self, headline_id: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE headlines
                SET status = 'processed', processed_at = ?
                WHERE id = ?
                """,
                [datetime.now(UTC), headline_id],
            )

    def record_score(self, score: ScoredHeadline) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO scores (
                    headline_id, model, scored_at, escalation_score, category,
                    urgency, time_horizon, instruments_json, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    score.headline_id,
                    score.model,
                    score.scored_at,
                    score.escalation_score,
                    score.category.value,
                    score.urgency.value,
                    score.time_horizon.value,
                    json.dumps([item.model_dump(mode="json") for item in score.instruments]),
                    json.dumps(score.metadata),
                ],
            )

    def record_trade_decisions(self, decisions: list[TradeDecision]) -> None:
        if not decisions:
            return
        with self._lock:
            for decision in decisions:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO trade_decisions (
                        id, headline_id, symbol, broker, direction, quantity, confidence,
                    urgency, time_horizon, strategy_name, reason, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    decision.id,
                        decision.headline_id,
                        decision.symbol,
                        decision.broker.value,
                        decision.direction.value,
                        decision.quantity,
                        decision.confidence,
                        decision.urgency.value,
                    decision.time_horizon.value,
                    decision.strategy_name,
                    decision.reason,
                    json.dumps(
                        {
                            **decision.metadata,
                            "order_type": decision.order_type,
                            "reference_price": decision.reference_price,
                            "atr": decision.atr,
                            "stop_loss": decision.stop_loss,
                            "take_profit": decision.take_profit,
                            "time_exit_at": decision.time_exit_at.isoformat()
                            if decision.time_exit_at
                            else None,
                        }
                    ),
                ],
            )

    def record_execution(self, report: ExecutionReport) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO executions (
                    decision_id, symbol, broker, status, broker_order_id, filled_quantity,
                    average_fill_price, executed_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    report.decision_id,
                    report.symbol,
                    report.broker.value,
                    report.status,
                    report.broker_order_id,
                    report.filled_quantity,
                    report.average_fill_price,
                    report.executed_at,
                    json.dumps(report.metadata),
                ],
            )

    def load_headlines(self, limit: int = 500) -> list[HeadlineEvent]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, source, source_kind, text, published_at, url, provider,
                       article_id, body, symbols_json, metadata_json, dedupe_key
                FROM headlines
                ORDER BY published_at ASC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [self._row_to_headline(row) for row in rows]

    def source_performance(self) -> list[tuple[str, int, int]]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT h.source, COUNT(*) AS headlines, COUNT(e.decision_id) AS executions
                FROM headlines h
                LEFT JOIN trade_decisions d ON d.headline_id = h.id
                LEFT JOIN executions e ON e.decision_id = d.id
                GROUP BY 1
                ORDER BY headlines DESC, executions DESC
                """
            ).fetchall()

    def recent_scored_headlines(self, minutes: int) -> list[HeadlineEvent]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT h.id, h.source, h.source_kind, h.text, h.published_at, h.url, h.provider,
                       h.article_id, h.body, h.symbols_json, h.metadata_json, h.dedupe_key
                FROM headlines h
                INNER JOIN scores s ON s.headline_id = h.id
                WHERE h.published_at >= ?
                ORDER BY h.published_at DESC
                """,
                [datetime.now(UTC) - timedelta(minutes=minutes)],
            ).fetchall()
        return [self._row_to_headline(row) for row in rows]

    def count_recent_confirmations(self, symbol: str, direction: str, minutes: int) -> int:
        with self._lock:
            result = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM trade_decisions
                WHERE symbol = ?
                  AND direction = ?
                  AND created_at >= ?
                """,
                [symbol, direction, datetime.now(UTC) - timedelta(minutes=minutes)],
            ).fetchone()
        return int(result[0]) if result else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_headline(row: tuple[object, ...]) -> HeadlineEvent:
        (
            headline_id,
            source,
            source_kind,
            text,
            published_at,
            url,
            provider,
            article_id,
            body,
            symbols_json,
            metadata_json,
            dedupe_key,
        ) = row
        return HeadlineEvent(
            id=str(headline_id),
            source=str(source),
            source_kind=str(source_kind),
            text=str(text),
            published_at=published_at if isinstance(published_at, datetime) else datetime.now(UTC),
            url=url if isinstance(url, str) else None,
            provider=provider if isinstance(provider, str) else None,
            article_id=article_id if isinstance(article_id, str) else None,
            body=body if isinstance(body, str) else None,
            symbols=json.loads(symbols_json) if isinstance(symbols_json, str) and symbols_json else [],
            metadata=json.loads(metadata_json) if isinstance(metadata_json, str) and metadata_json else {},
            dedupe_key=str(dedupe_key) if dedupe_key else None,
        )
