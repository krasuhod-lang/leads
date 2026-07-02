#!/usr/bin/env python3
"""
sync_leads.py — второй контур загрузки данных leads.su в тот же stats.db.

Не заменяет PHP-`update_stats` — тот остаётся для UI-триггера. Этот скрипт
предназначен для системного cron/systemd-timer как замена cron_update.php:
он делает те же 4 прохода к /webmaster/reports/summary + /platforms +
/offers, но с типизацией ответов, экспоненциальным backoff и явным
rate-limiter (2 rps), уважающим Retry-After.

Использование:
    LEADS_API_TOKEN=... python3 tools/sync_leads.py \
        --db /path/to/stats.db --from 2026-06-01 --to 2026-07-01

Зависимости: httpx, tenacity, aiolimiter (pip install httpx tenacity aiolimiter).
Скрипт заботится о leads_api_lock.txt (тот же lock, что использует PHP),
чтобы не бежать параллельно с UI-триггером.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    import httpx
    from aiolimiter import AsyncLimiter
    from tenacity import (
        AsyncRetrying,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
    )
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(
        "Не найдены зависимости httpx/tenacity/aiolimiter.\n"
        "Установите: pip install httpx tenacity aiolimiter\n"
    )
    raise SystemExit(1) from exc


BASE_URL = "https://webmaster.leads.su"
DEFAULT_LIMIT = 500


class LeadsRateLimit(Exception):
    def __init__(self, retry_after: int):
        super().__init__(f"rate limited, retry after {retry_after}s")
        self.retry_after = retry_after


@contextmanager
def acquire_lock(lock_path: Path):
    """Совместимо с leadsApiAcquireLock() из leads-proxy.php — flock(LOCK_EX|LOCK_NB)."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("leads_api_lock.txt удержан другим процессом, выхожу.")
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fh.close()


class LeadsClient:
    """Тонкий обёрточный клиент над leads.su webmaster API."""

    def __init__(self, token: str, limiter: AsyncLimiter):
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=httpx.Timeout(30.0),
            headers={"Accept": "application/json"},
        )
        self._token = token
        self._limiter = limiter

    async def close(self):
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        params = {**params, "token": self._token}
        async with self._limiter:
            resp = await self._client.get(path, params=params)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After") or 5)
            raise LeadsRateLimit(retry_after)
        resp.raise_for_status()
        return resp.json()

    async def get_paged(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        limit = DEFAULT_LIMIT
        while True:
            page_params = {**params, "offset": offset, "limit": limit}
            async for attempt in AsyncRetrying(
                retry=retry_if_exception_type((LeadsRateLimit, httpx.HTTPError)),
                stop=stop_after_attempt(5),
                wait=wait_exponential(multiplier=1, min=1, max=30),
                reraise=True,
            ):
                with attempt:
                    body = await self._get(path, page_params)
            data = body.get("data") or []
            rows.extend(data)
            if len(data) < limit:
                break
            offset += limit
        return rows


def upsert_daily_stats(db: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    """Пишет в daily_stats, полностью соответствуя UNIQUE-ключу PHP-кода.

    Скрипт нарочно НЕ создаёт таблицы — их создаёт PHP-код при инициализации.
    """
    count = 0
    for row in rows:
        date = _parse_date(row.get("period_day") or row.get("period"))
        if not date:
            continue
        platform_id = str(row.get("platform_id") or "").strip()
        source_name = (row.get("source") or "").strip()
        if not platform_id and not source_name:
            # account-агрегат, пропускаем — иначе задвоим клики.
            continue
        offer_id = str(row.get("offer_id") or "")
        sub1 = str(row.get("aff_sub1") or row.get("sub1") or "")
        source_id = platform_id or f"src:{abs(hash(source_name)) % 4294967296:x}"
        try:
            db.execute(
                """
                INSERT INTO daily_stats
                    (date, source_id, source_name, offer_id, offer_name, sub1,
                     clicks, raw_clicks, conversions, approved, revenue)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, source_id, offer_id, sub1) DO UPDATE SET
                    source_name = excluded.source_name,
                    offer_name = excluded.offer_name,
                    clicks = excluded.clicks,
                    raw_clicks = excluded.raw_clicks,
                    conversions = excluded.conversions,
                    approved = excluded.approved,
                    revenue = excluded.revenue
                """,
                (
                    date,
                    source_id,
                    source_name or f"Platform #{platform_id}",
                    offer_id,
                    row.get("offer_name") or (f"Offer #{offer_id}" if offer_id else "Unknown offer"),
                    sub1,
                    int(row.get("unique_clicks") or 0),
                    int(row.get("clicks") or 0),
                    int(row.get("unique_conversions") or row.get("conversions") or 0),
                    int(row.get("conversions_approved") or 0),
                    float(row.get("payout") or 0),
                ),
            )
            count += 1
        except sqlite3.Error as exc:
            sys.stderr.write(f"skip row (sqlite error {exc}): {row}\n")
    db.commit()
    return count


def _parse_date(raw: Any) -> str | None:
    if not raw:
        return None
    s = str(raw)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def write_sync_log(
    db: sqlite3.Connection,
    run_id: str,
    pass_name: str,
    rows: int | None,
    http_status: int | None,
    message: str = "",
) -> None:
    """Совместимо со схемой sync_log из leads-proxy.php."""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        """INSERT INTO sync_log (run_id, pass, rows, http_status, retry_after,
                                 started_at, finished_at, message)
           VALUES (?, ?, ?, ?, NULL, ?, ?, ?)""",
        (run_id, pass_name, rows, http_status, now, now, message[:500]),
    )
    db.commit()


async def main_async(args: argparse.Namespace) -> int:
    token = os.environ.get("LEADS_API_TOKEN") or args.token
    if not token:
        sys.stderr.write("Токен не задан: установите LEADS_API_TOKEN или --token.\n")
        return 2
    db_path = Path(args.db)
    if not db_path.exists():
        sys.stderr.write(f"БД {db_path} не найдена. Сначала запустите PHP-интерфейс, чтобы создать схему.\n")
        return 2

    run_id = f"py-{uuid.uuid4().hex[:8]}-{int(time.time())}"
    limiter = AsyncLimiter(max_rate=2, time_period=1)  # 2 rps
    client = LeadsClient(token=token, limiter=limiter)
    db = sqlite3.connect(db_path)

    date_from = f"{args.date_from} 00:00:00"
    date_to = f"{args.date_to} 23:59:59"

    try:
        with acquire_lock(db_path.parent / "leads_api_lock.txt"):
            # /reports/summary с полной детализацией.
            try:
                rows = await client.get_paged(
                    "/webmaster/reports/summary",
                    {
                        "start_date": date_from,
                        "end_date": date_to,
                        "grouping": "day",
                        "field": "offer_id,source,aff_sub1",
                    },
                )
                saved = upsert_daily_stats(db, rows)
                write_sync_log(db, run_id, "daily_stats", saved, 200, f"received {len(rows)}")
                print(f"daily_stats: rows={len(rows)} saved={saved}")
            except LeadsRateLimit as e:
                write_sync_log(db, run_id, "daily_stats", 0, 429, f"retry_after={e.retry_after}")
                raise SystemExit(3)
    finally:
        await client.close()
        db.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Загрузка leads.su в stats.db (второй контур).")
    parser.add_argument("--db", required=True, help="Путь к stats.db")
    parser.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    parser.add_argument("--token", default=None, help="LEADS_API_TOKEN override (иначе из окружения)")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
