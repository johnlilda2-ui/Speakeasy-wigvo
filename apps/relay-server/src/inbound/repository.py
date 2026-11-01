from __future__ import annotations

from typing import Iterable
from uuid import UUID

import asyncpg

from src.db.pg_client import get_pool
from src.inbound.models import ALLOWED_TRANSITIONS, DispatchRecord, DispatchState


def _record(row: asyncpg.Record | None) -> DispatchRecord | None:
    if row is None:
        return None
    return DispatchRecord.model_validate(dict(row))


async def resolve_inbound_tenant(
    inbound_number: str,
    *,
    provider: str = "twilio",
) -> tuple[UUID, list[str]] | None:
    """Resolve an inbound DID only within the selected PSTN provider."""
    if provider not in {"twilio", "signalwire"}:
        raise ValueError("Unsupported inbound telephony provider")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, languages
            FROM tenant_call_config
            WHERE provider = $1 AND inbound_number = $2
            """,
            provider,
            inbound_number,
        )
    if row is None:
        return None
    return UUID(str(row["tenant_id"])), list(row["languages"] or [])
