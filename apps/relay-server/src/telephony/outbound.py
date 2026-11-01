"""Provider selection boundary for WIGVO outbound calls."""
from __future__ import annotations

import logging
from uuid import UUID

from src.config import settings

logger = logging.getLogger(__name__)


async def make_provider_call_async(
    phone_number: str,
    call_id: str,
    tenant_id: UUID | str,
) -> str:
    """Dispatch call creation to the configured PSTN provider."""
    provider = settings.telephony_provider
    if provider == "twilio":
        from src.twilio.outbound import make_call_async

        return await make_call_async(
            phone_number=phone_number,
            call_id=call_id,
            tenant_id=tenant_id,
        )
    if provider == "signalwire":
        from src.signalwire.outbound import make_call_async

        return await make_call_async(
            phone_number=phone_number,
            call_id=call_id,
            tenant_id=tenant_id,
        )
    # Settings validates this, but keep the dispatch boundary fail-closed.
    raise RuntimeError(f"Unsupported telephony provider: {provider}")


async def hangup_provider_call_async(provider: str, call_sid: str) -> None:
    """Terminate a provider call after a start/end race without crossing providers."""
    if provider == "twilio":
        from src.twilio.outbound import get_twilio_client
        import asyncio

        client = get_twilio_client()
        await asyncio.to_thread(
            client.calls(call_sid).update,
            status="completed",
        )
        return
    if provider == "signalwire":
        from src.signalwire.outbound import hangup_call_async

        await hangup_call_async(call_sid)
        return
    raise RuntimeError(f"Unsupported telephony provider: {provider}")
