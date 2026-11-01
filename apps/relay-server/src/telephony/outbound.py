"""Provider selection boundary for WIGVO outbound calls."""
from __future__ import annotations

import asyncio
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
    elif provider == "signalwire":
        from src.signalwire.outbound import make_call_async
    else:
        # Settings validates this, but keep the dispatch boundary fail-closed.
        raise RuntimeError(f"Unsupported telephony provider: {provider}")

    create_task = asyncio.create_task(
        make_call_async(
            phone_number=phone_number,
            call_id=call_id,
            tenant_id=tenant_id,
        )
    )
    try:
        return await asyncio.shield(create_task)
    except asyncio.CancelledError:
        # Provider REST requests may be running in worker threads. Cancellation
        # must not abandon a successfully-created PSTN call without its SID.
        try:
            call_sid = await asyncio.shield(create_task)
        except Exception:
            logger.exception(
                "%s call creation failed while start request was cancelled "
                "(call_id=%s)",
                provider,
                call_id,
            )
        else:
            try:
                await hangup_provider_call_async(provider, call_sid)
            except Exception:
                logger.exception(
                    "Failed to terminate %s call created during cancellation "
                    "(call_id=%s)",
                    provider,
                    call_id,
                )
        raise


async def hangup_provider_call_async(provider: str, call_sid: str) -> None:
    """Terminate a provider call after a start/end race without crossing providers."""
    if provider == "twilio":
        from src.twilio.outbound import get_twilio_client

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
