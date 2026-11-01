"""SignalWire inbound routing and pending-media regression tests."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from twilio.request_validator import RequestValidator

from src.config import settings
from src.inbound.media import PendingCall, PendingMediaHandler, pending_media_registry
from src.inbound.models import DispatchRecord, DispatchState
from src.main import app
from src.signalwire.media_stream import SignalWireMediaStreamHandler

SIGNING_KEY = "signalwire-test-signing-key"
STREAM_TOKEN = "test-signalwire-stream-token-at-least-32-bytes"
PUBLIC_BASE = "https://relay.example.com"
TENANT_ID = UUID("10000000-0000-0000-0000-000000000001")
CALL_ID = UUID("50000000-0000-0000-0000-000000000005")


class FakeWebSocket:
    def __init__(self):
        self.sent = []
        self.closed = []

    async def send_json(self, message):
        self.sent.append(message)

    async def close(self, code=1000, reason=None):
        self.closed.append((code, reason))


@pytest.fixture(autouse=True)
def configure_signalwire(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_signing_key", SIGNING_KEY)
    monkeypatch.setattr(settings, "signalwire_stream_token", STREAM_TOKEN)
    monkeypatch.setattr(settings, "public_callback_base_url", PUBLIC_BASE)


def _signature(path: str, form: dict[str, str]) -> str:
    return RequestValidator(SIGNING_KEY).compute_signature(f"{PUBLIC_BASE}{path}", form)


@pytest.mark.asyncio
async def test_pending_inbound_handler_selects_signalwire_adapter():
    pending = PendingCall(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        languages=("ko", "en"),
        provider_call_sid="SW-inbound",
        provider="signalwire",
    )
    ws = FakeWebSocket()
    handler = PendingMediaHandler(ws, pending)

    assert isinstance(handler.media, SignalWireMediaStreamHandler)
    assert handler.call.telephony_provider == "signalwire"

    event = await handler.media.handle_message(
        '{"event":"start","start":{"streamSid":"SW-stream","callSid":"SW-inbound"}}'
    )
    assert event is not None and event.event == "start"
    assert handler.media.stream_sid == "SW-stream"
    await handler.close()


def test_signalwire_incoming_creates_provider_scoped_dispatch_and_cxml():
    path = "/signalwire/incoming"
    form = {"CallSid": "SW-inbound-123", "To": "+12025550100"}
    now = datetime.now(timezone.utc)
    dispatch = DispatchRecord(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        provider="signalwire",
        provider_call_sid=form["CallSid"],
        state=DispatchState.RINGING,
        version=0,
        created_at=now,
        updated_at=now,
        languages=["ko", "en"],
    )
    prepare = AsyncMock()
    with (
        patch(
            "src.routes.signalwire_webhook.dispatch_service.resolve_tenant",
            new=AsyncMock(return_value=(TENANT_ID, ["ko", "en"])),
        ),
        patch(
            "src.routes.signalwire_webhook.dispatch_service.create_ringing",
            new=AsyncMock(return_value=dispatch),
        ) as create_ringing,
        patch(
            "src.routes.signalwire_webhook.pending_media_registry.prepare",
            new=prepare,
        ),
        TestClient(app) as client,
    ):
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )

    assert response.status_code == 200
    assert b"/signalwire/media-stream/" in response.content
    assert STREAM_TOKEN.encode() in response.content
    create_ringing.assert_awaited_once()
    assert create_ringing.await_args.kwargs["provider"] == "signalwire"
    prepare.assert_awaited_once()
    assert prepare.await_args.kwargs["provider"] == "signalwire"
    assert prepare.await_args.kwargs["provider_call_sid"] == form["CallSid"]


@pytest.mark.asyncio
async def test_provider_specific_stream_attach_cannot_claim_other_provider_call():
    await pending_media_registry.prepare(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        languages=["ko", "en"],
        provider_call_sid="CA-twilio-inbound",
        provider="twilio",
    )
    ws = FakeWebSocket()
    try:
        wrong_provider = await pending_media_registry.attach(
            str(CALL_ID), ws, provider="signalwire"
        )
        assert wrong_provider is None

        twilio_handler = await pending_media_registry.attach(
            str(CALL_ID), ws, provider="twilio"
        )
        assert twilio_handler is not None
        assert twilio_handler.call.telephony_provider == "twilio"
        await twilio_handler.close()
    finally:
        await pending_media_registry.pop(str(CALL_ID))


def test_signalwire_terminal_callback_cleans_matching_pending_inbound_call():
    path = f"/signalwire/status-callback/{CALL_ID}"
    form = {
        "CallSid": "SW-inbound-123",
        "CallStatus": "completed",
        "StreamEvent": "",
    }
    pending = PendingCall(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        languages=("ko", "en"),
        provider_call_sid=form["CallSid"],
        provider="signalwire",
    )
    cleanup = AsyncMock()
    with (
        patch(
            "src.routes.signalwire_webhook.pending_media_registry.get_pending",
            new=AsyncMock(return_value=pending),
        ),
        patch(
            "src.routes.signalwire_webhook.pending_media_registry.contains",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.routes.signalwire_webhook.cleanup_inbound_session",
            new=cleanup,
        ),
        patch(
            "src.routes.signalwire_webhook.call_manager.get_call",
            return_value=None,
        ),
        TestClient(app) as client,
    ):
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )

    assert response.status_code == 200
    cleanup.assert_awaited_once_with(str(CALL_ID), "signalwire_completed")


def test_signalwire_terminal_callback_rejects_mismatched_pending_call_sid():
    path = f"/signalwire/status-callback/{CALL_ID}"
    form = {
        "CallSid": "SW-someone-elses-call",
        "CallStatus": "completed",
        "StreamEvent": "",
    }
    pending = PendingCall(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        languages=("ko", "en"),
        provider_call_sid="SW-inbound-123",
        provider="signalwire",
    )
    cleanup = AsyncMock()
    with (
        patch(
            "src.routes.signalwire_webhook.pending_media_registry.get_pending",
            new=AsyncMock(return_value=pending),
        ),
        patch(
            "src.routes.signalwire_webhook.cleanup_inbound_session",
            new=cleanup,
        ),
        patch(
            "src.routes.signalwire_webhook.call_manager.get_call",
            return_value=None,
        ),
        TestClient(app) as client,
    ):
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )

    assert response.status_code == 403
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start,expected_closed",
    [
        (
            {
                "streamSid": "SW-stream",
                "callSid": "SW-someone-elses-call",
                "mediaFormat": {
                    "encoding": "audio/x-mulaw",
                    "sampleRate": 8000,
                    "channels": 1,
                },
            },
            True,
        ),
        (
            {
                "streamSid": "SW-stream",
                "callSid": "SW-inbound",
                "mediaFormat": {
                    "encoding": "audio/x-L16",
                    "sampleRate": 24000,
                    "channels": 1,
                },
            },
            True,
        ),
        (
            {
                "callSid": "SW-inbound",
                "mediaFormat": {
                    "encoding": "audio/x-mulaw",
                    "sampleRate": 8000,
                    "channels": 1,
                },
            },
            True,
        ),
    ],
)
async def test_signalwire_rejects_invalid_start_metadata(start, expected_closed):
    import json

    pending = PendingCall(
        call_id=CALL_ID,
        tenant_id=TENANT_ID,
        languages=("ko", "en"),
        provider_call_sid="SW-inbound",
        provider="signalwire",
    )
    handler = PendingMediaHandler(FakeWebSocket(), pending)

    event = await handler.media.handle_message(
        json.dumps({"event": "start", "start": start})
    )

    assert event is None
    assert handler.media.is_closed is expected_closed
    await handler.close()
