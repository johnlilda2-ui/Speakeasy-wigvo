"""SignalWire media transport and security regression tests."""
from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from twilio.request_validator import RequestValidator
from xml.etree import ElementTree as ET

from src.call_manager import CallManager
from src.config import Settings, settings
from src.main import app
from src.signalwire.auth import validate_signalwire_websocket
from src.signalwire.media_stream import SignalWireMediaStreamHandler
from src.types import ActiveCall

SIGNING_KEY = "signalwire-test-signing-key"
STREAM_TOKEN = "test-signalwire-stream-token-at-least-32-bytes"
PUBLIC_BASE = "https://relay.example.com"


@pytest.fixture(autouse=True)
def configure_signalwire(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_signing_key", SIGNING_KEY)
    monkeypatch.setattr(settings, "signalwire_stream_token", STREAM_TOKEN)
    monkeypatch.setattr(settings, "public_callback_base_url", PUBLIC_BASE)


def _websocket(headers: dict[str, str] | None = None) -> MagicMock:
    ws = MagicMock()
    ws.headers = headers or {}
    ws.close = AsyncMock()
    return ws


@pytest.mark.asyncio
async def test_valid_signalwire_bearer_is_accepted_before_upgrade():
    ws = _websocket({"authorization": f"Bearer {STREAM_TOKEN}"})

    assert await validate_signalwire_websocket(ws) is True
    ws.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_signalwire_bearer_is_rejected_before_upgrade():
    ws = _websocket({"authorization": "Bearer wrong-token"})

    assert await validate_signalwire_websocket(ws) is False
    ws.close.assert_awaited_once_with(
        code=4403,
        reason="Invalid SignalWire bearer token",
    )


@pytest.mark.asyncio
async def test_missing_signalwire_bearer_is_rejected():
    ws = _websocket()

    assert await validate_signalwire_websocket(ws) is False
    ws.close.assert_awaited_once_with(
        code=4403,
        reason="Missing SignalWire bearer token",
    )


@pytest.mark.asyncio
async def test_missing_server_token_fails_closed(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_stream_token", "")
    ws = _websocket({"authorization": f"Bearer {STREAM_TOKEN}"})

    assert await validate_signalwire_websocket(ws) is False
    ws.close.assert_awaited_once()
    assert ws.close.await_args.kwargs["code"] == 1011


@pytest.mark.asyncio
async def test_signalwire_start_event_reads_nested_stream_sid():
    ws = MagicMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    call = ActiveCall(call_id="call-123")
    handler = SignalWireMediaStreamHandler(ws=ws, call=call)

    event = await handler.handle_message(
        json.dumps(
            {
                "event": "start",
                "sequenceNumber": "1",
                "start": {"streamSid": "SW-stream-123", "callSid": "SW-call-123"},
            }
        )
    )

    assert event is not None
    assert event.event == "start"
    assert handler.stream_sid == "SW-stream-123"
    assert call.stream_sid == "SW-stream-123"
    assert handler.is_closed is False


@pytest.mark.asyncio
async def test_signalwire_media_payload_decodes_and_outbound_envelopes_match_protocol():
    ws = MagicMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    call = ActiveCall(call_id="call-123")
    handler = SignalWireMediaStreamHandler(ws=ws, call=call)
    await handler.handle_message(
        json.dumps({"event": "start", "start": {"streamSid": "SW-stream-123"}})
    )

    pcmu = b"\x00\x7f\xff"
    event = await handler.handle_message(
        json.dumps(
            {
                "event": "media",
                "sequenceNumber": "2",
                "media": {"track": "inbound", "payload": base64.b64encode(pcmu).decode()},
            }
        )
    )
    assert event is not None
    assert handler.extract_audio(event) == pcmu

    await handler.send_audio(pcmu)
    await handler.send_clear()

    assert ws.send_json.await_args_list[0].args[0] == {
        "event": "media",
        "streamSid": "SW-stream-123",
        "media": {"payload": base64.b64encode(pcmu).decode()},
    }
    assert ws.send_json.await_args_list[1].args[0] == {
        "event": "clear",
        "streamSid": "SW-stream-123",
    }


@pytest.mark.asyncio
async def test_signalwire_start_rejects_non_pcmu_format():
    ws = MagicMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    handler = SignalWireMediaStreamHandler(ws=ws, call=ActiveCall(call_id="call-123"))

    event = await handler.handle_message(
        json.dumps(
            {
                "event": "start",
                "start": {
                    "streamSid": "SW-stream-123",
                    "mediaFormat": {
                        "encoding": "audio/x-L16",
                        "sampleRate": 16000,
                        "channels": 1,
                    },
                },
            }
        )
    )

    assert event is not None
    assert handler.is_closed is True


def test_non_ascii_signalwire_stream_token_is_rejected_by_settings():
    with pytest.raises(ValueError, match="ASCII bytes"):
        Settings(signalwire_stream_token="é" * 40)


@pytest.mark.asyncio
async def test_signalwire_stop_event_marks_handler_closed():
    ws = MagicMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    handler = SignalWireMediaStreamHandler(ws=ws, call=ActiveCall(call_id="call-123"))

    event = await handler.handle_message(json.dumps({"event": "stop"}))

    assert event is not None
    assert handler.is_closed is True


def test_call_manager_router_slot_only_accepts_one_stream_owner():
    manager = CallManager()
    first_router = object()
    second_router = object()

    assert manager.try_register_router("call-123", first_router) is True
    assert manager.try_register_router("call-123", second_router) is False
    assert manager.get_router("call-123") is first_router


@pytest.mark.asyncio
async def test_malformed_signalwire_start_event_closes_handler():
    ws = MagicMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    handler = SignalWireMediaStreamHandler(ws=ws, call=ActiveCall(call_id="call-123"))

    event = await handler.handle_message(json.dumps({"event": "start"}))

    assert event is None
    assert handler.is_closed is True


def _http_signature(path: str, form: dict[str, str]) -> str:
    return RequestValidator(SIGNING_KEY).compute_signature(
        f"{PUBLIC_BASE}{path}",
        form,
    )


def test_valid_signalwire_webhook_returns_signed_bidirectional_cxml():
    path = "/signalwire/webhook/call-123"
    form = {"CallSid": "SW-call-123"}
    with TestClient(app) as client:
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _http_signature(path, form)},
        )

    assert response.status_code == 200
    root = ET.fromstring(response.content)
    stream = root.find("./Connect/Stream")
    assert stream is not None
    assert stream.attrib["url"] == (
        "wss://relay.example.com/signalwire/media-stream/call-123"
    )
    assert stream.attrib["authBearerToken"] == STREAM_TOKEN
    assert stream.attrib["codec"] == "PCMU@8000h"
    assert stream.attrib["realtime"] == "true"
    assert stream.find("./Parameter").attrib == {
        "name": "call_id",
        "value": "call-123",
    }


def test_invalid_signalwire_webhook_signature_is_rejected():
    with TestClient(app) as client:
        response = client.post(
            "/signalwire/webhook/call-123",
            data={"CallSid": "SW-call-123"},
            headers={"X-SignalWire-Signature": "invalid"},
        )

    assert response.status_code == 403


def test_missing_signalwire_webhook_signing_key_fails_closed(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_signing_key", "")
    with TestClient(app) as client:
        response = client.post("/signalwire/webhook/call-123")

    assert response.status_code == 503


def test_short_signalwire_stream_token_is_rejected_by_settings():
    with pytest.raises(ValueError, match="at least 32 ASCII bytes"):
        Settings(signalwire_stream_token="too-short")
