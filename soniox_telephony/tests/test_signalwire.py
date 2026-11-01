from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from xml.etree import ElementTree as ET

import pytest
from fastapi.testclient import TestClient
from twilio.request_validator import RequestValidator

import app as soniox_app
from signalwire import (
    build_cxml,
    validate_bearer_header,
    validate_credentials,
)


SIGNING_KEY = "signalwire-test-signing-key"
STREAM_TOKEN = "signalwire-test-stream-token-with-more-than-32-bytes"
PUBLIC_BASE = "https://relay.example.com"


@pytest.fixture(autouse=True)
def signalwire_config(monkeypatch):
    monkeypatch.setattr(soniox_app, "PUBLIC_BASE_URL", PUBLIC_BASE)
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_SPACE_URL", "https://wigvo.signalwire.com")
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_PROJECT_ID", "00000000-0000-4000-8000-000000000001")
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_API_TOKEN", "test-api-token")
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_PHONE_NUMBER", "+12025550100")
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_SIGNING_KEY", SIGNING_KEY)
    monkeypatch.setattr(soniox_app, "SIGNALWIRE_STREAM_TOKEN", STREAM_TOKEN)
    monkeypatch.setattr(soniox_app, "SONIOX_API_KEY", "test-soniox-key")
    soniox_app.SESSIONS.clear()


def _signature(path: str, form: dict[str, str]) -> str:
    return RequestValidator(SIGNING_KEY).compute_signature(PUBLIC_BASE + path, form)


def _session(provider: str = "signalwire") -> soniox_app.CallSession:
    session = soniox_app.CallSession(
        session_id="session-test-123",
        language_a="en",
        language_b="pt",
        phone_a="+12025550101",
        phone_b="+12025550102",
        provider=provider,
    )
    session.legs = {
        "a": soniox_app.Leg(session, "a", "en", "pt", "Maya"),
        "b": soniox_app.Leg(session, "b", "pt", "en", "Adrian"),
    }
    soniox_app.SESSIONS[session.session_id] = session
    return session


def test_signalwire_credentials_require_secure_space_origin():
    with pytest.raises(RuntimeError, match="HTTPS SignalWire Space"):
        validate_credentials(
            "http://wigvo.signalwire.com",
            "00000000-0000-4000-8000-000000000001",
            "token",
        )


def test_signalwire_cxml_configures_secure_bidirectional_stream():
    xml = build_cxml(
        stream_url="wss://relay.example.com/signalwire/stream/session-test-123/a",
        bearer_token=STREAM_TOKEN,
        status_callback_url="https://relay.example.com/signalwire/status/session-test-123/a",
        session_id="session-test-123",
        leg="a",
    )
    root = ET.fromstring(xml)
    stream = root.find("./Connect/Stream")
    assert stream is not None
    assert stream.attrib["url"].startswith("wss://")
    assert stream.attrib["authBearerToken"] == STREAM_TOKEN
    assert stream.attrib["codec"] == "PCMU@8000h"
    assert stream.attrib["realtime"] == "true"
    assert stream.attrib["statusCallback"].endswith("/signalwire/status/session-test-123/a")
    assert {p.attrib["name"]: p.attrib["value"] for p in stream.findall("Parameter")} == {
        "session_id": "session-test-123",
        "leg": "a",
    }


def test_signalwire_bearer_validation_is_constant_time_safe():
    assert validate_bearer_header("Bearer " + STREAM_TOKEN, STREAM_TOKEN)
    assert not validate_bearer_header("Bearer wrong", STREAM_TOKEN)
    assert not validate_bearer_header("Basic " + STREAM_TOKEN, STREAM_TOKEN)
    assert not validate_bearer_header("Bearer " + STREAM_TOKEN, "")


def test_health_reports_signalwire_configured_without_exposing_secrets():
    with TestClient(soniox_app.app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["signalwire_configured"] is True
    assert "SIGNALWIRE_API_TOKEN" not in json.dumps(body)
    assert STREAM_TOKEN not in json.dumps(body)


def test_signalwire_voice_webhook_returns_signed_cxml():
    session = _session()
    path = f"/signalwire/voice/{session.session_id}/a"
    form = {"CallSid": "SW-call-a"}
    with TestClient(soniox_app.app) as client:
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )
    assert response.status_code == 200
    root = ET.fromstring(response.content)
    stream = root.find("./Connect/Stream")
    assert stream is not None
    assert stream.attrib["url"] == (
        f"wss://relay.example.com/signalwire/stream/{session.session_id}/a"
    )
    assert stream.attrib["authBearerToken"] == STREAM_TOKEN
    assert session.legs["a"].call_sid == "SW-call-a"


def test_signalwire_voice_webhook_rejects_invalid_signature():
    session = _session()
    path = f"/signalwire/voice/{session.session_id}/a"
    with TestClient(soniox_app.app) as client:
        response = client.post(
            path,
            data={"CallSid": "SW-call-a"},
            headers={"X-SignalWire-Signature": "invalid"},
        )
    assert response.status_code == 403


def test_signalwire_status_callback_validates_sid_and_ends_session():
    session = _session()
    path = f"/signalwire/status/{session.session_id}/a"
    form = {"CallSid": "SW-call-a", "CallStatus": "completed"}
    with (
        patch.object(session, "close", new=AsyncMock()) as close,
        TestClient(soniox_app.app) as client,
    ):
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )
    assert response.status_code == 200
    close.assert_awaited_once_with(reason="signalwire_completed")
    assert session.legs["a"].call_sid == "SW-call-a"
    assert session.session_id not in soniox_app.SESSIONS


def test_signalwire_status_callback_rejects_sid_mismatch():
    session = _session()
    session.legs["a"].call_sid = "SW-expected"
    path = f"/signalwire/status/{session.session_id}/a"
    form = {"CallSid": "SW-other", "CallStatus": "completed"}
    with TestClient(soniox_app.app) as client:
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )
    assert response.status_code == 403
    assert session.session_id in soniox_app.SESSIONS


def test_twilio_voice_route_rejects_signalwire_owned_session():
    session = _session("signalwire")
    with TestClient(soniox_app.app) as client:
        response = client.post(f"/twilio/voice/{session.session_id}/a")
    assert response.status_code == 200
    assert b"<Reject" in response.content


def test_twilio_remains_the_default_provider(monkeypatch):
    monkeypatch.setattr(soniox_app, "TWILIO_ACCOUNT_SID", "AC-test")
    monkeypatch.setattr(soniox_app, "TWILIO_AUTH_TOKEN", "twilio-test-token")
    monkeypatch.setattr(soniox_app, "TWILIO_PHONE_NUMBER", "+12025550199")
    monkeypatch.setattr(
        soniox_app,
        "initiate_twilio_call",
        AsyncMock(side_effect=["CA-leg-a", "CA-leg-b"]),
    )
    with TestClient(soniox_app.app) as client:
        response = client.post(
            "/api/calls",
            json={
                "phone_a": "+12025550101",
                "phone_b": "+12025550102",
                "language_a": "en",
                "language_b": "pt",
            },
        )
    assert response.status_code == 200
    assert response.json()["provider"] == "twilio"
    assert response.json()["call_sids"] == {"a": "CA-leg-a", "b": "CA-leg-b"}
    soniox_app.SESSIONS.pop(response.json()["session_id"], None)


def test_signalwire_provider_selection_uses_signalwire_dialer(monkeypatch):
    monkeypatch.setattr(
        soniox_app,
        "initiate_signalwire_call",
        AsyncMock(side_effect=["SW-leg-a", "SW-leg-b"]),
    )
    with TestClient(soniox_app.app) as client:
        response = client.post(
            "/api/calls",
            json={
                "phone_a": "+12025550101",
                "phone_b": "+12025550102",
                "language_a": "en",
                "language_b": "pt",
                "provider": "signalwire",
            },
        )
    assert response.status_code == 200
    assert response.json()["provider"] == "signalwire"
    assert response.json()["call_sids"] == {"a": "SW-leg-a", "b": "SW-leg-b"}
    soniox_app.SESSIONS.pop(response.json()["session_id"], None)


def test_partial_signalwire_call_setup_hangs_up_created_leg(monkeypatch):
    monkeypatch.setattr(
        soniox_app,
        "initiate_signalwire_call",
        AsyncMock(side_effect=["SW-created-leg", RuntimeError("provider unavailable")]),
    )
    hangup = MagicMock()
    monkeypatch.setattr(soniox_app, "hangup_signalwire_call", hangup)
    with TestClient(soniox_app.app) as client:
        response = client.post(
            "/api/calls",
            json={
                "phone_a": "+12025550101",
                "phone_b": "+12025550102",
                "language_a": "en",
                "language_b": "pt",
                "provider": "signalwire",
            },
        )
    assert response.status_code == 502
    hangup.assert_called_once()
    assert not soniox_app.SESSIONS


def test_signalwire_stream_error_callback_ends_both_call_legs():
    session = _session()
    path = f"/signalwire/status/{session.session_id}/a"
    form = {"CallSid": "SW-call-a", "StreamEvent": "stream-error"}
    with (
        patch.object(session, "close", new=AsyncMock()) as close,
        TestClient(soniox_app.app) as client,
    ):
        response = client.post(
            path,
            data=form,
            headers={"X-SignalWire-Signature": _signature(path, form)},
        )
    assert response.status_code == 200
    close.assert_awaited_once_with(reason="signalwire_stream_error")
    assert session.session_id not in soniox_app.SESSIONS
