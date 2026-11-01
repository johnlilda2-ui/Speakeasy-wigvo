"""SignalWire Compatibility API call creation and termination tests."""
from __future__ import annotations

import json
from urllib.parse import parse_qs
from unittest.mock import patch

import pytest

from src.config import settings
from src.signalwire import outbound


class FakeResponse:
    def __init__(self, payload: dict[str, object]):
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return self._payload


@pytest.fixture(autouse=True)
def configure_signalwire(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_space_url", "https://wigvo-test.signalwire.com")
    monkeypatch.setattr(settings, "signalwire_project_id", "00000000-0000-0000-0000-000000000123")
    monkeypatch.setattr(settings, "signalwire_api_token", "private-api-token")
    monkeypatch.setattr(settings, "signalwire_signing_key", "signalwire-signing-test-key")
    monkeypatch.setattr(
        settings,
        "signalwire_stream_token",
        "test-signalwire-stream-token-at-least-32-bytes",
    )
    monkeypatch.setattr(settings, "public_callback_base_url", "https://relay.example.com")
    monkeypatch.setattr(settings, "recipient_answer_timeout_s", 35)


def test_make_call_uses_signalwire_compatibility_api_and_signed_callbacks():
    captured = {}

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeResponse({"sid": "SW-call-123"})

    with patch("src.signalwire.outbound.urlopen", side_effect=fake_urlopen):
        sid = outbound.make_call(
            phone_number="+14155550123",
            call_id="call-123",
            outbound_number="+12025550100",
        )

    request = captured["request"]
    form = parse_qs(request.data.decode("utf-8"))

    assert sid == "SW-call-123"
    assert request.full_url == (
        "https://wigvo-test.signalwire.com/api/laml/2010-04-01/"
        "Accounts/00000000-0000-0000-0000-000000000123/Calls"
    )
    assert request.get_method() == "POST"
    assert request.get_header("Authorization").startswith("Basic ")
    assert form["To"] == ["+14155550123"]
    assert form["From"] == ["+12025550100"]
    assert form["Url"] == [
        "https://relay.example.com/signalwire/webhook/call-123"
    ]
    assert form["StatusCallback"] == [
        "https://relay.example.com/signalwire/status-callback/call-123"
    ]
    assert form["StatusCallbackEvent"] == [
        "initiated",
        "ringing",
        "answered",
        "completed",
    ]
    assert form["Timeout"] == ["35"]
    assert captured["timeout"] == 20


def test_make_call_rejects_non_e164_numbers_before_network_request():
    with patch("src.signalwire.outbound.urlopen") as urlopen:
        with pytest.raises(ValueError, match="E.164"):
            outbound.make_call(
                phone_number="4155550123",
                call_id="call-123",
                outbound_number="+12025550100",
            )

    urlopen.assert_not_called()


def test_make_call_requires_all_server_credentials(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_api_token", "")
    with pytest.raises(RuntimeError, match="SIGNALWIRE_API_TOKEN"):
        outbound.make_call(
            phone_number="+14155550123",
            call_id="call-123",
            outbound_number="+12025550100",
        )



def test_make_call_fails_before_dial_when_stream_bearer_is_missing(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_stream_token", "")
    with patch("src.signalwire.outbound.urlopen") as urlopen:
        with pytest.raises(RuntimeError, match="SIGNALWIRE_STREAM_TOKEN"):
            outbound.make_call(
                phone_number="+14155550123",
                call_id="call-123",
                outbound_number="+12025550100",
            )
    urlopen.assert_not_called()


def test_make_call_fails_before_dial_when_webhook_signing_key_is_missing(monkeypatch):
    monkeypatch.setattr(settings, "signalwire_signing_key", "")
    with patch("src.signalwire.outbound.urlopen") as urlopen:
        with pytest.raises(RuntimeError, match="SIGNALWIRE_SIGNING_KEY"):
            outbound.make_call(
                phone_number="+14155550123",
                call_id="call-123",
                outbound_number="+12025550100",
            )
    urlopen.assert_not_called()


def test_make_call_requires_public_https_callback_origin(monkeypatch):
    monkeypatch.setattr(settings, "public_callback_base_url", "http://localhost:8000")
    with patch("src.signalwire.outbound.urlopen") as urlopen:
        with pytest.raises(RuntimeError, match="must use HTTPS"):
            outbound.make_call(
                phone_number="+14155550123",
                call_id="call-123",
                outbound_number="+12025550100",
            )
    urlopen.assert_not_called()


def test_hangup_call_posts_completed_status_to_the_specific_call_sid():
    captured = {}

    def fake_urlopen(request, timeout):
        captured["request"] = request
        return FakeResponse({"sid": "SW-call-123", "status": "completed"})

    with patch("src.signalwire.outbound.urlopen", side_effect=fake_urlopen):
        outbound.hangup_call("SW-call-123")

    request = captured["request"]
    form = parse_qs(request.data.decode("utf-8"))
    assert request.full_url.endswith(
        "/Accounts/00000000-0000-0000-0000-000000000123/Calls/SW-call-123"
    )
    assert request.get_method() == "POST"
    assert form == {"Status": ["completed"]}


def test_signalwire_settings_reject_unsupported_provider_and_untrusted_host():
    from src.config import Settings

    with pytest.raises(ValueError, match="telephony_provider"):
        Settings(telephony_provider="another-provider")
    with pytest.raises(ValueError, match="HTTPS origin"):
        Settings(signalwire_space_url="https://attacker.example")


def test_hangup_call_rejects_mismatched_response_sid():
    with patch(
        "src.signalwire.outbound.urlopen",
        return_value=FakeResponse({"sid": "SW-other-call"}),
    ):
        with pytest.raises(RuntimeError, match="SID did not match"):
            outbound.hangup_call("SW-call-123")


@pytest.mark.asyncio
async def test_provider_dispatcher_uses_signalwire_when_selected(monkeypatch):
    from src.telephony.outbound import make_provider_call_async
    from unittest.mock import AsyncMock

    monkeypatch.setattr(settings, "telephony_provider", "signalwire")
    with patch(
        "src.signalwire.outbound.make_call_async",
        new=AsyncMock(return_value="SW-call-123"),
    ) as signalwire_call:
        result = await make_provider_call_async(
            phone_number="+14155550123",
            call_id="call-123",
            tenant_id="00000000-0000-0000-0000-000000000001",
        )

    assert result == "SW-call-123"
    signalwire_call.assert_awaited_once_with(
        phone_number="+14155550123",
        call_id="call-123",
        tenant_id="00000000-0000-0000-0000-000000000001",
    )


@pytest.mark.asyncio
async def test_provider_dispatcher_keeps_twilio_as_default(monkeypatch):
    from src.telephony.outbound import make_provider_call_async
    from unittest.mock import AsyncMock

    monkeypatch.setattr(settings, "telephony_provider", "twilio")
    with patch(
        "src.twilio.outbound.make_call_async",
        new=AsyncMock(return_value="CA-call-123"),
    ) as twilio_call:
        result = await make_provider_call_async(
            phone_number="+14155550123",
            call_id="call-123",
            tenant_id="00000000-0000-0000-0000-000000000001",
        )

    assert result == "CA-call-123"
    twilio_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_tenant_number_resolution_queries_selected_provider(monkeypatch):
    from src.db import pg_client
    from unittest.mock import AsyncMock

    class Connection:
        fetchval = AsyncMock(return_value="+12025550100")

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    monkeypatch.setattr(pg_client, "get_pool", AsyncMock(return_value=Pool()))
    number = await pg_client.get_tenant_outbound_number(
        "00000000-0000-0000-0000-000000000001",
        provider="signalwire",
    )

    assert number == "+12025550100"


@pytest.mark.asyncio
async def test_default_tenant_signalwire_fallback_replaces_twilio_default(monkeypatch):
    from src.db import pg_client
    from unittest.mock import AsyncMock

    class Connection:
        def __init__(self):
            self.calls = []

        async def fetchval(self, query, *args):
            self.calls.append((query, args))
            return None if len(self.calls) == 1 else "+12025550100"

    connection = Connection()

    class Acquire:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, *_args):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    monkeypatch.setattr(pg_client, "get_pool", AsyncMock(return_value=Pool()))
    monkeypatch.setattr(settings, "signalwire_phone_number", "+12025550100")

    number = await pg_client.get_tenant_outbound_number(
        "00000000-0000-0000-0000-000000000001",
        provider="signalwire",
    )

    assert number == "+12025550100"
    assert "provider <> $3 OR outbound_number = ''" in connection.calls[1][0]
    assert connection.calls[1][1][2] == "signalwire"


@pytest.mark.asyncio
async def test_non_default_tenant_does_not_fall_back_to_global_signalwire_number(monkeypatch):
    from src.db import pg_client
    from unittest.mock import AsyncMock

    class Connection:
        fetchval = AsyncMock(return_value=None)

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    monkeypatch.setattr(pg_client, "get_pool", AsyncMock(return_value=Pool()))
    monkeypatch.setattr(settings, "signalwire_phone_number", "+12025550100")

    with pytest.raises(LookupError, match="provider signalwire"):
        await pg_client.get_tenant_outbound_number(
            "00000000-0000-0000-0000-000000000002",
            provider="signalwire",
        )


def test_make_call_rejects_unsafe_call_id_before_network_request():
    with patch("src.signalwire.outbound.urlopen") as urlopen:
        with pytest.raises(ValueError, match="URL-safe identifier"):
            outbound.make_call(
                phone_number="+14155550123",
                call_id="../other-call",
                outbound_number="+12025550100",
            )

    urlopen.assert_not_called()


@pytest.mark.asyncio
async def test_provider_hangup_dispatcher_uses_signalwire_adapter():
    from unittest.mock import AsyncMock
    from src.telephony.outbound import hangup_provider_call_async

    with patch(
        "src.signalwire.outbound.hangup_call_async",
        new=AsyncMock(),
    ) as signalwire_hangup:
        await hangup_provider_call_async("signalwire", "SW-call-123")

    signalwire_hangup.assert_awaited_once_with("SW-call-123")


@pytest.mark.asyncio
async def test_provider_hangup_dispatcher_rejects_unknown_provider():
    from src.telephony.outbound import hangup_provider_call_async

    with pytest.raises(RuntimeError, match="Unsupported telephony provider"):
        await hangup_provider_call_async("unknown", "call-123")


@pytest.mark.asyncio
async def test_cancelled_signalwire_create_hangs_up_call_created_by_worker(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    started = asyncio.Event()
    release_worker = asyncio.Event()

    async def fake_to_thread(_func, **_kwargs):
        started.set()
        await release_worker.wait()
        return "SW-created-during-cancel"

    monkeypatch.setattr(outbound.asyncio, "to_thread", fake_to_thread)
    from src.db import pg_client
    monkeypatch.setattr(
        pg_client,
        "get_tenant_outbound_number",
        AsyncMock(return_value="+12025550100"),
    )

    with patch(
        "src.signalwire.outbound.hangup_call_async",
        new=AsyncMock(),
    ) as hangup:
        task = asyncio.create_task(
            outbound.make_call_async(
                phone_number="+14155550123",
                call_id="call-123",
                tenant_id="00000000-0000-0000-0000-000000000001",
            )
        )
        await started.wait()
        task.cancel()
        release_worker.set()

        with pytest.raises(asyncio.CancelledError):
            await task

    hangup.assert_awaited_once_with("SW-created-during-cancel")


@pytest.mark.asyncio
async def test_cancelled_provider_dispatch_hangs_up_late_created_twilio_call(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock
    from src.telephony.outbound import make_provider_call_async

    started = asyncio.Event()
    release_provider = asyncio.Event()

    async def slow_twilio_create(**_kwargs):
        started.set()
        await release_provider.wait()
        return "CA-created-during-cancel"

    monkeypatch.setattr(settings, "telephony_provider", "twilio")
    with (
        patch(
            "src.twilio.outbound.make_call_async",
            side_effect=slow_twilio_create,
        ),
        patch(
            "src.telephony.outbound.hangup_provider_call_async",
            new=AsyncMock(),
        ) as hangup,
    ):
        task = asyncio.create_task(
            make_provider_call_async(
                phone_number="+14155550123",
                call_id="call-123",
                tenant_id="00000000-0000-0000-0000-000000000001",
            )
        )
        await started.wait()
        task.cancel()
        release_provider.set()

        with pytest.raises(asyncio.CancelledError):
            await task

    hangup.assert_awaited_once_with("twilio", "CA-created-during-cancel")
