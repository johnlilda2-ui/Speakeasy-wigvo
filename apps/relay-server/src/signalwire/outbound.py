"""SignalWire Compatibility API outbound PSTN call control."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID

from src.config import settings
from src.twilio.signature import public_http_url

logger = logging.getLogger(__name__)
_HTTP_TIMEOUT_S = 20


def _credentials() -> tuple[str, str, str]:
    """Return validated Space URL, Project ID, and API Token."""
    space_url = settings.signalwire_space_url.rstrip("/")
    project_id = settings.signalwire_project_id.strip()
    api_token = settings.signalwire_api_token.strip()
    if not space_url or not project_id or not api_token:
        raise RuntimeError(
            "SIGNALWIRE_SPACE_URL, SIGNALWIRE_PROJECT_ID, and "
            "SIGNALWIRE_API_TOKEN must be configured"
        )
    try:
        UUID(project_id)
    except ValueError as exc:
        raise RuntimeError("SIGNALWIRE_PROJECT_ID must be a UUID Project ID") from exc
    parsed = urlsplit(space_url)
    hostname = (parsed.hostname or "").lower()
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("SIGNALWIRE_SPACE_URL has an invalid port") from exc
    if (
        parsed.scheme != "https"
        or not hostname.endswith(".signalwire.com")
        or hostname == "signalwire.com"
        or port is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise RuntimeError(
            "SIGNALWIRE_SPACE_URL must be an HTTPS SignalWire Space origin"
        )
    return space_url, project_id, api_token


def _api_request(path: str, data: dict[str, object]) -> dict[str, object]:
    """Make an authenticated, form-encoded request to SignalWire."""
    space_url, project_id, api_token = _credentials()
    account = quote(project_id, safe="")
    url = (
        f"{space_url}/api/laml/2010-04-01/Accounts/{account}/{path.lstrip('/')}"
    )
    credentials = base64.b64encode(
        f"{project_id}:{api_token}".encode("utf-8")
    ).decode("ascii")
    body = urlencode(data, doseq=True).encode("utf-8")
    request = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=_HTTP_TIMEOUT_S) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        # Do not log the provider response body: it can contain caller data.
        logger.error(
            "SignalWire API request failed: status=%s endpoint=%s",
            exc.code,
            path,
        )
        raise RuntimeError(
            f"SignalWire API rejected the request (HTTP {exc.code})"
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        logger.error("SignalWire API connection failed endpoint=%s: %s", path, exc)
        raise RuntimeError("Could not connect to SignalWire API") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("SignalWire API returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("SignalWire API returned an unexpected response")
    return payload


def _validate_e164(number: str, label: str) -> None:
    if not re.fullmatch(r"\+[1-9]\d{1,14}", number):
        raise ValueError(f"{label} must be in E.164 format")


def _validate_call_id(call_id: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", call_id):
        raise ValueError("call_id must contain only URL-safe identifier characters")


def make_call(
    phone_number: str,
    call_id: str,
    outbound_number: str,
) -> str:
    """Create a PSTN call whose cXML attaches WIGVO's bidirectional stream."""
    _validate_e164(phone_number, "Destination number")
    _validate_e164(outbound_number, "SignalWire outbound number")
    _validate_call_id(call_id)
    call_id_path = quote(call_id, safe="")
    webhook_url = public_http_url(f"/signalwire/webhook/{call_id_path}")
    status_callback_url = public_http_url(
        f"/signalwire/status-callback/{call_id_path}"
    )
    payload = _api_request(
        "Calls",
        {
            "To": phone_number,
            "From": outbound_number,
            "Url": webhook_url,
            "Method": "POST",
            "StatusCallback": status_callback_url,
            "StatusCallbackMethod": "POST",
            "StatusCallbackEvent": [
                "initiated",
                "ringing",
                "answered",
                "completed",
            ],
            "Timeout": settings.recipient_answer_timeout_s,
        },
    )
    sid = payload.get("sid")
    if not isinstance(sid, str) or not sid.strip():
        raise RuntimeError("SignalWire API response did not include a call SID")
    logger.info("SignalWire call created: call_id=%s", call_id)
    return sid.strip()


def hangup_call(call_sid: str) -> None:
    """Terminate an active SignalWire PSTN call using its Compatibility SID."""
    sid_path = quote(call_sid, safe="")
    payload = _api_request(f"Calls/{sid_path}", {"Status": "completed"})
    returned_sid = payload.get("sid")
    if returned_sid and returned_sid != call_sid:
        raise RuntimeError("SignalWire hang-up response SID did not match request")
    logger.info("SignalWire call terminated: call_sid=%s", call_sid)


async def make_call_async(
    phone_number: str,
    call_id: str,
    tenant_id: UUID | str,
) -> str:
    """Resolve the tenant's SignalWire DID and perform REST call creation off-loop."""
    from src.db.pg_client import get_tenant_outbound_number

    outbound_number = await get_tenant_outbound_number(
        tenant_id,
        provider="signalwire",
    )
    create_task = asyncio.create_task(
        asyncio.to_thread(
            make_call,
            phone_number=phone_number,
            call_id=call_id,
            outbound_number=outbound_number,
        )
    )
    try:
        return await asyncio.shield(create_task)
    except asyncio.CancelledError:
        # Cancelling asyncio.to_thread does not stop its worker thread. Wait
        # for the provider response and release the PSTN leg before propagating
        # cancellation, otherwise an untracked call can remain active.
        try:
            call_sid = await asyncio.shield(create_task)
        except Exception:
            logger.exception(
                "SignalWire call creation failed while start request was cancelled "
                "(call_id=%s)",
                call_id,
            )
        else:
            try:
                await hangup_call_async(call_sid)
            except Exception:
                logger.exception(
                    "Failed to hang up SignalWire call created during cancellation "
                    "(call_id=%s, call_sid=%s)",
                    call_id,
                    call_sid,
                )
        raise


async def hangup_call_async(call_sid: str) -> None:
    """Perform provider call termination without blocking the event loop."""
    await asyncio.to_thread(hangup_call, call_sid)
