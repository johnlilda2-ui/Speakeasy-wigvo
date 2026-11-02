"""SignalWire Compatibility API transport helpers for the Soniox WIGVO app."""
from __future__ import annotations

import base64
import hmac
import json
import logging
import re
import xml.etree.ElementTree as ET
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID

from twilio.request_validator import RequestValidator

logger = logging.getLogger("speakeasy_wigvo.signalwire")
HTTP_TIMEOUT_S = 20


def validate_credentials(space_url: str, project_id: str, api_token: str) -> tuple[str, str, str]:
    space_url = space_url.strip().rstrip("/")
    project_id = project_id.strip()
    api_token = api_token.strip()
    if not space_url or not project_id or not api_token:
        raise RuntimeError(
            "SIGNALWIRE_SPACE_URL, SIGNALWIRE_PROJECT_ID, and SIGNALWIRE_API_TOKEN are required"
        )
    try:
        project_id = str(UUID(project_id))
    except ValueError as exc:
        raise RuntimeError("SIGNALWIRE_PROJECT_ID must be a UUID Project ID") from exc
    parsed = urlsplit(space_url)
    hostname = (parsed.hostname or "").lower()
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("SIGNALWIRE_SPACE_URL contains an invalid port") from exc
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
        raise RuntimeError("SIGNALWIRE_SPACE_URL must be an HTTPS SignalWire Space origin")
    return space_url, project_id, api_token


def _api_request(
    space_url: str,
    project_id: str,
    api_token: str,
    path: str,
    data: dict[str, object],
) -> dict[str, object]:
    space_url, project_id, api_token = validate_credentials(
        space_url, project_id, api_token
    )
    url = (
        f"{space_url}/api/laml/2010-04-01/Accounts/"
        f"{quote(project_id, safe='')}/{path.lstrip('/')}"
    )
    credentials = base64.b64encode(
        f"{project_id}:{api_token}".encode("utf-8")
    ).decode("ascii")
    request = Request(
        url,
        data=urlencode(data, doseq=True).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raw_error = exc.read(16384).decode("utf-8", errors="replace")
        message = ""
        provider_code = ""
        try:
            error_payload = json.loads(raw_error)
        except json.JSONDecodeError:
            error_payload = {}
        if isinstance(error_payload, dict):
            message = str(
                error_payload.get("message")
                or error_payload.get("error_message")
                or error_payload.get("error")
                or ""
            )
            provider_code = str(error_payload.get("code") or error_payload.get("error_code") or "")
        # Never echo credentials or arbitrary provider response fields to logs/UI.
        for secret in (api_token, project_id):
            if secret:
                message = message.replace(secret, "[redacted]")
        message = re.sub(r"\\+?[0-9][0-9 ()-]{6,}[0-9]", "[redacted number]", message)
        message = re.sub(r"[\\r\\n\\t]+", " ", message).strip()[:240]
        provider_code = re.sub(r"[^A-Za-z0-9_.-]", "", provider_code)[:40]
        logger.error(
            "SignalWire API rejected request endpoint=%s status=%s provider_code=%s message=%s",
            path, exc.code, provider_code or "unavailable", message or "no safe message returned",
        )
        detail = f"SignalWire API rejected the request (HTTP {exc.code}"
        if provider_code:
            detail += f", code {provider_code}"
        detail += ")"
        if message:
            detail += f": {message}"
        raise RuntimeError(detail) from exc
    except (URLError, TimeoutError, OSError) as exc:
        logger.error("SignalWire API connection failed endpoint=%s error_type=%s", path, type(exc).__name__)
        raise RuntimeError("Could not connect to SignalWire API") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("SignalWire API returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("SignalWire API returned an unexpected response")
    return payload


def create_call(
    *,
    space_url: str,
    project_id: str,
    api_token: str,
    phone: str,
    from_number: str,
    public_base_url: str,
    session_id: str,
    leg: str,
    timeout_s: int = 30,
) -> str:
    for label, number in (("Destination number", phone), ("SignalWire number", from_number)):
        if not re.fullmatch(r"\+[1-9][0-9]{1,14}", number):
            raise ValueError(f"{label} must be in E.164 format")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", session_id) or leg not in {"a", "b"}:
        raise ValueError("Invalid WIGVO session or leg identifier")
    base = public_base_url.strip().rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment:
        raise RuntimeError("PUBLIC_BASE_URL must be an HTTPS origin for SignalWire calls")
    webhook = f"{base}/signalwire/voice/{session_id}/{leg}"
    status = f"{base}/signalwire/status/{session_id}/{leg}"
    result = _api_request(
        space_url,
        project_id,
        api_token,
        "Calls",
        {
            "To": phone,
            "From": from_number,
            "Url": webhook,
            "Method": "POST",
            "StatusCallback": status,
            "StatusCallbackMethod": "POST",
            "StatusCallbackEvent": ["initiated", "ringing", "answered", "completed"],
            "Timeout": timeout_s,
        },
    )
    sid = result.get("sid")
    if not isinstance(sid, str) or not sid.strip():
        raise RuntimeError("SignalWire API response did not include a call SID")
    return sid.strip()


def hangup_call(
    *, space_url: str, project_id: str, api_token: str, call_sid: str
) -> None:
    if not call_sid.strip():
        return
    result = _api_request(
        space_url,
        project_id,
        api_token,
        f"Calls/{quote(call_sid, safe='')}",
        {"Status": "completed"},
    )
    returned_sid = result.get("sid")
    if returned_sid and returned_sid != call_sid:
        raise RuntimeError("SignalWire hang-up response SID did not match request")


def build_cxml(
    *,
    stream_url: str,
    bearer_token: str,
    status_callback_url: str,
    session_id: str,
    leg: str,
) -> str:
    if not stream_url.startswith("wss://"):
        raise RuntimeError("SignalWire media streams require a secure WSS URL")
    if not bearer_token or len(bearer_token.encode("utf-8")) < 32 or not bearer_token.isascii():
        raise RuntimeError("SIGNALWIRE_STREAM_TOKEN must be at least 32 ASCII bytes")
    response = ET.Element("Response")
    connect = ET.SubElement(response, "Connect")
    stream = ET.SubElement(
        connect,
        "Stream",
        {
            "url": stream_url,
            "authBearerToken": bearer_token,
            "codec": "PCMU@8000h",
            "realtime": "true",
            "statusCallback": status_callback_url,
            "statusCallbackMethod": "POST",
        },
    )
    ET.SubElement(stream, "Parameter", {"name": "session_id", "value": session_id})
    ET.SubElement(stream, "Parameter", {"name": "leg", "value": leg})
    return ET.tostring(response, encoding="utf-8", xml_declaration=True).decode("utf-8")


def validate_webhook_signature(
    *, url: str, form: object, signing_key: str, signature: str
) -> bool:
    if not signing_key or not signature:
        return False
    try:
        return bool(RequestValidator(signing_key).validate(url, form, signature))
    except Exception:
        logger.exception("SignalWire webhook signature validation raised")
        return False


def validate_bearer_header(authorization: str, expected_token: str) -> bool:
    if not expected_token:
        return False
    scheme, separator, supplied = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not supplied.strip():
        return False
    try:
        supplied_bytes = supplied.strip().encode("ascii")
        expected_bytes = expected_token.encode("ascii")
    except UnicodeEncodeError:
        return False
    return hmac.compare_digest(supplied_bytes, expected_bytes)
