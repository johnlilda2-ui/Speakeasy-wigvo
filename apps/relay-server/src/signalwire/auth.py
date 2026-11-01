"""SignalWire webhook and media-WebSocket authentication.

The Compatibility API signs HTTP callbacks with the project's Dashboard
Signing Key. Media Streams use the separate authBearerToken setting, which
SignalWire sends as an Authorization: Bearer header during the WS handshake.
"""
from __future__ import annotations

import hmac
import logging

from fastapi import HTTPException, Request, WebSocket
from twilio.request_validator import RequestValidator

from src.config import settings
from src.twilio.signature import public_http_url

logger = logging.getLogger(__name__)


async def validate_signalwire_http_request(request: Request):
    """Return form data only after validating SignalWire's webhook signature."""
    signing_key = settings.signalwire_signing_key
    if not signing_key:
        logger.error("SignalWire webhook signing key is not configured")
        raise HTTPException(
            status_code=503,
            detail="SignalWire webhook signature validation is not configured",
        )

    try:
        url = public_http_url(
            request.url.path,
            request.scope.get("query_string", b"").decode("latin-1"),
        )
        validator = RequestValidator(signing_key)
    except RuntimeError as exc:
        logger.error("SignalWire webhook validation misconfigured: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="SignalWire webhook signature validation is not configured",
        ) from exc

    form = await request.form()
    signature = request.headers.get("x-signalwire-signature", "")
    if not signature or not validator.validate(url, form, signature):
        logger.warning("Rejected invalid SignalWire webhook path=%s", request.url.path)
        raise HTTPException(status_code=403, detail="Invalid SignalWire signature")
    return form


async def validate_signalwire_websocket(ws: WebSocket) -> bool:
    """Authenticate a SignalWire Media Stream before accepting its upgrade."""
    expected = settings.signalwire_stream_token
    if not expected:
        logger.error("SignalWire media stream bearer token is not configured")
        await ws.close(
            code=1011,
            reason="SignalWire media stream authentication is not configured",
        )
        return False

    authorization = ws.headers.get("authorization", "")
    scheme, separator, supplied = authorization.partition(" ")
    if (
        not separator
        or scheme.lower() != "bearer"
        or not supplied.strip()
    ):
        await ws.close(code=4403, reason="Missing SignalWire bearer token")
        return False

    # compare_digest avoids ordinary string-comparison timing leakage.
    if not hmac.compare_digest(supplied.strip(), expected):
        logger.warning("Rejected SignalWire media WebSocket without valid bearer token")
        await ws.close(code=4403, reason="Invalid SignalWire bearer token")
        return False
    return True
