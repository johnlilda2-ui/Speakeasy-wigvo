"""SignalWire cXML webhook and bidirectional Media Stream endpoints."""
from __future__ import annotations

import hmac
import logging
import xml.etree.ElementTree as ET

from fastapi import (
    APIRouter,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import Response

from src.call_manager import call_manager
from src.config import settings
from src.logging_config import call_id_var, call_mode_var, tenant_id_var
from src.inbound.bootstrap import cleanup_inbound_session
from src.inbound.media import pending_media_registry
from src.inbound.service import DispatchError, DispatchNotFound, DispatchUnavailable, dispatch_service
from uuid import NAMESPACE_URL, uuid5
from src.realtime.audio_router import AudioRouter
from src.signalwire.auth import (
    validate_signalwire_http_request,
    validate_signalwire_websocket,
)
from src.signalwire.media_stream import SignalWireMediaStreamHandler
from src.twilio.signature import public_http_url, public_websocket_url
from src.types import WsMessage, WsMessageType

router = APIRouter(tags=["signalwire"])
logger = logging.getLogger(__name__)


def _signalwire_cxml(call_id: str) -> str:
    """Build a secured, bidirectional cXML stream using the WIGVO PCMU pipeline."""
    bearer_token = settings.signalwire_stream_token
    if not bearer_token:
        raise HTTPException(
            status_code=503,
            detail="SignalWire media stream authentication is not configured",
        )

    try:
        stream_url = public_websocket_url(
            f"/signalwire/media-stream/{call_id}"
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail="SignalWire media stream URL is not configured",
        ) from exc

    # SignalWire only accepts secure WebSocket URLs. Do not silently downgrade.
    if not stream_url.startswith("wss://"):
        raise HTTPException(
            status_code=503,
            detail="SignalWire media streams require an HTTPS public callback URL",
        )

    try:
        stream_status_url = public_http_url(
            f"/signalwire/status-callback/{call_id}"
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail="SignalWire stream status callback URL is not configured",
        ) from exc

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
            "statusCallback": stream_status_url,
            "statusCallbackMethod": "POST",
        },
    )
    ET.SubElement(
        stream,
        "Parameter",
        {"name": "call_id", "value": call_id},
    )
    return ET.tostring(
        response,
        encoding="utf-8",
        xml_declaration=True,
    ).decode("utf-8")


@router.post("/incoming")
async def signalwire_incoming(request: Request):
    """Resolve a SignalWire DID and establish the provider-aware pending media lifecycle."""
    form = await validate_signalwire_http_request(request)
    provider_call_sid = str(form.get("CallSid", "")).strip()
    inbound_number = str(form.get("To", "")).strip()
    if not provider_call_sid or not inbound_number:
        logger.warning("Rejected SignalWire inbound call with missing CallSid/To")
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="rejected"/></Response>',
            media_type="application/xml",
        )

    try:
        tenant_id, languages = await dispatch_service.resolve_tenant(
            inbound_number, provider="signalwire"
        )
    except DispatchNotFound:
        logger.warning("Rejected unmapped SignalWire inbound DID=%s", inbound_number)
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="rejected"/></Response>',
            media_type="application/xml",
        )
    except Exception:
        logger.exception("SignalWire inbound DID resolution unavailable")
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="busy"/></Response>',
            media_type="application/xml",
        )
    if len(languages) < 2 or not languages[0] or not languages[1]:
        logger.error("Rejected SignalWire inbound DID with invalid language mapping=%s", inbound_number)
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="rejected"/></Response>',
            media_type="application/xml",
        )

    deterministic_call_id = uuid5(NAMESPACE_URL, f"wigvo:signalwire:{provider_call_sid}")
    dispatch = None
    try:
        dispatch = await dispatch_service.create_ringing(
            call_id=deterministic_call_id,
            tenant_id=tenant_id,
            provider_call_sid=provider_call_sid,
            provider="signalwire",
        )
        await pending_media_registry.prepare(
            call_id=dispatch.call_id,
            tenant_id=dispatch.tenant_id,
            languages=dispatch.languages or languages,
            provider_call_sid=provider_call_sid,
            provider="signalwire",
        )
        cxml = _signalwire_cxml(str(dispatch.call_id))
    except DispatchUnavailable:
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="busy"/></Response>',
            media_type="application/xml",
        )
    except (DispatchError, RuntimeError, ValueError, HTTPException):
        logger.exception("Failed to create SignalWire inbound dispatch")
        if dispatch is not None:
            try:
                await dispatch_service.finish(dispatch.call_id, "media_prepare_failed")
            except Exception:
                logger.exception("Failed to roll back SignalWire inbound dispatch")
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="rejected"/></Response>',
            media_type="application/xml",
        )

    call_id_var.set(str(dispatch.call_id))
    logger.info(
        "SignalWire inbound dispatch created call=%s tenant=%s did=%s",
        dispatch.call_id,
        dispatch.tenant_id,
        inbound_number,
    )
    return Response(content=cxml, media_type="application/xml")


@router.post("/webhook/{call_id}")
async def signalwire_webhook(call_id: str, request: Request):
    """Return SignalWire cXML only for a correctly signed provider callback."""
    form = await validate_signalwire_http_request(request)
    call = call_manager.get_call(call_id)
    if call is None or call.telephony_provider != "signalwire":
        raise HTTPException(status_code=404, detail="SignalWire call not found")

    provider_call_sid = str(form.get("CallSid", "")).strip()
    if not provider_call_sid:
        raise HTTPException(status_code=400, detail="SignalWire CallSid is required")
    if call.call_sid and not hmac.compare_digest(
        call.call_sid.encode("utf-8"),
        provider_call_sid.encode("utf-8"),
    ):
        raise HTTPException(status_code=403, detail="SignalWire CallSid mismatch")
    if not call.call_sid and provider_call_sid:
        # The webhook may arrive before the create-call REST request returns.
        call.call_sid = provider_call_sid

    cxml = _signalwire_cxml(call_id)
    call_id_var.set(call_id)
    logger.info("SignalWire cXML webhook served for call=%s", call_id)
    return Response(content=cxml, media_type="application/xml")


@router.post("/status-callback/{call_id}")
async def signalwire_status_callback(call_id: str, request: Request):
    """Authenticate provider callbacks and converge terminal states on call cleanup."""
    form = await validate_signalwire_http_request(request)
    call_status = str(form.get("CallStatus", "")).strip().lower()
    stream_event = str(form.get("StreamEvent", "")).strip().lower()
    call_sid = str(form.get("CallSid", "")).strip()
    if not call_sid:
        raise HTTPException(status_code=400, detail="SignalWire CallSid is required")

    call_id_var.set(call_id)
    call = call_manager.get_call(call_id)
    if call is not None:
        if call.telephony_provider != "signalwire":
            raise HTTPException(status_code=404, detail="SignalWire call not found")
        if call.call_sid and not hmac.compare_digest(
            call.call_sid.encode("utf-8"),
            call_sid.encode("utf-8"),
        ):
            raise HTTPException(status_code=403, detail="SignalWire CallSid mismatch")
        if not call.call_sid and call_sid:
            # Capture the provider SID if a status callback races the REST response.
            call.call_sid = call_sid

    logger.info(
        "SignalWire status callback: call_id=%s status=%s stream_event=%s sid=%s",
        call_id,
        call_status,
        stream_event,
        call_sid,
    )

    terminal_statuses = {"completed", "failed", "busy", "no-answer", "canceled"}
    terminal_stream_events = {"stream-stopped", "stream-error"}
    if call_status in terminal_statuses or stream_event in terminal_stream_events:
        reason = (
            f"signalwire_{call_status}"
            if call_status in terminal_statuses
            else f"signalwire_{stream_event.replace('-', '_')}"
        )
        if await pending_media_registry.contains(call_id, provider="signalwire"):
            await cleanup_inbound_session(call_id, reason)
        else:
            await call_manager.cleanup_call(call_id, reason=reason)

    return {"status": "ok"}


@router.websocket("/media-stream/{call_id}")
async def signalwire_media_stream(ws: WebSocket, call_id: str):
    """Run a SignalWire bidirectional stream through the existing WIGVO router."""
    if not await validate_signalwire_websocket(ws):
        return

    try:
        pending_handler = await pending_media_registry.attach(call_id, ws)
    except RuntimeError:
        await ws.close(code=4409, reason="SignalWire inbound stream already connected")
        return
    if pending_handler is not None:
        call_id_var.set(call_id)
        call_mode_var.set("voice_to_voice")
        tenant_id_var.set(str(pending_handler.pending.tenant_id))
        await ws.accept()
        await pending_handler.run()
        return

    call = call_manager.get_call(call_id)
    if call is None:
        await ws.close(code=4404, reason="WIGVO call not found")
        return
    if call.telephony_provider != "signalwire":
        await ws.close(code=4403, reason="Call belongs to a different PSTN provider")
        return

    dual_session = call_manager.get_session(call_id)
    if dual_session is None:
        await ws.close(code=1011, reason="WIGVO call session is not ready")
        return

    # Do not let a second provider socket replace the media router of an active call.
    if call_manager.get_router(call_id) is not None:
        await ws.close(code=4409, reason="SignalWire stream already connected")
        return

    handler = SignalWireMediaStreamHandler(ws=ws, call=call)

    async def send_to_app(message: WsMessage) -> None:
        await call_manager.send_to_app(call_id, message)

    audio_router = AudioRouter(
        call=call,
        dual_session=dual_session,
        # AudioRouter consumes the provider-neutral send/extract interface.
        twilio_handler=handler,
        app_ws_send=send_to_app,
        prompt_a=call.prompt_a,
        prompt_b=call.prompt_b,
    )
    # Claim the router slot before accepting. This synchronous check-and-set
    # is atomic within the event loop, so only one stream can own this call.
    if not call_manager.try_register_router(call_id, audio_router):
        await ws.close(code=4409, reason="SignalWire stream already connected")
        return

    # Never let CallManager send a SignalWire SID to Twilio's REST client.
    call.telephony_provider = "signalwire"

    call_id_var.set(call_id)
    call_mode_var.set(call.communication_mode.value)
    tenant_id_var.set(str(call.tenant_id))

    reason = "signalwire_disconnected"
    try:
        await ws.accept()
        logger.info("SignalWire Media Stream connected (call=%s)", call_id)
        await audio_router.start()

        listen_task = asyncio.create_task(dual_session.listen_all())
        call_manager.register_listen_task(call_id, listen_task)

        await send_to_app(
            WsMessage(
                type=WsMessageType.CALL_STATUS,
                data={
                    "status": "connected",
                    "message": "SignalWire call media connected.",
                },
            )
        )

        while True:
            raw = await ws.receive_text()
            event = await handler.handle_message(raw)

            if event and event.event == "media" and handler.stream_sid:
                audio = handler.extract_audio(event)
                if audio:
                    await audio_router.handle_twilio_audio(audio)

            if event and event.event == "stop":
                reason = "signalwire_stopped"
            if handler.is_closed:
                break

    except WebSocketDisconnect:
        logger.info("SignalWire Media Stream disconnected (call=%s)", call_id)
    except Exception:
        reason = "signalwire_media_error"
        logger.exception("SignalWire Media Stream failed (call=%s)", call_id)
    finally:
        await handler.close()
        await call_manager.cleanup_call(call_id, reason=reason)
