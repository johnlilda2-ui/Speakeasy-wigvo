"""SignalWire cXML webhook and bidirectional Media Stream endpoints."""
from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response

from src.call_manager import call_manager
from src.config import settings
from src.logging_config import call_id_var, call_mode_var, tenant_id_var
from src.realtime.audio_router import AudioRouter
from src.signalwire.auth import (
    validate_signalwire_http_request,
    validate_signalwire_websocket,
)
from src.signalwire.media_stream import SignalWireMediaStreamHandler
from src.twilio.signature import public_websocket_url
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

    response = ET.Element("Response")
    connect = ET.SubElement(response, "Connect")
    stream = ET.SubElement(
        connect,
        "Stream",
        {
            "url": stream_url,
            "authBearerToken": bearer_token,
            "realtime": "true",
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


@router.post("/webhook/{call_id}")
async def signalwire_webhook(call_id: str, request: Request):
    """Return SignalWire cXML only for a correctly signed provider callback."""
    await validate_signalwire_http_request(request)
    cxml = _signalwire_cxml(call_id)
    call_id_var.set(call_id)
    logger.info("SignalWire cXML webhook served for call=%s", call_id)
    return Response(content=cxml, media_type="application/xml")


@router.websocket("/media-stream/{call_id}")
async def signalwire_media_stream(ws: WebSocket, call_id: str):
    """Run a SignalWire bidirectional stream through the existing WIGVO router."""
    if not await validate_signalwire_websocket(ws):
        return

    call = call_manager.get_call(call_id)
    if call is None:
        await ws.close(code=4404, reason="WIGVO call not found")
        return

    dual_session = call_manager.get_session(call_id)
    if dual_session is None:
        await ws.close(code=1011, reason="WIGVO call session is not ready")
        return

    # Mark the provider before cleanup can run; CallManager must not send a
    # SignalWire call SID to the Twilio REST client.
    call.telephony_provider = "signalwire"

    # Do not let a second provider socket replace the media router of an active call.
    if call_manager.get_router(call_id) is not None:
        await ws.close(code=4409, reason="SignalWire stream already connected")
        return

    await ws.accept()
    logger.info("SignalWire Media Stream connected (call=%s)", call_id)

    call_id_var.set(call_id)
    call_mode_var.set(call.communication_mode.value)
    tenant_id_var.set(str(call.tenant_id))

    handler = SignalWireMediaStreamHandler(ws=ws, call=call)
    reason = "signalwire_disconnected"
    try:
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
        call_manager.register_router(call_id, audio_router)
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
