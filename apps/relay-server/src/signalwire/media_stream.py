"""SignalWire bidirectional Media Stream adapter.

SignalWire puts streamSid inside the start object, unlike the top-level
streamSid consumed by the existing Twilio handler. Audio frames and the
bidirectional media/clear envelopes are otherwise compatible with the
current WIGVO AudioRouter when the default PCMU@8000h codec is used.
"""
from __future__ import annotations

import base64
import binascii
import json
import logging

from fastapi import WebSocket

from src.twilio.media_stream import TwilioMediaStreamHandler
from src.types import ActiveCall, TwilioMediaEvent

logger = logging.getLogger(__name__)


class SignalWireMediaStreamHandler(TwilioMediaStreamHandler):
    """Adapt SignalWire's nested start metadata to WIGVO's media-handler API."""

    def __init__(self, ws: WebSocket, call: ActiveCall):
        super().__init__(ws=ws, call=call)

    async def handle_message(self, raw: str) -> TwilioMediaEvent | None:
        """Parse SignalWire events and extract start.streamSid safely."""
        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("SignalWire event must be a JSON object")

            if data.get("event") == "start":
                start = data.get("start")
                if not isinstance(start, dict):
                    raise ValueError("SignalWire start event is missing start metadata")
                # Accept the provider's documented nested ID. The top-level
                # fallback also makes replay fixtures and compatible gateways safe.
                stream_sid = start.get("streamSid") or data.get("streamSid")
                if isinstance(stream_sid, str) and stream_sid:
                    data["streamSid"] = stream_sid

            event = TwilioMediaEvent(**data)
        except Exception as exc:
            logger.warning(
                "Failed to parse SignalWire media event (call=%s): %s",
                self.call.call_id,
                exc,
            )
            return None

        match event.event:
            case "connected":
                logger.info(
                    "SignalWire media stream connected (call=%s)",
                    self.call.call_id,
                )
            case "start":
                self.stream_sid = event.stream_sid or ""
                self.call.stream_sid = self.stream_sid
                if not self.stream_sid:
                    logger.error(
                        "SignalWire start event has no streamSid (call=%s)",
                        self.call.call_id,
                    )
                    self._closed = True
                else:
                    logger.info(
                        "SignalWire media stream started: stream_sid=%s",
                        self.stream_sid,
                    )
            case "media":
                pass  # The route forwards decoded audio to the active AudioRouter.
            case "stop":
                logger.info(
                    "SignalWire media stream stopped (call=%s)",
                    self.call.call_id,
                )
                self._closed = True

        return event

    def extract_audio(self, event: TwilioMediaEvent) -> bytes | None:
        """Decode the base64 PCMU payload supplied by SignalWire."""
        if not event.media:
            return None
        payload = event.media.get("payload")
        if not payload:
            return None
        try:
            return base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError):
            logger.warning(
                "Invalid base64 audio payload from SignalWire (call=%s)",
                self.call.call_id,
            )
            return None

    async def send_audio(self, audio_bytes: bytes) -> None:
        """Send raw PCMU audio back to the bidirectional SignalWire stream."""
        if self._closed or not self.stream_sid or not audio_bytes:
            return
        message = {
            "event": "media",
            "streamSid": self.stream_sid,
            "media": {"payload": base64.b64encode(audio_bytes).decode("ascii")},
        }
        try:
            await self.ws.send_json(message)
        except Exception:
            logger.warning(
                "Failed to send audio to SignalWire (call=%s)",
                self.call.call_id,
            )
            self._closed = True

    async def send_clear(self) -> None:
        """Interrupt pending SignalWire playback using its documented clear event."""
        if self._closed or not self.stream_sid:
            return
        try:
            await self.ws.send_json(
                {"event": "clear", "streamSid": self.stream_sid}
            )
        except Exception:
            logger.warning(
                "Failed to clear SignalWire playback buffer (call=%s)",
                self.call.call_id,
            )

    async def close(self) -> None:
        """Close the media stream once; safe after remote disconnects."""
        self._closed = True
        if self._ws_closed:
            return
        self._ws_closed = True
        try:
            await self.ws.close()
        except Exception:
            pass
