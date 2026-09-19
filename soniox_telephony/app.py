from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from html import escape
from typing import Any
from uuid import uuid4

import websockets
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from twilio.rest import Client
from twilio.request_validator import RequestValidator
from twilio.twiml.voice_response import VoiceResponse

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("speakeasy_wigvo")

SONIOX_API_KEY = os.getenv("SONIOX_API_KEY", "")
SONIOX_STT_URL = os.getenv("SONIOX_STT_URL", "wss://stt-rt.soniox.com/transcribe-websocket")
SONIOX_TTS_URL = os.getenv("SONIOX_TTS_URL", "wss://tts-rt.soniox.com/tts-websocket")
SONIOX_STT_MODEL = os.getenv("SONIOX_STT_MODEL", "stt-rt-v5")
SONIOX_TTS_MODEL = os.getenv("SONIOX_TTS_MODEL", "tts-rt-v2")
SONIOX_ENDPOINT_DELAY_MS = max(500, min(3000, int(os.getenv("SONIOX_ENDPOINT_DELAY_MS", "500"))))
SONIOX_TTS_VOICE_A = os.getenv("SONIOX_TTS_VOICE_A", "Maya")
SONIOX_TTS_VOICE_B = os.getenv("SONIOX_TTS_VOICE_B", "Adrian")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
TWILIO_VALIDATE_SIGNATURE = os.getenv("TWILIO_VALIDATE_SIGNATURE", "false").lower() == "true"
MAX_CALL_DURATION_S = max(60, int(os.getenv("MAX_CALL_DURATION_S", "1800")))

app = FastAPI(title="SpeakEasy WIGVO — Soniox Telephony Core", version="0.1.0")


def public_base_url() -> str:
    return PUBLIC_BASE_URL or os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")


def ensure_configured() -> None:
    missing = []
    if not SONIOX_API_KEY:
        missing.append("SONIOX_API_KEY")
    if not TWILIO_ACCOUNT_SID:
        missing.append("TWILIO_ACCOUNT_SID")
    if not TWILIO_AUTH_TOKEN:
        missing.append("TWILIO_AUTH_TOKEN")
    if not TWILIO_PHONE_NUMBER:
        missing.append("TWILIO_PHONE_NUMBER")
    if not public_base_url():
        missing.append("PUBLIC_BASE_URL")
    if missing:
        raise RuntimeError("Missing configuration: " + ", ".join(missing))


def validate_e164(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"\+[1-9]\d{7,14}", value):
        raise ValueError(f"Phone number must be E.164 format, got {value!r}")
    return value


async def validate_twilio_request(request: Request) -> dict[str, str]:
    form = await request.form()
    if TWILIO_VALIDATE_SIGNATURE:
        token = TWILIO_AUTH_TOKEN
        signature = request.headers.get("x-twilio-signature", "")
        if not token or not signature:
            raise HTTPException(status_code=403, detail="Invalid Twilio signature")
        validator = RequestValidator(token)
        if not validator.validate(str(request.url), dict(form), signature):
            raise HTTPException(status_code=403, detail="Invalid Twilio signature")
    return {str(k): str(v) for k, v in form.items()}


class StartCallRequest(BaseModel):
    phone_a: str = Field(..., description="First participant in E.164")
    phone_b: str = Field(..., description="Second participant in E.164")
    language_a: str = Field("en", min_length=2, max_length=10)
    language_b: str = Field("pt", min_length=2, max_length=10)


class Leg:
    def __init__(self, session: "CallSession", name: str, language: str, target_language: str, voice: str):
        self.session = session
        self.name = name
        self.language = language
        self.target_language = target_language
        self.voice = voice
        self.twilio_ws: WebSocket | None = None
        self.stream_sid = ""
        self.call_sid = ""
        self.twilio_connected = asyncio.Event()
        self.closed = False
        self.source_speaking = False
        self.stt_ws = None
        self.tts_ws = None
        self.tts_reader_task: asyncio.Task | None = None
        self.stt_reader_task: asyncio.Task | None = None
        self.current_tts_stream_id: str | None = None
        self.tts_stream_seq = 0
        self.tts_lock = asyncio.Lock()
        self.pending_tts_text = ""
        self.turn_started_at = 0.0
        self.last_audio_at = 0.0

    async def connect_soniox(self) -> None:
        self.stt_ws = await websockets.connect(
            SONIOX_STT_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )
        await self.stt_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_STT_MODEL,
            "audio_format": "mulaw",
            "sample_rate": 8000,
            "num_channels": 1,
            "language_hints": [self.language],
            "enable_endpoint_detection": True,
            "max_endpoint_delay_ms": SONIOX_ENDPOINT_DELAY_MS,
            "translation": {
                "type": "one_way",
                "target_language": self.target_language,
            },
        }))
        self.tts_ws = await websockets.connect(
            SONIOX_TTS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )
        self.tts_reader_task = asyncio.create_task(self._read_tts())
        log.info("Soniox connected leg=%s %s->%s", self.name, self.language, self.target_language)

    async def start_stt_reader(self) -> None:
        self.stt_reader_task = asyncio.create_task(self._read_stt())

    async def send_audio_to_stt(self, audio: bytes) -> None:
        if self.stt_ws and not self.closed:
            await self.stt_ws.send(audio)
            self.last_audio_at = time.monotonic()

    async def interrupt_output(self) -> None:
        peer = self.session.legs[self.session.other_leg(self.name)]
        async with peer.tts_lock:
            sid = peer.current_tts_stream_id
            if peer.tts_ws and sid:
                try:
                    await peer.tts_ws.send(json.dumps({"stream_id": sid, "cancel": True}))
                except Exception:
                    pass
                peer.current_tts_stream_id = None
                peer.pending_tts_text = ""
                log.info("barge_in source=%s destination=%s stream=%s", self.name, peer.name, sid)
            if peer.twilio_ws and peer.stream_sid:
                try:
                    await peer.twilio_ws.send_json({"event": "clear", "streamSid": peer.stream_sid})
                except Exception:
                    pass

    async def _open_tts_stream(self) -> str:
        async with self.tts_lock:
            if self.tts_ws is None:
                return ""
            if self.current_tts_stream_id:
                return self.current_tts_stream_id
            self.tts_stream_seq += 1
            sid = f"{self.name}-utt-{self.tts_stream_seq}"
            await self.tts_ws.send(json.dumps({
                "api_key": SONIOX_API_KEY,
                "model": SONIOX_TTS_MODEL,
                "language": self.target_language,
                "voice": self.voice,
                "audio_format": "pcm_mulaw",
                "sample_rate": 8000,
                "stream_id": sid,
                "speed": 1.0,
            }))
            self.current_tts_stream_id = sid
            return sid

    async def _send_tts_text(self, text: str, text_end: bool = False) -> None:
        if not text and not text_end:
            return
        sid = await self._open_tts_stream()
        if not sid or not self.tts_ws:
            return
        await self.tts_ws.send(json.dumps({
            "stream_id": sid,
            "text": text,
            "text_end": text_end,
        }))

    async def _flush_tts_buffer(self, final: bool = False) -> None:
        if final:
            text = self.pending_tts_text
            self.pending_tts_text = ""
            if text:
                await self._send_tts_text(text, text_end=True)
            elif self.current_tts_stream_id:
                await self._send_tts_text("", text_end=True)
            return

        text = self.pending_tts_text.strip()
        if not text:
            return
        boundary = max(text.rfind(" "), text.rfind(","), text.rfind("."), text.rfind("?"), text.rfind("!"))
        if len(text) >= 24 and boundary >= 12:
            chunk = text[:boundary + 1]
            self.pending_tts_text = text[boundary + 1:]
            await self._send_tts_text(chunk, text_end=False)

    async def _handle_translation(self, translated_text: str, is_final: bool) -> None:
        if not translated_text:
            return
        self.pending_tts_text += translated_text
        if is_final:
            await self._flush_tts_buffer(final=False)

    async def _read_stt(self) -> None:
        assert self.stt_ws is not None
        try:
            async for raw in self.stt_ws:
                data = json.loads(raw)
                if data.get("error_code") is not None:
                    log.error("Soniox STT error leg=%s code=%s msg=%s", self.name, data.get("error_code"), data.get("error_message"))
                    continue

                tokens = data.get("tokens") or []
                saw_source_token = False
                saw_endpoint = False
                for token in tokens:
                    text = str(token.get("text") or "")
                    status = token.get("translation_status")
                    is_final = bool(token.get("is_final"))
                    if text == "<end>":
                        saw_endpoint = True
                        continue
                    if not text:
                        continue

                    if status in (None, "none", "original"):
                        saw_source_token = True
                        if not self.source_speaking:
                            self.source_speaking = True
                            self.turn_started_at = time.monotonic()
                            await self.interrupt_output()

                    if status == "translation":
                        await self._handle_translation(text, is_final)

                if saw_source_token and self.session.dashboard_ws:
                    await self.session.send_event({
                        "type": "speech",
                        "leg": self.name,
                        "state": "speaking",
                    })

                if saw_endpoint:
                    self.source_speaking = False
                    await self._flush_tts_buffer(final=True)
                    latency = None
                    if self.turn_started_at:
                        latency = round((time.monotonic() - self.turn_started_at) * 1000, 1)
                    await self.session.send_event({
                        "type": "turn_complete",
                        "leg": self.name,
                        "latency_ms": latency,
                    })
                    self.turn_started_at = 0.0

                if data.get("finished"):
                    break
        except asyncio.CancelledError:
            pass
        except Exception:
            if not self.closed:
                log.exception("STT reader failed leg=%s", self.name)

    async def _read_tts(self) -> None:
        assert self.tts_ws is not None
        try:
            async for raw in self.tts_ws:
                data = json.loads(raw)
                if data.get("error_code") is not None:
                    log.error("Soniox TTS error leg=%s code=%s msg=%s", self.name, data.get("error_code"), data.get("error_message"))
                    continue
                audio_b64 = data.get("audio")
                if audio_b64 and self.twilio_ws and self.stream_sid:
                    await self.twilio_ws.send_json({
                        "event": "media",
                        "streamSid": self.stream_sid,
                        "media": {"payload": audio_b64},
                    })
                if data.get("terminated"):
                    sid = data.get("stream_id")
                    async with self.tts_lock:
                        if sid == self.current_tts_stream_id:
                            self.current_tts_stream_id = None
                            self.pending_tts_text = ""
        except asyncio.CancelledError:
            pass
        except Exception:
            if not self.closed:
                log.exception("TTS reader failed leg=%s", self.name)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for task in (self.stt_reader_task, self.tts_reader_task):
            if task:
                task.cancel()
        for ws in (self.stt_ws, self.tts_ws):
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass
        if self.twilio_ws:
            try:
                await self.twilio_ws.close()
            except Exception:
                pass


@dataclass
class CallSession:
    session_id: str
    language_a: str
    language_b: str
    phone_a: str
    phone_b: str
    created_at: float = field(default_factory=time.time)
    dashboard_ws: WebSocket | None = None
    legs: dict[str, Leg] = field(default_factory=dict)
    call_sids: dict[str, str] = field(default_factory=dict)
    closed: bool = False

    def other_leg(self, name: str) -> str:
        return "b" if name == "a" else "a"

    async def send_event(self, payload: dict[str, Any]) -> None:
        if not self.dashboard_ws:
            return
        try:
            await self.dashboard_ws.send_json(payload)
        except Exception:
            self.dashboard_ws = None

    async def close(self, reason: str = "ended") -> None:
        if self.closed:
            return
        self.closed = True
        for leg in self.legs.values():
            await leg.close()
        if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
            client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            for sid in self.call_sids.values():
                try:
                    await asyncio.to_thread(client.calls(sid).update, status="completed")
                except Exception:
                    pass
        await self.send_event({"type": "call_closed", "reason": reason})


SESSIONS: dict[str, CallSession] = {}


async def initiate_twilio_call(phone: str, session_id: str, leg: str) -> str:
    ensure_configured()
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    base = public_base_url()
    url = f"{base}/twilio/voice/{session_id}/{leg}"
    status = f"{base}/twilio/status/{session_id}/{leg}"
    call = await asyncio.to_thread(
        client.calls.create,
        to=phone,
        from_=TWILIO_PHONE_NUMBER,
        url=url,
        method="POST",
        status_callback=status,
        status_callback_method="POST",
        status_callback_event=["initiated", "ringing", "answered", "completed"],
        timeout=30,
    )
    return str(call.sid)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "app": "SpeakEasy WIGVO Soniox Telephony Core",
        "version": "0.1.0",
        "soniox_configured": bool(SONIOX_API_KEY),
        "twilio_configured": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE_NUMBER),
    }


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return HTML_PAGE


@app.post("/api/calls")
async def start_call(payload: StartCallRequest) -> JSONResponse:
    try:
        phone_a = validate_e164(payload.phone_a)
        phone_b = validate_e164(payload.phone_b)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if payload.language_a.lower() == payload.language_b.lower():
        raise HTTPException(status_code=422, detail="language_a and language_b must be different")
    ensure_configured()

    session_id = str(uuid4())
    session = CallSession(
        session_id=session_id,
        language_a=payload.language_a.lower(),
        language_b=payload.language_b.lower(),
        phone_a=phone_a,
        phone_b=phone_b,
    )
    session.legs = {
        "a": Leg(session, "a", session.language_a, session.language_b, SONIOX_TTS_VOICE_B),
        "b": Leg(session, "b", session.language_b, session.language_a, SONIOX_TTS_VOICE_A),
    }
    SESSIONS[session_id] = session

    try:
        sid_a, sid_b = await asyncio.gather(
            initiate_twilio_call(phone_a, session_id, "a"),
            initiate_twilio_call(phone_b, session_id, "b"),
        )
        session.call_sids = {"a": sid_a, "b": sid_b}
    except Exception:
        SESSIONS.pop(session_id, None)
        raise

    return JSONResponse({
        "ok": True,
        "session_id": session_id,
        "call_sids": session.call_sids,
        "languages": {"a": session.language_a, "b": session.language_b},
    })


@app.get("/api/calls/{session_id}")
async def call_status(session_id: str) -> JSONResponse:
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Call session not found")
    return JSONResponse({
        "session_id": session_id,
        "created_at": session.created_at,
        "closed": session.closed,
        "legs": {
            k: {
                "language": v.language,
                "target_language": v.target_language,
                "call_sid": v.call_sid,
                "stream_connected": bool(v.twilio_ws and v.stream_sid),
            }
            for k, v in session.legs.items()
        },
    })


@app.api_route("/twilio/voice/{session_id}/{leg}", methods=["GET", "POST"])
async def twilio_voice(session_id: str, leg: str, request: Request) -> Response:
    session = SESSIONS.get(session_id)
    voice = VoiceResponse()
    if not session or leg not in ("a", "b"):
        voice.reject(reason="rejected")
        return Response(str(voice), media_type="application/xml")

    if TWILIO_VALIDATE_SIGNATURE:
        await validate_twilio_request(request)

    base = public_base_url()
    stream_url = base.replace("https://", "wss://").replace("http://", "ws://") + f"/twilio/stream/{session_id}/{leg}"
    connect = voice.connect()
    connect.stream(url=stream_url)
    return Response(str(voice), media_type="application/xml")


@app.post("/twilio/status/{session_id}/{leg}")
async def twilio_status(session_id: str, leg: str, request: Request) -> dict[str, str]:
    session = SESSIONS.get(session_id)
    if session and leg in session.legs:
        form = await validate_twilio_request(request)
        sid = form.get("CallSid", "")
        if sid:
            session.legs[leg].call_sid = sid
            session.call_sids[leg] = sid
        status = form.get("CallStatus", "")
        if status in {"completed", "failed", "busy", "no-answer", "canceled"}:
            await session.close(reason=f"twilio_{status}")
            SESSIONS.pop(session_id, None)
    return {"status": "ok"}


@app.websocket("/twilio/stream/{session_id}/{leg}")
async def twilio_stream(ws: WebSocket, session_id: str, leg: str) -> None:
    session = SESSIONS.get(session_id)
    if not session or leg not in ("a", "b"):
        await ws.close(code=4404)
        return
    await ws.accept()
    current = session.legs[leg]
    current.twilio_ws = ws
    await session.send_event({"type": "leg_connecting", "leg": leg})

    try:
        await current.connect_soniox()
        await current.start_stt_reader()

        async for raw in ws.iter_text():
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            event = data.get("event")
            if event == "connected":
                await session.send_event({"type": "media_connected", "leg": leg})
            elif event == "start":
                start = data.get("start") or {}
                current.stream_sid = data.get("streamSid") or ""
                current.call_sid = start.get("callSid") or current.call_sid
                session.call_sids[leg] = current.call_sid
                current.twilio_connected.set()
                await session.send_event({"type": "connected", "leg": leg})
            elif event == "media":
                media = data.get("media") or {}
                payload = media.get("payload")
                if payload:
                    await current.send_audio_to_stt(base64.b64decode(payload))
            elif event == "stop":
                break
    except WebSocketDisconnect:
        log.info("Twilio WS disconnected session=%s leg=%s", session_id, leg)
    except Exception:
        log.exception("Twilio WS error session=%s leg=%s", session_id, leg)
    finally:
        current.closed = True
        try:
            await current.close()
        except Exception:
            pass
        if not session.closed:
            # End both legs together; a translated call is meaningful only while both are connected.
            await session.close(reason=f"leg_{leg}_disconnected")
            SESSIONS.pop(session_id, None)


@app.websocket("/api/calls/{session_id}/events")
async def call_events(ws: WebSocket, session_id: str) -> None:
    session = SESSIONS.get(session_id)
    if not session:
        await ws.close(code=4404)
        return
    await ws.accept()
    session.dashboard_ws = ws
    try:
        await session.send_event({
            "type": "session",
            "session_id": session_id,
            "languages": {"a": session.language_a, "b": session.language_b},
        })
        while not session.closed:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        if session.dashboard_ws is ws:
            session.dashboard_ws = None


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SpeakEasy WIGVO</title>
<style>
body{font-family:system-ui,sans-serif;max-width:820px;margin:0 auto;padding:24px;background:#0b1020;color:#eef2ff}
.card{background:#151c31;border:1px solid #293453;border-radius:18px;padding:20px;margin:14px 0}
input,button{width:100%;box-sizing:border-box;padding:13px;border-radius:10px;border:1px solid #3b496f;background:#0e1527;color:white;margin-top:7px}
button{cursor:pointer;font-weight:700}
.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
#log{white-space:pre-wrap;max-height:320px;overflow:auto;font-size:13px}
small{color:#9eabca}
@media(max-width:650px){.row{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>SpeakEasy WIGVO</h1>
<small>Soniox real-time bidirectional telephony test</small>
<div class="card">
<div class="row">
<div><label>Person A phone</label><input id="a" placeholder="+234..." inputmode="tel"></div>
<div><label>Person B phone</label><input id="b" placeholder="+234..." inputmode="tel"></div>
</div>
<div class="row">
<div><label>Language A</label><input id="la" value="en"></div>
<div><label>Language B</label><input id="lb" value="pt"></div>
</div>
<button id="start">Start translated call</button>
</div>
<div class="card">
<strong>Status</strong>
<div id="status">Idle</div>
<div id="log"></div>
</div>
<script>
let sid="";
const statusEl=document.querySelector("#status"),log=document.querySelector("#log");
function add(x){log.textContent += x+"\n"; log.scrollTop=log.scrollHeight}
document.querySelector("#start").onclick=async()=>{
  statusEl.textContent="Starting…";
  const body={
    phone_a:document.querySelector("#a").value,
    phone_b:document.querySelector("#b").value,
    language_a:document.querySelector("#la").value,
    language_b:document.querySelector("#lb").value
  };
  const r=await fetch("/api/calls",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)});
  const j=await r.json();
  if(!r.ok){statusEl.textContent="Error"; add(JSON.stringify(j)); return}
  sid=j.session_id; statusEl.textContent="Calling both participants…"; add("Session "+sid);
  const proto=location.protocol==="https:"?"wss":"ws";
  const ws=new WebSocket(proto+"://"+location.host+"/api/calls/"+sid+"/events");
  ws.onmessage=e=>{const d=JSON.parse(e.data); add(JSON.stringify(d)); if(d.type==="connected") statusEl.textContent="Participant "+d.leg+" connected"; if(d.type==="call_closed") statusEl.textContent="Call ended"}};
};
</script>
</body>
</html>"""
