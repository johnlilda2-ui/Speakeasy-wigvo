from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any
from uuid import uuid4

import websockets
from fastapi import WebSocket, WebSocketDisconnect

SONIOX_API_KEY = os.getenv("SONIOX_API_KEY", "")
SONIOX_STT_URL = os.getenv("SONIOX_STT_URL", "wss://stt-rt.soniox.com/transcribe-websocket")
SONIOX_TTS_URL = os.getenv("SONIOX_TTS_URL", "wss://tts-rt.soniox.com/tts-websocket")
SONIOX_STT_MODEL = os.getenv("SONIOX_STT_MODEL", "stt-rt-v5")
SONIOX_TTS_MODEL = os.getenv("SONIOX_TTS_MODEL", "tts-rt-v2")
SONIOX_ENDPOINT_DELAY_MS = max(500, min(1500, int(os.getenv("SONIOX_ENDPOINT_DELAY_MS", "500"))))
SONIOX_TTS_VOICE_A = os.getenv("SONIOX_TTS_VOICE_A", "Maya")
SONIOX_TTS_VOICE_B = os.getenv("SONIOX_TTS_VOICE_B", "Adrian")

AGORA_APP_ID = os.getenv("AGORA_APP_ID", "").strip()
AGORA_SDK_VERSION = os.getenv("AGORA_SDK_VERSION", "4.24.8").strip()
AGORA_CHANNEL_PREFIX = os.getenv("AGORA_CHANNEL_PREFIX", "speakeasy-").strip() or "speakeasy-"


def resolve_languages(cfg: dict[str, Any]) -> tuple[str, str, str]:
    source = str(cfg.get("language") or "").lower().strip()
    target = str(cfg.get("target_language") or "").lower().strip()
    voice = str(cfg.get("voice") or "").strip()
    if not source or not target:
        a = str(cfg.get("language_a") or "en").lower().strip()
        b = str(cfg.get("language_b") or "pt").lower().strip()
        if str(cfg.get("direction") or "a_to_b") == "b_to_a":
            source, target = b, a
        else:
            source, target = a, b
    if not voice:
        voice = SONIOX_TTS_VOICE_A if target in {"en", "ko"} else SONIOX_TTS_VOICE_B
    return source, target, voice


async def send_tts(tts_ws: Any, state: dict[str, Any], text: str, *, end: bool = False) -> None:
    clean = text.strip()
    if (not clean and not end) or tts_ws is None:
        return

    # Feed incremental translation fragments into one persistent Soniox TTS
    # stream for this utterance. The stream is finalized only at the natural
    # utterance boundary, so Soniox can begin generating audio while the
    # speaker is still talking.
    await state["tts_queue"].put({"text": clean, "end": end})


async def tts_worker(tts_ws: Any, state: dict[str, Any]) -> None:
    active_sid: str | None = None
    active_done: asyncio.Event | None = None

    async def ensure_stream() -> bool:
        nonlocal active_sid, active_done
        if tts_ws is None:
            return False

        if active_sid and active_done and active_done.is_set():
            state["tts_done"].pop(active_sid, None)
            active_sid = None
            active_done = None

        if active_sid is not None:
            return True

        state["seq"] += 1
        active_sid = f"browser-{state['seq']}-{uuid4().hex[:8]}"
        active_done = asyncio.Event()
        state["tts_done"][active_sid] = active_done

        await tts_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_TTS_MODEL,
            "language": state["target"],
            "voice": state["voice"],
            "audio_format": "pcm_s16le",
            "sample_rate": 24000,
            "stream_id": active_sid,
        }))
        return True

    while True:
        item = await state["tts_queue"].get()
        try:
            if tts_ws is None:
                continue

            text = str(item.get("text") or "").strip()
            end = bool(item.get("end"))
            prepare = bool(item.get("prepare"))

            if not await ensure_stream():
                continue

            if prepare:
                # Prepare the per-utterance stream as soon as speech begins.
                continue

            if text:
                await tts_ws.send(json.dumps({
                    "stream_id": active_sid,
                    "text": text,
                    "text_end": end,
                }))
                if state["first_tts_text_at"] is None:
                    state["first_tts_text_at"] = time.monotonic()
                    elapsed_ms = round(
                        (state["first_tts_text_at"] - state["speech_started_at"]) * 1000
                    ) if state["speech_started_at"] is not None else None
                    try:
                        await state["owner_ws"].send_json({
                            "type": "pipeline_timing",
                            "stage": "tts_text_sent",
                            "elapsed_ms": elapsed_ms,
                        })
                    except Exception:
                        pass
            elif end:
                await tts_ws.send(json.dumps({
                    "stream_id": active_sid,
                    "text": "",
                    "text_end": True,
                }))

            if end and active_done:
                try:
                    await asyncio.wait_for(active_done.wait(), timeout=30)
                except asyncio.TimeoutError:
                    await state["owner_ws"].send_json({
                        "type": "error",
                        "stage": "tts",
                        "message": "Soniox TTS stream timed out before termination.",
                    })
                finally:
                    state["tts_done"].pop(active_sid, None)
                    active_sid = None
                    active_done = None

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                await state["owner_ws"].send_json({
                    "type": "error",
                    "stage": "tts",
                    "message": str(exc),
                })
            except Exception:
                pass
            if active_sid:
                state["tts_done"].pop(active_sid, None)
                active_sid = None
                active_done = None
        finally:
            state["tts_queue"].task_done()


async def browser_translation(ws: WebSocket) -> None:
    await ws.accept()
    if not SONIOX_API_KEY:
        await ws.send_json({
            "type": "error",
            "stage": "config",
            "message": "SONIOX_API_KEY is not configured on Render.",
        })
        await ws.close(code=1011)
        return

    stt_ws = tts_ws = None
    tasks: list[asyncio.Task] = []
    started = time.monotonic()

    try:
        cfg = await ws.receive_json()
        source, target, voice = resolve_languages(cfg)
        if source == target:
            await ws.send_json({
                "type": "error",
                "stage": "config",
                "message": "Source and target languages must be different.",
            })
            await ws.close(code=1008)
            return

        stt_ws = await websockets.connect(
            SONIOX_STT_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )
        await stt_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_STT_MODEL,
            "audio_format": "pcm_s16le",
            "sample_rate": 16000,
            "num_channels": 1,
            "language_hints": [source],
            "enable_endpoint_detection": True,
            "endpoint_latency_adjustment_level": 1,
            "endpoint_sensitivity": 0.0,
            "max_endpoint_delay_ms": SONIOX_ENDPOINT_DELAY_MS,
            "context": {
                "general": [
                    {"key": "domain", "value": "live conversation"},
                ],
            },
            "translation": {
                "type": "one_way",
                "target_language": target,
            },
        }))

        tts_ws = await websockets.connect(
            SONIOX_TTS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )

        state: dict[str, Any] = {
            "seq": 0,
            "translation_seq": 0,
            "target": target,
            "voice": voice,
            "final_translation": "",
            "translation_partial": "",
            "translation_prev_hypothesis": "",
            "translation_tts_sent": 0,
            "spoken_chars": 0,
            "tts_queue": asyncio.Queue(maxsize=32),
            "tts_done": {},
            "owner_ws": ws,
            "speech_started_at": None,
            "first_translation_token_at": None,
            "first_tts_text_at": None,
            "first_audio_at": None,
        }
        final_original = ""
        speech_started = False
        last_original_text = ""

        await ws.send_json({
            "type": "ready",
            "source_language": source,
            "target_language": target,
        })

        def longest_common_prefix(a: str, b: str) -> int:
            limit = min(len(a), len(b))
            i = 0
            while i < limit and a[i] == b[i]:
                i += 1
            return i

        def queue_stable_translation(force: bool = False) -> str:
            # Release the earliest stable word/phrase instead of waiting for a
            # whole sentence. The first provisional hypothesis has no previous
            # hypothesis to compare against, so release its first completed
            # word or punctuation boundary immediately. Later hypotheses use
            # the longest unchanged prefix to avoid replaying revised text.
            candidate = (
                state["final_translation"] + state["translation_partial"]
            )
            sent = int(state["translation_tts_sent"])
            if len(candidate) <= sent:
                return ""

            if force:
                stable_end = len(candidate)
            else:
                final_len = len(state["final_translation"])
                previous = state["translation_prev_hypothesis"]
                if previous:
                    common = longest_common_prefix(previous, candidate)
                    stable_end = max(final_len, common)
                else:
                    # No prior hypothesis yet: release only the first
                    # completed word/punctuation boundary, not the whole
                    # provisional sentence.
                    first_boundary = -1
                    for marker in (" ", ",", ".", "?", "!", ":", ";"):
                        pos = candidate.find(marker, sent)
                        if pos >= 0 and (first_boundary < 0 or pos < first_boundary):
                            first_boundary = pos
                    stable_end = first_boundary + 1 if first_boundary >= 0 else 0

            if stable_end <= sent:
                return ""

            delta = candidate[sent:stable_end]
            if force:
                chunk = delta.strip()
                cut = len(delta)
            else:
                # Prefer the earliest completed boundary so TTS can start
                # while the speaker is still talking. A short first word such
                # as "Olá," is valid and useful for starting the stream.
                boundary = -1
                for i, ch in enumerate(delta):
                    if ch in " ,.!?:;":
                        boundary = i
                        break
                if boundary < 0:
                    return ""
                cut = boundary + 1
                chunk = delta[:cut].strip()
                if len(chunk) < 2:
                    return ""

            if not chunk:
                return ""

            state["translation_tts_sent"] = sent + cut
            state["translation_seq"] += 1
            try:
                state["tts_queue"].put_nowait({"text": chunk, "end": False})
            except asyncio.QueueFull:
                # Keep TTS bounded; if the queue is saturated, don't advance
                # further until the worker catches up.
                state["translation_tts_sent"] = sent
                state["translation_seq"] -= 1
                return ""
            return chunk

        async def read_stt() -> None:
            nonlocal final_original, speech_started
            try:
                async for raw in stt_ws:
                    data = json.loads(raw)
                    if data.get("error_code") is not None:
                        await ws.send_json({
                            "type": "error",
                            "stage": "stt",
                            "message": data.get("error_message", "Soniox STT error"),
                        })
                        continue

                    preview_original: list[str] = []
                    preview_translation: list[str] = []
                    endpoint = False
                    tokens = data.get("tokens") or []

                    previous_translation_hypothesis = (
                        state["final_translation"] + state["translation_partial"]
                    )

                    for token in tokens:
                        token_text = str(token.get("text") or "")
                        status = token.get("translation_status")
                        is_final = bool(token.get("is_final"))

                        if token_text == "<end>":
                            endpoint = True
                            continue
                        if not token_text:
                            continue

                        if status in (None, "none", "original"):
                            if is_final:
                                final_original += token_text
                            else:
                                preview_original.append(token_text)
                                if not speech_started:
                                    speech_started = True
                                    state["speech_started_at"] = time.monotonic()
                                    try:
                                        state["tts_queue"].put_nowait({"prepare": True, "text": "", "end": False})
                                    except asyncio.QueueFull:
                                        pass
                                    await ws.send_json({"type": "speech_start"})
                        elif status == "translation":
                            if state["first_translation_token_at"] is None:
                                state["first_translation_token_at"] = time.monotonic()
                                elapsed_ms = round(
                                    (state["first_translation_token_at"] - state["speech_started_at"]) * 1000
                                ) if state["speech_started_at"] is not None else None
                                try:
                                    await ws.send_json({
                                        "type": "pipeline_timing",
                                        "stage": "translation_token",
                                        "elapsed_ms": elapsed_ms,
                                    })
                                except Exception:
                                    pass
                            if is_final:
                                state["final_translation"] += token_text
                            else:
                                preview_translation.append(token_text)

                    state["translation_partial"] = "".join(preview_translation)
                    state["translation_prev_hypothesis"] = previous_translation_hypothesis

                    # Final translation tokens are append-only. Release their
                    # newly confirmed text immediately at a natural word
                    # boundary instead of waiting for endpoint detection.
                    final_sent = int(state["translation_tts_sent"])
                    final_text = state["final_translation"]
                    if len(final_text) > final_sent:
                        final_delta = final_text[final_sent:]
                        boundary = -1
                        for i, ch in enumerate(final_delta):
                            if ch in " ,.!?:;":
                                boundary = i
                                break
                        if boundary >= 0:
                            cut = boundary + 1
                            chunk = final_delta[:cut].strip()
                            if len(chunk) >= 2:
                                try:
                                    state["tts_queue"].put_nowait({"text": chunk, "end": False})
                                    state["translation_tts_sent"] = final_sent + cut
                                    state["translation_seq"] += 1
                                    await ws.send_json({
                                        "type": "translation_text",
                                        "text": chunk,
                                        "final": False,
                                        "chunk_id": f"tr-{state['translation_seq']}",
                                    })
                                except asyncio.QueueFull:
                                    pass

                    current_original = (
                        final_original + "".join(preview_original)
                    ).strip()

                    # Only publish a raw STT diagnostic when Soniox actually
                    # supplied useful recognition data. Metadata/empty frames
                    # must never overwrite the last real transcript.
                    if tokens or current_original or endpoint:
                        if current_original:
                            last_original_text = current_original
                        await ws.send_json({
                            "type": "raw_stt",
                            "final_text": final_original.strip(),
                            "partial_text": "".join(preview_original).strip(),
                            "text": current_original or last_original_text,
                            "source_language": source,
                            "token_count": len(tokens),
                            "endpoint": endpoint,
                        })

                    if current_original:
                        await ws.send_json({
                            "type": "transcript",
                            "text": current_original,
                            "final": False,
                        })

                    if preview_translation:
                        await ws.send_json({
                            "type": "translation_preview",
                            "text": "".join(preview_translation),
                        })

                    chunk = queue_stable_translation(endpoint)
                    if chunk:
                        await ws.send_json({
                            "type": "translation_text",
                            "text": chunk,
                            "final": endpoint,
                            "chunk_id": f"tr-{state['translation_seq']}",
                        })

                    if endpoint:
                        # Close the current TTS stream only at the utterance
                        # boundary. Earlier stable translation chunks stayed on
                        # the same stream and could already produce audio.
                        try:
                            state["tts_queue"].put_nowait({"text": "", "end": True})
                        except asyncio.QueueFull:
                            pass

                        await ws.send_json({
                            "type": "transcript",
                            "text": final_original.strip(),
                            "final": True,
                        })
                        await ws.send_json({"type": "utterance_end"})

                        final_original = ""
                        speech_started = False
                        state["final_translation"] = ""
                        state["translation_partial"] = ""
                        state["translation_prev_hypothesis"] = ""
                        state["translation_tts_sent"] = 0
                        state["spoken_chars"] = 0
                        state["speech_started_at"] = None
                        state["first_translation_token_at"] = None
                        state["first_tts_text_at"] = None
                        state["first_audio_at"] = None

                    if data.get("finished"):
                        break
            except asyncio.CancelledError:
                pass

        async def read_tts() -> None:
            try:
                async for raw in tts_ws:
                    data = json.loads(raw)
                    sid = data.get("stream_id")
                    if data.get("error_code") is not None:
                        await ws.send_json({
                            "type": "error",
                            "stage": "tts",
                            "message": data.get("error_message", "Soniox TTS error"),
                            "stream_id": sid,
                        })
                        done = state["tts_done"].get(sid)
                        if done:
                            done.set()
                        continue

                    audio = data.get("audio")
                    if audio:
                        if state["first_audio_at"] is None:
                            state["first_audio_at"] = time.monotonic()
                            elapsed_ms = round(
                                (state["first_audio_at"] - state["speech_started_at"]) * 1000
                            ) if state["speech_started_at"] is not None else None
                            try:
                                await ws.send_json({
                                    "type": "pipeline_timing",
                                    "stage": "audio_server",
                                    "elapsed_ms": elapsed_ms,
                                })
                            except Exception:
                                pass
                        await ws.send_json({
                            "type": "audio",
                            "audio": audio,
                            "sample_rate": 24000,
                            "stream_id": sid,
                        })

                    if data.get("terminated"):
                        done = state["tts_done"].get(sid)
                        if done:
                            done.set()
            except asyncio.CancelledError:
                pass

        tasks.extend([
            asyncio.create_task(read_stt()),
            asyncio.create_task(read_tts()),
            asyncio.create_task(tts_worker(tts_ws, state)),
        ])

        async def keepalive() -> None:
            try:
                while True:
                    await asyncio.sleep(15)
                    if stt_ws:
                        await stt_ws.send(json.dumps({"type": "keepalive"}))
                    if tts_ws:
                        await tts_ws.send(json.dumps({"keep_alive": True}))
            except (asyncio.CancelledError, Exception):
                pass

        tasks.append(asyncio.create_task(keepalive()))

        while True:
            if time.monotonic() - started > 600:
                await ws.send_json({
                    "type": "info",
                    "message": "Maximum browser call session reached.",
                })
                break

            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break

            if msg.get("bytes") is not None:
                audio = msg["bytes"]
                if audio and stt_ws:
                    await stt_ws.send(audio)
            elif msg.get("text") is not None:
                try:
                    cmd = json.loads(msg["text"])
                except json.JSONDecodeError:
                    cmd = {}

                if cmd.get("type") == "stop":
                    break
                if cmd.get("type") == "barge_in":
                    # Do not chop already translated audio. Browser/Agora queues it.
                    await ws.send_json({"type": "keep_audio"})

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await ws.send_json({
                "type": "error",
                "stage": "server",
                "message": str(exc),
            })
        except Exception:
            pass
    finally:
        for task in tasks:
            task.cancel()
        for sock in (stt_ws, tts_ws):
            if sock:
                try:
                    await sock.close()
                except Exception:
                    pass
        try:
            await ws.close()
        except Exception:
            pass


BROWSER_HTML_PAGE = r'''<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>SpeakEasy — Realtime Call</title>
<meta http-equiv="Cache-Control" content="no-store">
<style>
*{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at top,#1b2d52,#080d18 55%);color:#eef2ff;font-family:system-ui,sans-serif;padding:12px}
main{width:min(720px,100%);margin:auto}.card{background:rgba(16,25,45,.9);border:1px solid rgba(126,153,207,.25);border-radius:20px;padding:16px;margin:10px 0;box-shadow:0 14px 38px rgba(0,0,0,.2)}
.brand{display:flex;align-items:center;gap:10px}.logo{width:44px;height:44px;border-radius:13px;background:#eef2ff;color:#0b1324;display:grid;place-items:center;font-weight:900}.title{margin:0;font-size:23px}.muted{color:#9daaca;font-size:13px}.sub{color:#b9c4dc;font-size:13px}.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}.field{margin-top:10px}.field label{display:block;font-size:12px;color:#aeb9d2;margin-bottom:5px}.field input,.field select,button{width:100%;padding:12px;border-radius:11px;border:1px solid #40527a;background:#0d1527;color:#fff;font-size:15px}button{font-weight:750}.primary{background:#315fe9;border-color:#315fe9}.danger{background:#6b2940;border-color:#6b2940}button:disabled{opacity:.45}.status{margin-top:12px;padding:10px;border-radius:11px;background:#0b1324}.orb{width:128px;height:128px;margin:10px auto;border-radius:50%;background:radial-gradient(circle at 35% 30%,#425b89,#152544 52%,#0a1120);box-shadow:0 18px 45px rgba(0,0,0,.35)}.orb.speaking{animation:pulse 1.15s infinite}.call{text-align:center}.direction{font-size:18px;font-weight:800}.room{font-size:12px;color:#9daaca;margin-top:4px}.meter{height:6px;background:#0c1425;border-radius:20px;overflow:hidden;margin-top:14px}.meter i{display:block;height:100%;width:0;background:#6f8fff}.box{background:#0b1324;border-radius:12px;padding:12px;min-height:56px;margin-top:7px;line-height:1.5}.translated{font-size:18px;font-weight:650}.controls{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.log{font:11px ui-monospace,monospace;color:#8f9dbb;max-height:130px;overflow:auto;white-space:pre-wrap}.note{font-size:11px;color:#93a2bd;line-height:1.4;margin-top:9px}.error{color:#ff9eaf}@keyframes pulse{50%{transform:scale(1.04)}}@media(max-width:600px){.row{grid-template-columns:1fr}.card{padding:14px}}
</style></head>
<body><main>
<div class="card"><div class="brand"><div class="logo">S</div><div><h1 class="title">SpeakEasy</h1><div class="sub">Realtime translated call</div></div><span class="muted" style="margin-left:auto">Agora + Soniox</span></div></div>
<div class="card">
<div class="row"><div class="field"><label>Room code</label><input id="room" value="demo-room" autocomplete="off"></div>
<div class="field"><label>My language</label><select id="src"><option value="en">English</option><option value="pt">Português</option><option value="es">Español</option><option value="fr">Français</option><option value="ko">한국어</option></select></div></div>
<div class="field"><label>Translate to</label><select id="tgt"><option value="pt">Português</option><option value="en">English</option><option value="es">Español</option><option value="fr">Français</option><option value="ko">한국어</option></select></div>
<div class="note">Both people use the same room code. Your microphone goes to Soniox; only translated audio is published to Agora.</div>
<div class="controls"><button id="start" class="primary">Start call</button><button id="stop" class="danger" disabled>End call</button></div>
<div class="status"><span id="status">Ready</span><span id="uid" class="muted" style="float:right"></span></div>
</div>
<div class="card call"><div id="orb" class="orb"></div><div id="dir" class="direction">Not connected</div><div id="roomline" class="room">Choose languages and start</div><div class="meter"><i id="meter"></i></div></div>
<div class="card"><div class="muted">What I am saying</div><div id="original" class="box">—</div><div class="muted" style="margin-top:12px">Translated speech</div><div id="translated" class="box translated">—</div></div>
<div class="card"><div class="muted">Raw Soniox STT diagnostic</div><div id="rawStt" class="box">—</div><div id="sttMeta" class="note">Waiting for microphone audio…</div></div>
<div class="card"><div class="muted">Session log</div><div id="log" class="log"></div></div>
</main>
<script>
let AgoraRTC=null,client=null,localTrack=null,stream=null,playCtx=null,micCtx=null,micSource=null,processor=null,silent=null,monitorGain=null,outDestination=null,ws=null,running=false,playAt=0,staged=[],config=null,micPackets=0,micBytes=0,speechAt=0,firstSttAt=0,translationAt=0,audioAt=0,lastRawSttText="",timingTranslationShown=false,timingAudioShown=false,pipelineTiming={},diagnosticSourceLanguage="",diagnosticTokenCount=0;
const $=id=>document.getElementById(id);
const write=x=>{$("log").textContent+=String(x)+"\n";$("log").scrollTop=$("log").scrollHeight};
const setStatus=(x,err=false)=>{$("status").textContent=x;$("status").className=err?"error":""};
function renderTiming(){const lagParts=[];if(speechAt&&firstSttAt)lagParts.push("first STT token: "+Math.round(firstSttAt-speechAt)+" ms");lagParts.push("mic packets: "+micPackets+" ("+Math.round(micBytes/1024)+" KB)");if(diagnosticSourceLanguage)lagParts.push("language: "+diagnosticSourceLanguage);if(diagnosticTokenCount>0)lagParts.push("last token frame: "+diagnosticTokenCount);if(pipelineTiming.translation_token!=null)lagParts.push("translation token: "+pipelineTiming.translation_token+" ms");if(pipelineTiming.tts_text_sent!=null)lagParts.push("TTS text sent: "+pipelineTiming.tts_text_sent+" ms");if(pipelineTiming.audio_server!=null)lagParts.push("server audio: "+pipelineTiming.audio_server+" ms");if(speechAt&&translationAt)lagParts.push("first translation: "+Math.round(translationAt-speechAt)+" ms");if(speechAt&&audioAt)lagParts.push("first audio: "+Math.round(audioAt-speechAt)+" ms");$("sttMeta").textContent=lagParts.join(" · ")}
const roomName=v=>{const x=String(v||"").toLowerCase().replace(/[^a-z0-9_-]/g,"").slice(0,40);return x||"demo-room"};
const channel=v=>(config.agora_channel_prefix||"speakeasy-")+roomName(v);
function down(a,from,to){if(from===to)return a;const r=from/to,n=Math.max(1,Math.round(a.length/r)),o=new Float32Array(n);let p=0;for(let i=0;i<n;i++){const q=Math.min(a.length,Math.round((i+1)*r));let s=0,c=0;for(let j=p;j<q;j++){s+=a[j];c++}o[i]=c?s/c:0;p=q}return o}
function pcm(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const s=Math.max(-1,Math.min(1,a[i]));o[i]=s<0?s*32768:s*32767}return o}
function clearQueue(){staged.forEach(n=>{try{n.stop()}catch(e){}});staged=[];if(playCtx)playAt=playCtx.currentTime+.02}
function playTranslated(b64,rate){if(!playCtx||!outDestination)return;const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));const s=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));const b=playCtx.createBuffer(1,s.length,rate);const ch=b.getChannelData(0);for(let i=0;i<s.length;i++)ch[i]=s[i]/32768;const n=playCtx.createBufferSource();n.buffer=b;n.connect(outDestination);n.connect(monitorGain);playAt=Math.max(playAt,playCtx.currentTime+.01);n.start(playAt);playAt+=b.duration;staged.push(n);n.onended=()=>staged=staged.filter(x=>x!==n);$("orb").classList.add("speaking")}
async function loadAgora(){if(AgoraRTC)return;await new Promise((ok,bad)=>{const s=document.createElement("script");s.src="https://download.agora.io/sdk/release/AgoraRTC_N-"+encodeURIComponent(config.agora_sdk_version)+".js";s.onload=ok;s.onerror=()=>bad(new Error("Could not load Agora Web SDK"));document.head.appendChild(s)});AgoraRTC=window.AgoraRTC}
async function stopCall(){running=false;if(ws)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}if(ws)try{ws.close()}catch(e){}ws=null;if(processor)try{processor.disconnect()}catch(e){}if(micSource)try{micSource.disconnect()}catch(e){}if(silent)try{silent.disconnect()}catch(e){}processor=micSource=silent=null;clearQueue();if(localTrack){try{await client?.unpublish([localTrack])}catch(e){}try{localTrack.close()}catch(e){}localTrack=null}if(client){try{await client.leave()}catch(e){}client=null}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(micCtx){try{await micCtx.close()}catch(e){}micCtx=null}if(playCtx){try{await playCtx.close()}catch(e){}playCtx=null}monitorGain=null;outDestination=null;$("start").disabled=false;$("stop").disabled=true;$("dir").textContent="Not connected";$("roomline").textContent="Choose languages and start";$("orb").classList.remove("speaking");setStatus("Ready")}
async function startCall(){if(running)return;try{const src=$("src").value,tgt=$("tgt").value;if(src===tgt){setStatus("Choose different languages",true);return}if(!config.agora_app_id)throw new Error("AGORA_APP_ID is not configured on Render");await loadAgora();stream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true}});playCtx=new(window.AudioContext||window.webkitAudioContext)({sampleRate:24000});await playCtx.resume();monitorGain=playCtx.createGain();monitorGain.gain.value=1;monitorGain.connect(playCtx.destination);outDestination=playCtx.createMediaStreamDestination();const translatedTrack=outDestination.stream.getAudioTracks()[0];micCtx=new(window.AudioContext||window.webkitAudioContext)({sampleRate:16000});await micCtx.resume();if(!micCtx.audioWorklet)throw new Error("AudioWorklet is not supported by this browser");const workletCode=`class SpeakEasyPCM extends AudioWorkletProcessor{constructor(){super();this.buf=[];this.need=0;this.inputRate=sampleRate;this.outRate=16000;this.outFrames=320}process(inputs,outputs){const input=inputs[0]?.[0],output=outputs[0]?.[0];if(output)output.fill(0);if(!input)return true;for(let i=0;i<input.length;i++)this.buf.push(input[i]);const inNeeded=Math.round(this.outFrames*this.inputRate/this.outRate);while(this.buf.length>=inNeeded){const pcm=new Int16Array(this.outFrames);let peak=0;for(let i=0;i<this.outFrames;i++){const pos=i*(inNeeded-1)/(this.outFrames-1);const a=this.buf[Math.floor(pos)]||0;const b=this.buf[Math.min(inNeeded-1,Math.floor(pos)+1)]||a;const s=a+(b-a)*(pos-Math.floor(pos));const v=Math.max(-1,Math.min(1,s));peak=Math.max(peak,Math.abs(v));pcm[i]=v<0?v*32768:v*32767}this.buf.splice(0,inNeeded);this.port.postMessage({pcm:pcm.buffer,peak,inputRate:this.inputRate},{transfer:[pcm.buffer]})}return true}}registerProcessor("speakeasy-pcm",SpeakEasyPCM);`;const blob=new Blob([workletCode],{type:"application/javascript"});const workletUrl=URL.createObjectURL(blob);try{await micCtx.audioWorklet.addModule(workletUrl)}finally{URL.revokeObjectURL(workletUrl)};client=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});client.on("user-published",async(user,type)=>{if(type!=="audio")return;try{await client.subscribe(user,"audio");user.audioTrack?.play()}catch(e){write("Remote audio error: "+(e?.message||e))}});client.on("user-unpublished",(u,type)=>{if(type==="audio")write("Remote translated audio stopped")});const uid=Math.floor(100000+Math.random()*900000);const channelName=channel($("room").value);const tokenResponse=await fetch("/api/agora-token?channel="+encodeURIComponent(channelName)+"&uid="+uid,{cache:"no-store"});let tokenJson={};try{tokenJson=await tokenResponse.json()}catch(e){}if(!tokenResponse.ok||!tokenJson.token)throw new Error(tokenJson.detail||"Could not obtain Agora RTC token");await client.join(config.agora_app_id,channelName,tokenJson.token,uid);localTrack=AgoraRTC.createCustomAudioTrack({mediaStreamTrack:translatedTrack});await client.publish([localTrack]);$("uid").textContent="UID "+uid;$("dir").textContent=src.toUpperCase()+" → "+tgt.toUpperCase();$("roomline").textContent="Room: "+roomName($("room").value);setStatus("Connected — listening");write("Agora joined "+channelName+" with a short-lived RTC token");ws=new WebSocket((location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/api/browser");ws.onopen=()=>{ws.send(JSON.stringify({type:"start",language:src,target_language:tgt}));micPackets=0;micBytes=0;speechAt=firstSttAt=translationAt=audioAt=0;lastRawSttText="";timingTranslationShown=false;timingAudioShown=false;pipelineTiming={};diagnosticSourceLanguage="";diagnosticTokenCount=0;micSource=micCtx.createMediaStreamSource(stream);processor=new AudioWorkletNode(micCtx,"speakeasy-pcm",{numberOfInputs:1,numberOfOutputs:1,outputChannelCount:[1]});processor.port.onmessage=e=>{if(!running||!ws||ws.readyState!==1)return;const p=e.data?.pcm;if(p){micPackets++;micBytes+=p.byteLength||0;if(ws.bufferedAmount<262144)ws.send(p);$("meter").style.width=Math.min(100,(Number(e.data?.peak)||0)*170)+"%"}};micSource.connect(processor);silent=micCtx.createGain();silent.gain.value=0;processor.connect(silent);silent.connect(micCtx.destination);running=true;$("start").disabled=true;$("stop").disabled=false};ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==="ready")setStatus("Call connected — listening");else if(d.type==="speech_start"){speechAt=performance.now();$("orb").classList.add("speaking");setStatus("Speaking…")}else if(d.type==="raw_stt"){const t=String(d.text||"").trim();if(t){lastRawSttText=t;$("rawStt").textContent=t;if(!firstSttAt)firstSttAt=performance.now();}if(d.source_language)diagnosticSourceLanguage=String(d.source_language);if(d.token_count!=null&&Number(d.token_count)>0)diagnosticTokenCount=Number(d.token_count);renderTiming()}else if(d.type==="pipeline_timing"){pipelineTiming[d.stage]=d.elapsed_ms;renderTiming()}else if(d.type==="transcript"){const t=String(d.text||"").trim();if(t)$("original").textContent=t}else if(d.type==="translation_text"){if(!translationAt)translationAt=performance.now();const t=String(d.text||"").trim();if(t)$("translated").textContent=(($("translated").textContent==="—"?"":$("translated").textContent+" ")+t).trim();if(speechAt&&!timingTranslationShown)timingTranslationShown=true;renderTiming()}else if(d.type==="audio"){if(!audioAt)audioAt=performance.now();playTranslated(d.audio,d.sample_rate||24000);if(speechAt&&!timingAudioShown)timingAudioShown=true;renderTiming()}else if(d.type==="utterance_end"){$("orb").classList.remove("speaking");setStatus("Call connected — listening")}else if(d.type==="error"){write("ERROR ["+d.stage+"] "+d.message);setStatus("Error — see log",true)}else if(d.type==="info"){write(d.message)}};ws.onerror=()=>{write("Translation WebSocket error");setStatus("Translation connection error",true)};ws.onclose=()=>{if(running)stopCall()}}catch(e){write(e?.message||String(e));setStatus(e?.message||"Could not start call",true);await stopCall()}}
async function boot(){try{const r=await fetch("/api/config",{cache:"no-store"});config=await r.json();if(!config.agora_app_id)write("AGORA_APP_ID is not configured. Add it to the existing speakeasy-wigvo Render service.");else write("Agora SDK "+config.agora_sdk_version+" configured")}catch(e){write("Config error: "+(e?.message||e))}}
$("start").onclick=startCall;$("stop").onclick=stopCall;window.addEventListener("beforeunload",()=>{if(ws&&ws.readyState===1)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}});boot();
</script></body></html>'''


def register_browser_route(app: Any) -> None:
    app.websocket("/api/browser")(browser_translation)


def register_browser_home(app: Any) -> None:
    from fastapi.responses import HTMLResponse, JSONResponse

    headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    @app.get("/", response_class=HTMLResponse)
    async def browser_root() -> HTMLResponse:
        return HTMLResponse(BROWSER_HTML_PAGE, headers=headers)

    @app.get("/browser", response_class=HTMLResponse)
    async def browser_home() -> HTMLResponse:
        return HTMLResponse(BROWSER_HTML_PAGE, headers=headers)

    @app.get("/api/config", response_class=JSONResponse)
    async def browser_config() -> JSONResponse:
        return JSONResponse({
            "transport": "agora_soniox",
            "agora_app_id": AGORA_APP_ID,
            "agora_sdk_version": AGORA_SDK_VERSION,
            "agora_channel_prefix": AGORA_CHANNEL_PREFIX,
            "soniox_configured": bool(SONIOX_API_KEY),
        }, headers=headers)
