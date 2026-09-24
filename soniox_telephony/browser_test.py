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
SONIOX_ENDPOINT_DELAY_MS = max(300, min(1500, int(os.getenv("SONIOX_ENDPOINT_DELAY_MS", "400"))))
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
    # Soniox supports multiple concurrent streams on one TTS websocket.
    # Keep each utterance on its own stream so a finished turn never blocks
    # the next turn while its final audio/termination is still arriving.
    async def ensure_stream(utterance_id: int) -> str | None:
        if tts_ws is None:
            return None

        for sid, info in list(state["tts_streams"].items()):
            if info["utterance_id"] == utterance_id:
                done = info["done"]
                if not done.is_set():
                    return sid
                state["tts_streams"].pop(sid, None)
                state["tts_done"].pop(sid, None)

        state["seq"] += 1
        sid = f"browser-{state['seq']}-{uuid4().hex[:8]}"
        done = asyncio.Event()
        timing = state["timings"].get(utterance_id, {})
        state["tts_streams"][sid] = {
            "done": done,
            "utterance_id": utterance_id,
            "speech_started_at": timing.get("speech_started_at"),
        }
        state["tts_done"][sid] = done

        await tts_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_TTS_MODEL,
            "language": state["target"],
            "voice": state["voice"],
            "audio_format": "pcm_s16le",
            "sample_rate": 24000,
            "stream_id": sid,
        }))
        return sid

    while True:
        item = await state["tts_queue"].get()
        try:
            if tts_ws is None:
                continue

            text = str(item.get("text") or "").strip()
            end = bool(item.get("end"))
            prepare = bool(item.get("prepare"))
            utterance_id = int(
                item.get("utterance_id") or state.get("utterance_id") or 0
            )

            sid = await ensure_stream(utterance_id)
            if sid is None:
                continue

            if prepare:
                # The stream is already open before the first translated
                # chunk arrives, reducing TTS startup latency.
                continue

            if text:
                await tts_ws.send(json.dumps({
                    "stream_id": sid,
                    "text": text,
                    "text_end": end,
                }))
                timing = state["timings"].get(utterance_id)
                if timing is not None and timing["first_tts_text_at"] is None:
                    timing["first_tts_text_at"] = time.monotonic()
                    elapsed_ms = round(
                        (timing["first_tts_text_at"] - timing["speech_started_at"]) * 1000
                    )
                    try:
                        await state["owner_ws"].send_json({
                            "type": "pipeline_timing",
                            "stage": "tts_text_sent",
                            "elapsed_ms": elapsed_ms,
                            "utterance_id": utterance_id,
                        })
                    except Exception:
                        pass
            elif end:
                await tts_ws.send(json.dumps({
                    "stream_id": sid,
                    "text": "",
                    "text_end": True,
                }))
                # Do not wait here. The translated audio already queued by
                # the browser must continue playing, while the next speech
                # turn is allowed to open its own Soniox stream immediately.

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
        tts_only = str(cfg.get("mode") or "").strip().lower() == "tts_only"

        if source == target and not tts_only:
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
            "endpoint_latency_adjustment_level": 2,
            "endpoint_sensitivity": 0.0,
            "max_endpoint_delay_ms": SONIOX_ENDPOINT_DELAY_MS,
            "context": {
                "general": [
                    {"key": "domain", "value": "live conversation"},
                ],
            },
            **({"translation": {
                "type": "one_way",
                "target_language": target,
            }} if not tts_only else {}),
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
            "tts_only": tts_only,
            "final_translation": "",
            "translation_partial": "",
            "translation_prev_hypothesis": "",
            "translation_tts_sent": 0,
            "tts_only_sent": 0,
            "spoken_chars": 0,
            "tts_queue": asyncio.Queue(maxsize=32),
            "tts_done": {},
            "tts_streams": {},
            "utterance_id": 0,
            "owner_ws": ws,
            "speech_started_at": None,
            "first_translation_token_at": None,
            "first_tts_text_at": None,
            "first_audio_at": None,
            "timings": {},
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
                state["tts_queue"].put_nowait({
                    "text": chunk,
                    "end": False,
                    "utterance_id": state["utterance_id"],
                })
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
                            # Treat the first recognized original token as the
                            # start of a new speech turn regardless of whether
                            # Soniox marks that token final or partial. This
                            # keeps speech capture independent from TTS playout.
                            if not speech_started:
                                speech_started = True
                                state["utterance_id"] += 1
                                speech_started_at = time.monotonic()
                                state["speech_started_at"] = speech_started_at
                                state["timings"][state["utterance_id"]] = {
                                    "speech_started_at": speech_started_at,
                                    "first_translation_token_at": None,
                                    "first_tts_text_at": None,
                                    "first_audio_at": None,
                                }
                                if len(state["timings"]) > 16:
                                    for old_utt in sorted(state["timings"])[:-16]:
                                        state["timings"].pop(old_utt, None)
                                try:
                                    state["tts_queue"].put_nowait({
                                        "prepare": True,
                                        "text": "",
                                        "end": False,
                                        "utterance_id": state["utterance_id"],
                                    })
                                except asyncio.QueueFull:
                                    pass
                                await ws.send_json({
                                    "type": "speech_start",
                                    "utterance_id": state["utterance_id"],
                                })

                            if is_final:
                                final_original += token_text
                            else:
                                preview_original.append(token_text)

                            if state.get("tts_only"):
                                tts_candidate = final_original + "".join(preview_original)
                                sent = int(state.get("tts_only_sent", 0))
                                if len(tts_candidate) > sent:
                                    delta = tts_candidate[sent:]
                                    boundary = -1
                                    for i, ch in enumerate(delta):
                                        if ch in " ,.!?:;":
                                            boundary = i
                                            break
                                    if boundary >= 0:
                                        chunk = delta[:boundary + 1].strip()
                                        if len(chunk) >= 2:
                                            try:
                                                state["tts_queue"].put_nowait({
                                                    "text": chunk,
                                                    "end": False,
                                                    "utterance_id": state["utterance_id"],
                                                })
                                                state["tts_only_sent"] = sent + boundary + 1
                                                state["translation_seq"] += 1
                                                await ws.send_json({
                                                    "type": "tts_text",
                                                    "text": chunk,
                                                    "final": False,
                                                    "chunk_id": f"tts-{state['translation_seq']}",
                                                })
                                            except asyncio.QueueFull:
                                                pass
                        elif status == "translation":
                            timing = state["timings"].get(state["utterance_id"])
                            if timing is not None and timing["first_translation_token_at"] is None:
                                timing["first_translation_token_at"] = time.monotonic()
                                elapsed_ms = round(
                                    (timing["first_translation_token_at"] - timing["speech_started_at"]) * 1000
                                )
                                try:
                                    await ws.send_json({
                                        "type": "pipeline_timing",
                                        "stage": "translation_token",
                                        "elapsed_ms": elapsed_ms,
                                        "utterance_id": state["utterance_id"],
                                        "translation_token_text": token_text,
                                        "translation_token_final": is_final,
                                        "translation_token_status": status,
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
                                    state["tts_queue"].put_nowait({
                                        "text": chunk,
                                        "end": False,
                                        "utterance_id": state["utterance_id"],
                                    })
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
                        # Finish only the TTS stream belonging to this speech
                        # turn. The worker does not wait for termination, so a
                        # new turn can be captured and translated immediately.
                        ending_utterance_id = state["utterance_id"]
                        completed_original = (
                            final_original + "".join(preview_original)
                        ).strip()
                        if state.get("tts_only"):
                            # Flush any final characters that did not contain a
                            # word boundary while the speaker was talking.
                            # Short utterances such as "Hello" must still reach
                            # TTS before the stream is ended.
                            tts_candidate = completed_original
                            sent = int(state.get("tts_only_sent", 0))
                            remaining = tts_candidate[sent:].strip()
                            if remaining:
                                try:
                                    state["tts_queue"].put_nowait({
                                        "text": remaining,
                                        "end": True,
                                        "utterance_id": ending_utterance_id,
                                    })
                                    state["tts_only_sent"] = len(tts_candidate)
                                    state["translation_seq"] += 1
                                    await ws.send_json({
                                        "type": "tts_text",
                                        "text": remaining,
                                        "final": True,
                                        "chunk_id": f"tts-{state['translation_seq']}",
                                    })
                                except asyncio.QueueFull:
                                    pass
                            else:
                                try:
                                    state["tts_queue"].put_nowait({
                                        "text": "",
                                        "end": True,
                                        "utterance_id": ending_utterance_id,
                                    })
                                except asyncio.QueueFull:
                                    pass
                        else:
                            try:
                                state["tts_queue"].put_nowait({
                                    "text": "",
                                    "end": True,
                                    "utterance_id": ending_utterance_id,
                                })
                            except asyncio.QueueFull:
                                pass

                        await ws.send_json({
                            "type": "transcript",
                            "text": completed_original,
                            "final": True,
                            "utterance_id": ending_utterance_id,
                        })
                        await ws.send_json({
                            "type": "utterance_end",
                            "utterance_id": ending_utterance_id,
                        })

                        final_original = ""
                        speech_started = False
                        state["final_translation"] = ""
                        state["translation_partial"] = ""
                        state["translation_prev_hypothesis"] = ""
                        state["translation_tts_sent"] = 0
                        state["tts_only_sent"] = 0
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
                        state["tts_streams"].pop(sid, None)
                        state["tts_done"].pop(sid, None)
                        continue

                    audio = data.get("audio")
                    if audio:
                        info = state["tts_streams"].get(sid, {})
                        utterance_id = info.get("utterance_id")
                        timing = state["timings"].get(utterance_id)
                        if timing is not None and timing["first_audio_at"] is None:
                            timing["first_audio_at"] = time.monotonic()
                            elapsed_ms = round(
                                (timing["first_audio_at"] - timing["speech_started_at"]) * 1000
                            )
                            try:
                                await ws.send_json({
                                    "type": "pipeline_timing",
                                    "stage": "audio_server",
                                    "elapsed_ms": elapsed_ms,
                                    "utterance_id": utterance_id,
                                })
                            except Exception:
                                pass
                        await ws.send_json({
                            "type": "audio",
                            "audio": audio,
                            "sample_rate": 24000,
                            "stream_id": sid,
                            "utterance_id": utterance_id,
                        })

                    if data.get("terminated"):
                        done = state["tts_done"].get(sid)
                        if done:
                            done.set()
                        state["tts_streams"].pop(sid, None)
                        state["tts_done"].pop(sid, None)
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
<div class="card"><div class="brand"><div class="logo">S</div><div><h1 class="title">SpeakEasy</h1><div class="sub">Live voice call — TTS voice test</div></div><span class="muted" style="margin-left:auto">Agora + Soniox TTS</span></div></div>
<div class="card">
<div class="row"><div class="field"><label>Room code</label><input id="room" value="demo-room" autocomplete="off"></div>
<div class="field"><label>Language</label><select id="src"><option value="en">English</option></select></div></div>
<div class="field"><label>Call audio</label><select id="tgt"><option value="en">English voice</option></select></div>
<div class="note">Both people use the same room code. Your microphone is converted to a different English voice by Soniox TTS before it is sent to Agora. Translation is disabled for this test.</div>
<audio id="playback" autoplay playsinline style="display:none"></audio><div class="controls"><button id="start" class="primary">Start call</button><button id="stop" class="danger" disabled>End call</button></div>
<div class="status"><span id="status">Ready</span><span id="uid" class="muted" style="float:right"></span></div>
</div>
<div class="card call"><div id="orb" class="orb"></div><div id="dir" class="direction">Not connected</div><div id="roomline" class="room">Choose languages and start</div><div class="meter"><i id="meter"></i></div></div>
<div class="card"><div class="muted">Live voice</div><div id="original" class="box">Your real microphone audio is sent directly through Agora.</div><div class="muted" style="margin-top:12px">Audio path</div><div id="translated" class="box translated">Microphone → Soniox STT → Soniox TTS → Agora → remote speaker</div></div>
<div class="card"><div class="muted">Voice test</div><div id="rawStt" class="box">English speech is recognized and regenerated with the Soniox TTS voice “Adrian”. Translation is disabled.</div><div id="sttMeta" class="note">Testing the live voice-conversion path.</div></div>
<div class="card"><div class="muted">Session log</div><div id="log" class="log"></div></div>
</main>
<script>
let AgoraRTC=null,client=null,localTrack=null,stream=null,playCtx=null,micCtx=null,micSource=null,processor=null,silent=null,monitorGain=null,outDestination=null,playbackEl=null,ws=null,running=false,starting=false,playAt=0,staged=[],config=null,micPackets=0,micBytes=0,speechAt=0,firstSttAt=0,translationAt=0,audioAt=0,lastRawSttText="",timingTranslationShown=false,timingAudioShown=false,pipelineTiming={},diagnosticSourceLanguage="",diagnosticTokenCount=0,currentUtteranceId=0;
const $=id=>document.getElementById(id);
const write=x=>{$("log").textContent+=String(x)+"\n";$("log").scrollTop=$("log").scrollHeight};
const setStatus=(x,err=false)=>{$("status").textContent=x;$("status").className=err?"error":""};
function renderTiming(){const lagParts=[];if(speechAt&&firstSttAt)lagParts.push("first STT token: "+Math.round(firstSttAt-speechAt)+" ms");lagParts.push("mic packets: "+micPackets+" ("+Math.round(micBytes/1024)+" KB)");if(diagnosticSourceLanguage)lagParts.push("language: "+diagnosticSourceLanguage);if(diagnosticTokenCount>0)lagParts.push("last token frame: "+diagnosticTokenCount);if(pipelineTiming.translation_token!=null)lagParts.push("translation token: "+pipelineTiming.translation_token+" ms"+(pipelineTiming.translation_token_final!=null?" · final="+pipelineTiming.translation_token_final:""));if(pipelineTiming.tts_text_sent!=null)lagParts.push("TTS text sent: "+pipelineTiming.tts_text_sent+" ms");if(pipelineTiming.audio_server!=null)lagParts.push("server audio: "+pipelineTiming.audio_server+" ms");if(speechAt&&translationAt)lagParts.push("first translation: "+Math.round(translationAt-speechAt)+" ms");if(speechAt&&audioAt)lagParts.push("first audio: "+Math.round(audioAt-speechAt)+" ms");$("sttMeta").textContent=lagParts.join(" · ")}
const roomName=v=>{const x=String(v||"").toLowerCase().replace(/[^a-z0-9_-]/g,"").slice(0,40);return x||"demo-room"};
const channel=v=>(config.agora_channel_prefix||"speakeasy-")+roomName(v);
function down(a,from,to){if(from===to)return a;const r=from/to,n=Math.max(1,Math.round(a.length/r)),o=new Float32Array(n);let p=0;for(let i=0;i<n;i++){const q=Math.min(a.length,Math.round((i+1)*r));let s=0,c=0;for(let j=p;j<q;j++){s+=a[j];c++}o[i]=c?s/c:0;p=q}return o}
function pcm(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const s=Math.max(-1,Math.min(1,a[i]));o[i]=s<0?s*32768:s*32767}return o}
function clearQueue(){staged.forEach(n=>{try{n.stop()}catch(e){}});staged=[];if(playCtx)playAt=playCtx.currentTime+.02}
function playTranslated(b64,rate){if(!playCtx||!outDestination)return;const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));const s=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));const b=playCtx.createBuffer(1,s.length,rate);const ch=b.getChannelData(0);for(let i=0;i<s.length;i++)ch[i]=s[i]/32768;const n=playCtx.createBufferSource();n.buffer=b;n.connect(outDestination);playAt=Math.max(playAt,playCtx.currentTime+.01);n.start(playAt);playAt+=b.duration;staged.push(n);n.onended=()=>staged=staged.filter(x=>x!==n);$("orb").classList.add("speaking")}
async function loadAgora(){if(AgoraRTC)return;await new Promise((ok,bad)=>{const s=document.createElement("script");s.src="https://download.agora.io/sdk/release/AgoraRTC_N-"+encodeURIComponent(config.agora_sdk_version)+".js";s.onload=ok;s.onerror=()=>bad(new Error("Could not load Agora Web SDK"));document.head.appendChild(s)});AgoraRTC=window.AgoraRTC}
async function stopCall(){
  running=false;
  starting=false;
  if(ws)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}
  if(ws)try{ws.close()}catch(e){}
  ws=null;
  if(playbackEl){try{playbackEl.pause()}catch(e){}playbackEl.srcObject=null;playbackEl=null}
  if(processor)try{processor.disconnect()}catch(e){}
  if(micSource)try{micSource.disconnect()}catch(e){}
  if(silent)try{silent.disconnect()}catch(e){}
  processor=micSource=silent=null;
  clearQueue();
  const c=client;
  client=null;
  if(localTrack){
    try{if(c&&c.connectionState==="CONNECTED")await c.unpublish([localTrack])}catch(e){}
    try{localTrack.close()}catch(e){}
    localTrack=null;
  }
  if(c){
    try{if(c.connectionState!=="DISCONNECTED")await c.leave()}catch(e){}
  }
  if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}
  if(micCtx){try{await micCtx.close()}catch(e){}micCtx=null}
  if(playCtx){try{await playCtx.close()}catch(e){}playCtx=null}
  monitorGain=null;outDestination=null;
  $("start").disabled=false;$("stop").disabled=true;
  $("dir").textContent="Not connected";$("roomline").textContent="Choose languages and start";
  $("orb").classList.remove("speaking");setStatus("Ready")
}
async function startCall(){
  if(running||starting)return;
  starting=true;
  $("start").disabled=true;
  setStatus("Connecting...");
  let c=null;
  try{
    await loadAgora();
    c=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});
    client=c;
    const subscribeRemoteAudio=async(user)=>{
      if(!user||c!==client||c.connectionState!=="CONNECTED")return;
      if(!user.hasAudio)return;
      try{
        await c.subscribe(user,"audio");
        if(user.audioTrack){
          user.audioTrack.play();
          write("Remote live voice connected (UID "+user.uid+")");
        }
      }catch(e){
        if(c===client)write("Remote audio error: "+(e?.message||e));
      }
    };
    c.on("user-published",async(user,type)=>{
      if(type!=="audio")return;
      await subscribeRemoteAudio(user);
    });
    c.on("user-unpublished",(u,type)=>{
      if(type==="audio"&&c===client)write("Remote live voice stopped (UID "+u.uid+")");
    });

    const uid=Math.floor(100000+Math.random()*900000);
    const channelName=channel($("room").value);
    const tokenResponse=await fetch("/api/agora-token?channel="+encodeURIComponent(channelName)+"&uid="+uid,{cache:"no-store"});
    let tokenJson={};
    try{tokenJson=await tokenResponse.json()}catch(e){}
    if(!tokenResponse.ok||!tokenJson.token)throw new Error(tokenJson.detail||"Could not obtain Agora RTC token");

    await c.join(config.agora_app_id,channelName,tokenJson.token,uid);
    if(!starting)throw new Error("Call was stopped while connecting");

    stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
    if(!starting)throw new Error("Call was stopped while opening microphone");

    ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/api/browser");
    await new Promise((resolve,reject)=>{
      ws.onopen=resolve;
      ws.onerror=()=>reject(new Error("Could not connect to Soniox voice pipeline"));
    });
    ws.onmessage=async(ev)=>{
      try{
        const data=JSON.parse(ev.data);
        if(data.type==="ready"){write("Soniox TTS-only pipeline ready");return}
        if(data.type==="error"){write("["+data.stage+"] "+data.message);return}
        if(data.type==="tts_text"){write("TTS: "+data.text);return}
        if(data.type==="audio"){
          playTranslated(data.audio,data.sample_rate||24000);
          audioAt=performance.now();
          if(!timingAudioShown){timingAudioShown=true;renderTiming()}
        }
      }catch(e){write("Pipeline message error: "+(e?.message||e))}
    };
    ws.send(JSON.stringify({mode:"tts_only",language:"en",target_language:"en",voice:"Adrian"}));

    playCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:24000});
    await playCtx.resume();
    outDestination=playCtx.createMediaStreamDestination();

    micCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000});
    await micCtx.resume();
    const sourceNode=micCtx.createMediaStreamSource(stream);
    const gain=micCtx.createGain();
    processor=micCtx.createScriptProcessor(4096,1,1);
    silent=micCtx.createGain();
    silent.gain.value=0;
    sourceNode.connect(gain);
    gain.connect(processor);
    processor.connect(silent);
    silent.connect(micCtx.destination);
    processor.onaudioprocess=(ev)=>{
      if(!ws||ws.readyState!==WebSocket.OPEN)return;
      const samples=ev.inputBuffer.getChannelData(0);
      const bytes=pcm(down(samples,micCtx.sampleRate,16000)).buffer;
      micPackets++;
      micBytes+=bytes.byteLength;
      try{ws.send(bytes)}catch(e){}
    };

    const outTrack=outDestination.stream.getAudioTracks()[0];
    localTrack=AgoraRTC.createCustomAudioTrack({mediaStreamTrack:outTrack,encoderConfig:"speech_low_quality"});
    if(!starting)throw new Error("Call was stopped while preparing TTS audio");
    await c.publish([localTrack]);

    for(const user of (c.remoteUsers||[])){
      await subscribeRemoteAudio(user);
    }

    running=true;
    starting=false;
    $("uid").textContent="UID "+uid;
    $("dir").textContent="ENGLISH ↔ ENGLISH";
    $("roomline").textContent="Room: "+roomName($("room").value);
    $("orb").classList.add("speaking");$("meter").style.width="0%";
    setStatus("Live voice connected");
    write("Agora joined "+channelName+" — publishing Soniox TTS audio");
    write("English → English voice conversion test; translation disabled");
    setTimeout(()=>{if(running)$("orb").classList.remove("speaking")},700);
    $("stop").disabled=false;
  }catch(e){
    const message=e?.message||String(e);
    write(message);
    setStatus(message,true);
    starting=false;
    if(c===client){
      const old=client;client=null;
      try{if(old?.connectionState!=="DISCONNECTED")await old.leave()}catch(_){}
    }
    if(localTrack){try{localTrack.close()}catch(_){}localTrack=null}
    running=false;
    $("start").disabled=false;$("stop").disabled=true;
  }
}
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
