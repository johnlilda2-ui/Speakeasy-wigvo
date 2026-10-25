from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
import math
import urllib.error
import urllib.request
from typing import Any
from uuid import uuid4

import websockets
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("speakeasy_wigvo.krisp")

KRISP_API_KEY = os.getenv("KRISP_API_KEY", "").strip()
KRISP_TOKEN_URL = "https://api.developers.krisp.ai/v2/sdk/tokens/session"
KRISP_VT_TOKEN_URL = "https://api.developers.krisp.ai/v2/sdk/voice-translation/session/token"
KRISP_VT_URL = "wss://streaming.krisp.ai/vt"


def _safe_diagnostic_text(value: Any) -> str:
    """Log only bounded transcript text; redact credential-like strings and control characters."""
    if not isinstance(value, str):
        return ""
    text = re.sub(r"[\r\n\t]+", " ", value)
    text = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+", "[redacted]", text)
    text = re.sub(r"(?i)\b(api[-_ ]?key|token|secret|password)\s*[:=]\s*[^\s,;]+", r"\1=[redacted]", text)
    if KRISP_API_KEY:
        text = text.replace(KRISP_API_KEY, "[redacted]")
    return text[:240]


def _safe_krisp_http_error(exc: urllib.error.HTTPError) -> str:
    """Summarize a Krisp HTTP failure without logging credentials or raw HTML."""
    headers = exc.headers or {}
    parts = [f"HTTP {exc.code}"]
    for header, label in (
        ("Content-Type", "content_type"),
        ("Server", "server"),
        ("CF-Ray", "cf_ray"),
        ("CF-Mitigated", "cf_mitigated"),
    ):
        value = str(headers.get(header) or "").strip()
        if value:
            parts.append(f"{label}={re.sub(r'[^A-Za-z0-9._:/ -]', '', value)[:100]}")

    raw = exc.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        payload = None

    if isinstance(payload, dict):
        # Whitelist common diagnostic fields; never dump the full response object.
        for key in ("error_code", "code", "message", "detail", "req_id", "request_id"):
            value = payload.get(key)
            if value is not None and isinstance(value, (str, int, float)):
                raw_value = re.sub(r"[\r\n\t]+", " ", str(value))
                if KRISP_API_KEY:
                    raw_value = raw_value.replace(KRISP_API_KEY, "[redacted]")
                safe_value = re.sub(r"[^A-Za-z0-9._:/ -]", "", raw_value)[:220]
                if safe_value:
                    parts.append(f"{key}={safe_value}")
    else:
        # Avoid logging arbitrary HTML or response text from an upstream service.
        if re.search(r"cloudflare", raw, re.IGNORECASE):
            parts.append("response_hint=cloudflare_page")
        match = re.search(r"error\s*code\s*[:=]?\s*(\d{3,5})", raw, re.IGNORECASE)
        if match:
            parts.append(f"response_code={match.group(1)}")
        elif not re.search(r"cloudflare", raw, re.IGNORECASE):
            parts.append("response_hint=non_json_body")
    return "; ".join(parts)
AGORA_APP_ID = os.getenv("AGORA_APP_ID", "").strip()
AGORA_SDK_VERSION = os.getenv("AGORA_SDK_VERSION", "4.24.8").strip()
AGORA_CHANNEL_PREFIX = os.getenv("AGORA_CHANNEL_PREFIX", "speakeasy-").strip() or "speakeasy-"


def _create_session_token() -> str:
    if not KRISP_API_KEY:
        raise RuntimeError("KRISP_API_KEY is not configured on Render.")
    body = json.dumps({"usage_type": "vt", "expiration_ttl": 60}).encode()
    headers = {
        "Authorization": f"api-key {KRISP_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "WIGVO-Backend/1.0",
    }
    attempts = [
        ("voice_translation_session_token_get", urllib.request.Request(
            KRISP_VT_TOKEN_URL,
            method="GET",
            headers={
                "Authorization": headers["Authorization"],
                "Accept": "application/json",
                "User-Agent": headers["User-Agent"],
            },
        )),
        ("sdk_session_token_post", urllib.request.Request(
            KRISP_TOKEN_URL,
            data=body,
            method="POST",
            headers=headers,
        )),
    ]
    last_error = "Krisp did not return a session token."
    for endpoint_name, req in attempts:
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                result = json.loads(response.read().decode())
                status_code = getattr(response, "status", 200)
            token = ((result.get("data") or {}).get("session_key") or "").strip()
            if token:
                logger.info(
                    "Krisp session token created endpoint=%s method=%s status=%s",
                    endpoint_name, req.get_method(), status_code,
                )
                return token
            safe_code = result.get("error_code") or result.get("code") or "unknown"
            last_error = f"Krisp token endpoint {endpoint_name} returned no session_key (code={str(safe_code)[:80]})."
            logger.warning(
                "Krisp token attempt returned no session_key endpoint=%s method=%s status=%s code=%s",
                endpoint_name, req.get_method(), status_code, str(safe_code)[:80],
            )
        except urllib.error.HTTPError as exc:
            detail = _safe_krisp_http_error(exc)
            last_error = f"Krisp token request failed at {endpoint_name}: {detail}"
            logger.warning(
                "Krisp token request failed endpoint=%s method=%s %s",
                endpoint_name, req.get_method(), detail,
            )
        except Exception as exc:
            # Do not log request headers, API keys, or session tokens.
            last_error = f"Krisp token attempt failed at {endpoint_name} ({type(exc).__name__})."
            logger.warning(
                "Krisp token attempt failed endpoint=%s method=%s exception_type=%s",
                endpoint_name, req.get_method(), type(exc).__name__,
            )
    raise RuntimeError(last_error)

def _get_supported_languages() -> set[str]:
    request = urllib.request.Request(
        "https://api.developers.krisp.ai/v2/sdk/voice-translation/languages",
        headers={"Authorization": f"api-key {KRISP_API_KEY}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        # Krisp's response body may contain a useful error_code/message/req_id.
        # Never log request headers or the API key.
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(detail)
            error_code = payload.get("error_code") or payload.get("code")
            message = payload.get("message") or payload.get("detail") or ""
            req_id = payload.get("req_id") or ""
            parts = [f"HTTP {exc.code}"]
            if error_code:
                parts.append(f"code={str(error_code)[:100]}")
            if message:
                parts.append(f"message={str(message)[:240]}")
            if req_id:
                parts.append(f"request_id={str(req_id)[:100]}")
            safe_detail = "; ".join(parts)
        except Exception:
            safe_detail = f"HTTP {exc.code}: {detail[:240]}"
        raise RuntimeError(f"Krisp supported-languages request failed: {safe_detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"Could not verify Krisp's supported languages: {type(exc).__name__}: {str(exc)[:240]}") from exc
    entries = (result.get("data") or [])
    codes = {str(item.get("language_code") or "") for item in entries if isinstance(item, dict)}
    if not codes:
        raise RuntimeError("Krisp returned no supported language codes.")
    return codes

async def browser_translation(ws: WebSocket) -> None:
    await ws.accept()
    if not KRISP_API_KEY:
        await ws.send_json({"type": "error", "stage": "config", "message": "KRISP_API_KEY is not configured on Render."})
        await ws.close(code=1011)
        return
    upstream = None
    tasks: list[asyncio.Task[Any]] = []
    session_tag = uuid4().hex[:8]
    timing_marks: dict[str, float] = {}
    frame_timeline: list[str] = []
    frame_timeline_batch = 0
    input_send_seq = 0
    next_upstream_audio_at = 0.0
    output_recv_seq = 0
    transcript_event_seq = 0
    translate_event_seq = 0
    metrics: dict[str, Any] = {
        "started_at": time.monotonic(),
        "input_frames": 0, "input_bytes": 0, "input_frame_min": None, "input_frame_max": None,
        "input_last_at": None, "input_first_at": None, "input_interval_count": 0,
        "input_interval_sum_ms": 0.0, "input_interval_min_ms": None, "input_interval_max_ms": None,
        "output_frames": 0, "output_bytes": 0, "output_frame_min": None, "output_frame_max": None,
        "output_last_at": None, "output_first_at": None, "output_interval_count": 0,
        "output_interval_sum_ms": 0.0, "output_interval_min_ms": None, "output_interval_max_ms": None,
        "first_krisp_send_started_at": None, "first_krisp_send_completed_at": None,
        "first_output_forwarded_at": None, "first_transcript_at": None, "first_translate_at": None,
        "first_final_transcript_at": None, "first_final_translate_at": None,
        "transcript_events": 0, "transcript_final_events": 0,
        "translate_events": 0, "translate_final_events": 0,
        "output_frames_before_final_translate": 0,
        "playback_scheduled_frames": 0, "playback_end_events": 0,
        "playback_not_played_frames": 0, "playback_interrupted_frames": 0,
        "first_playback_schedule_client_ms": None, "first_playback_scheduled_start_client_ms": None,
        "first_playback_receive_to_start_ms": None, "last_playback_end_client_ms": None,
        "playback_callback_late_max_ms": None, "playback_queue_ahead_max_ms": None,
    }

    def mark_timing(name: str, at: float | None = None) -> None:
        timing_marks[name] = time.monotonic() if at is None else at

    def elapsed_ms(at: float | None) -> float | None:
        if at is None:
            return None
        return round((at - metrics["started_at"]) * 1000.0, 2)

    def timing_span_ms(start_name: str, end_name: str) -> float | None:
        start_at = timing_marks.get(start_name)
        end_at = timing_marks.get(end_name)
        if start_at is None or end_at is None:
            return None
        return round((end_at - start_at) * 1000.0, 2)

    mark_timing("session_opened", metrics["started_at"])

    def flush_frame_timeline() -> None:
        nonlocal frame_timeline_batch
        if not frame_timeline:
            return
        frame_timeline_batch += 1
        logger.info(
            "Krisp browser frame timeline session=%s batch=%d events=%s",
            session_tag, frame_timeline_batch, "|".join(frame_timeline),
        )
        frame_timeline.clear()

    def add_frame_timeline(event: str) -> None:
        frame_timeline.append(event)
        # Batch frame-level timestamps to avoid one log line per 16 ms audio frame.
        if len(frame_timeline) >= 64:
            flush_frame_timeline()

    def record_frame(direction: str, size: int, now: float) -> None:
        prefix = "input" if direction == "input" else "output"
        metrics[f"{prefix}_frames"] += 1
        metrics[f"{prefix}_bytes"] += size
        min_key, max_key = f"{prefix}_frame_min", f"{prefix}_frame_max"
        metrics[min_key] = size if metrics[min_key] is None else min(metrics[min_key], size)
        metrics[max_key] = size if metrics[max_key] is None else max(metrics[max_key], size)
        last_key, first_key = f"{prefix}_last_at", f"{prefix}_first_at"
        if metrics[first_key] is None:
            metrics[first_key] = now
        previous = metrics[last_key]
        if previous is not None:
            interval_ms = max(0.0, (now - previous) * 1000.0)
            count_key, sum_key = f"{prefix}_interval_count", f"{prefix}_interval_sum_ms"
            min_interval_key, max_interval_key = f"{prefix}_interval_min_ms", f"{prefix}_interval_max_ms"
            metrics[count_key] += 1
            metrics[sum_key] += interval_ms
            metrics[min_interval_key] = interval_ms if metrics[min_interval_key] is None else min(metrics[min_interval_key], interval_ms)
            metrics[max_interval_key] = interval_ms if metrics[max_interval_key] is None else max(metrics[max_interval_key], interval_ms)
        metrics[last_key] = now

    def interval_avg(direction: str) -> float | None:
        count = metrics[f"{direction}_interval_count"]
        return round(metrics[f"{direction}_interval_sum_ms"] / count, 2) if count else None

    def safe_number(value: Any) -> int | float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None
        return value

    try:
        cfg = await ws.receive_json()
        mark_timing("browser_config_received")
        source = str(cfg.get("source_language") or "en-US")
        target = str(cfg.get("target_language") or ("pt-BR" if source == "en-US" else "en-US"))
        allowed_languages = {"en-US", "pt-BR"}
        if source not in allowed_languages or target not in allowed_languages:
            raise RuntimeError("Choose English (US) or Portuguese (Brazil) for both language fields.")
        if source == target:
            raise RuntimeError("The source language and target language must be different.")
        # Do not block a translation session on the language-catalog endpoint.
        # Krisp may return an upstream HTTP 403 (including edge/WAF error 1010)
        # for this informational endpoint even when session-token/VT access works.
        # The VT session handshake validates the actual language pair and returns
        # a structured validation error if Krisp cannot use it.
        voice = str(cfg.get("voice") or "female").lower()
        if voice not in {"male", "female"}:
            voice = "female"
        mark_timing("token_request_started")
        token = await asyncio.to_thread(_create_session_token)
        mark_timing("token_ready")
        mark_timing("upstream_connect_started")
        upstream = await websockets.connect(
            KRISP_VT_URL,
            additional_headers={"Authorization": f"Api-Key {token}"},
            ping_interval=20, ping_timeout=20, close_timeout=5, max_size=8 * 1024 * 1024,
        )
        mark_timing("upstream_connected")
        mark_timing("upstream_config_send_started")
        await upstream.send(json.dumps({"config": {
            "audio": {"format": "pcm_s16le", "sample_rate": 16000, "channels": 1},
            "source_language": source, "target_language": target, "voice": voice,
            "features": {"background_voice_cancellation": False},
            "transcript": {"interim": True, "final": True, "translate": True},
        }}))
        mark_timing("upstream_config_sent")
        mark_timing("upstream_ready_wait_started")
        try:
            hello_raw = await asyncio.wait_for(upstream.recv(), timeout=45)
            hello = json.loads(hello_raw) if isinstance(hello_raw, str) else {}
            mark_timing("upstream_ready_received")
            if hello.get("type") == "ready":
                logger.info(
                    "Krisp browser upstream ready session=%s request_id=%s tracks_ms=%s",
                    session_tag,
                    str(hello.get("request_id") or "")[:100],
                    json.dumps(hello.get("tracks_ms"))[:300] if hello.get("tracks_ms") is not None else "unavailable",
                )
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Krisp did not become ready within 45 seconds.") from exc
        if hello.get("type") != "ready":
            error = hello.get("error") or {}
            await ws.send_json({"type": "error", "stage": "krisp",
                "message": error.get("message") or hello.get("message") or "Krisp could not start Voice Translation.",
                "code": error.get("code") or hello.get("error_code")})
            return
        await ws.send_json({"type": "ready", "provider": "krisp",
                            "source_language": source, "target_language": target, "sample_rate": 16000})
        mark_timing("browser_ready_sent")
        logger.info(
            "Krisp browser stage timings session=%s session_to_config_ms=%s token_request_ms=%s "
            "connect_ms=%s config_send_ms=%s ready_wait_ms=%s session_to_ready_ms=%s",
            session_tag,
            timing_span_ms("session_opened", "browser_config_received"),
            timing_span_ms("token_request_started", "token_ready"),
            timing_span_ms("upstream_connect_started", "upstream_connected"),
            timing_span_ms("upstream_config_send_started", "upstream_config_sent"),
            timing_span_ms("upstream_ready_wait_started", "upstream_ready_received"),
            elapsed_ms(timing_marks.get("upstream_ready_received")),
        )

        async def forward_input() -> None:
            nonlocal input_send_seq, next_upstream_audio_at
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    return
                if msg.get("bytes"):
                    audio = msg["bytes"]
                    received_at = time.monotonic()
                    record_frame("input", len(audio), received_at)
                    input_send_seq += 1
                    if len(audio) <= 65536:
                        # Krisp VT expects input audio paced in real time. Browser audio callbacks
                        # can arrive in bursts; preserve each PCM frame's duration at the upstream boundary.
                        frame_duration_s = len(audio) / 32000.0  # 16 kHz * mono * 16-bit PCM
                        now = time.monotonic()
                        if next_upstream_audio_at > now:
                            await asyncio.sleep(next_upstream_audio_at - now)
                        send_started_at = time.monotonic()
                        if metrics["first_krisp_send_started_at"] is None:
                            metrics["first_krisp_send_started_at"] = send_started_at
                            mark_timing("first_krisp_send_started", send_started_at)
                        try:
                            await upstream.send(audio)
                        finally:
                            send_completed_at = time.monotonic()
                            next_upstream_audio_at = max(send_started_at, send_completed_at) + frame_duration_s
                            if metrics["first_krisp_send_completed_at"] is None:
                                metrics["first_krisp_send_completed_at"] = send_completed_at
                                mark_timing("first_krisp_send_completed", send_completed_at)
                            add_frame_timeline(
                                f"I{input_send_seq}[r={elapsed_ms(received_at)},"
                                f"s={elapsed_ms(send_started_at)},e={elapsed_ms(send_completed_at)},"
                                f"wait={round((send_completed_at-send_started_at)*1000.0,3)},b={len(audio)}]"
                            )
                    else:
                        await ws.send_json({"type": "error", "stage": "audio", "message": "Audio frame exceeded 64 KB."})
                elif msg.get("text"):
                    try:
                        control = json.loads(msg["text"])
                        if not isinstance(control, dict):
                            continue
                        if control.get("type") == "audio_diagnostics":
                            stage = control.get("stage")
                            if stage in {"audio_context", "audio_summary"}:
                                allowed = (
                                    "mic_sample_rate", "play_sample_rate", "input_buffer_size",
                                    "input_callback_count", "input_samples", "input_bytes",
                                    "input_frame_bytes_min", "input_frame_bytes_max",
                                    "input_callback_interval_ms_avg", "input_callback_interval_ms_min",
                                    "input_callback_interval_ms_max", "output_audio_frames", "output_audio_bytes",
                                    "output_frame_bytes_min", "output_frame_bytes_max",
                                    "output_interval_ms_avg", "output_interval_ms_min",
                                    "output_interval_ms_max", "first_output_audio_ms_from_first_input",
                                    "playback_pending_frames",
                                )
                                fields = []
                                for key in allowed:
                                    value = safe_number(control.get(key))
                                    if value is not None:
                                        fields.append(f"{key}={value}")
                                logger.info("Browser audio diagnostics stage=%s %s", stage, " ".join(fields))
                        elif control.get("type") == "utterance_diagnostics":
                            diagnostic_events = control.get("events")
                            if isinstance(diagnostic_events, list):
                                for diagnostic in diagnostic_events[:16]:
                                    if not isinstance(diagnostic, dict):
                                        continue
                                    kind = diagnostic.get("kind")
                                    if kind not in {"speech_start", "speech_end"}:
                                        continue
                                    utterance_id = safe_number(diagnostic.get("utterance_id"))
                                    client_ms = safe_number(diagnostic.get("at_ms"))
                                    detected_ms = safe_number(diagnostic.get("detected_ms"))
                                    duration_ms = safe_number(diagnostic.get("duration_ms"))
                                    rms = safe_number(diagnostic.get("rms"))
                                    fields = [f"session={session_tag}", f"kind={kind}",
                                              f"utterance={int(utterance_id) if utterance_id is not None else 0}",
                                              f"client_ms={client_ms}"]
                                    if detected_ms is not None:
                                        fields.append(f"detected_ms={detected_ms}")
                                    if duration_ms is not None:
                                        fields.append(f"duration_ms={duration_ms}")
                                    if rms is not None:
                                        fields.append(f"rms={rms}")
                                    logger.info("WIGVO utterance timing %s", " ".join(fields))
                                    add_frame_timeline(
                                        f"U{int(utterance_id) if utterance_id is not None else 0}"
                                        f"[kind={kind},speech_end_ms={client_ms},detected_ms={detected_ms},duration_ms={duration_ms}]"
                                    )
                        elif control.get("type") == "playback_diagnostics":
                            diagnostic_events = control.get("events")
                            if isinstance(diagnostic_events, list):
                                for diagnostic in diagnostic_events[:32]:
                                    if not isinstance(diagnostic, dict):
                                        continue
                                    kind = diagnostic.get("kind")
                                    if kind not in {"scheduled", "ended", "not_played", "interrupted"}:
                                        continue
                                    seq = safe_number(diagnostic.get("seq"))
                                    client_ms = safe_number(diagnostic.get("at_ms"))
                                    if kind == "scheduled":
                                        metrics["playback_scheduled_frames"] += 1
                                        if metrics["first_playback_schedule_client_ms"] is None:
                                            metrics["first_playback_schedule_client_ms"] = safe_number(diagnostic.get("scheduled_at_ms"))
                                            scheduled_start_ms = safe_number(diagnostic.get("scheduled_start_ms"))
                                            metrics["first_playback_scheduled_start_client_ms"] = scheduled_start_ms
                                            rx_ms = safe_number(diagnostic.get("rx_ms"))
                                            if scheduled_start_ms is not None and rx_ms is not None:
                                                metrics["first_playback_receive_to_start_ms"] = round(scheduled_start_ms - rx_ms, 2)
                                        queue_ahead = safe_number(diagnostic.get("queue_ahead_ms"))
                                        if queue_ahead is not None:
                                            old_max = metrics["playback_queue_ahead_max_ms"]
                                            metrics["playback_queue_ahead_max_ms"] = queue_ahead if old_max is None else max(old_max, queue_ahead)
                                    elif kind == "ended":
                                        metrics["playback_end_events"] += 1
                                        metrics["last_playback_end_client_ms"] = safe_number(diagnostic.get("end_callback_ms"))
                                        callback_late = safe_number(diagnostic.get("end_callback_late_ms"))
                                        if callback_late is not None:
                                            old_late = metrics["playback_callback_late_max_ms"]
                                            metrics["playback_callback_late_max_ms"] = max(0.0, callback_late) if old_late is None else max(old_late, callback_late)
                                    elif kind == "not_played":
                                        metrics["playback_not_played_frames"] += 1
                                    elif kind == "interrupted":
                                        metrics["playback_interrupted_frames"] += 1
                                    prefix = {"scheduled": "PS", "ended": "PE", "not_played": "PN", "interrupted": "PI"}[kind]
                                    fields = [f"at={client_ms}"]
                                    for key in ("rx_ms", "scheduled_at_ms", "scheduled_start_ms", "scheduled_end_ms",
                                                "start_ctx_ms", "end_ctx_ms", "end_callback_ms", "end_callback_ctx_ms", "end_callback_late_ms",
                                                "queue_ahead_ms", "frame_ms", "remaining_ms"):
                                        value = safe_number(diagnostic.get(key))
                                        if value is not None:
                                            fields.append(f"{key}={value}")
                                    reason = diagnostic.get("reason")
                                    if isinstance(reason, str) and reason in {"no_audio_context", "no_stream_destination", "muted", "session_stop", "empty_frame"}:
                                        fields.append(f"reason={reason}")
                                    add_frame_timeline(f"{prefix}{int(seq) if seq is not None else 0}[{','.join(fields)}]")
                        elif control.get("type") == "stop":
                            return
                    except (json.JSONDecodeError, TypeError):
                        pass

        async def forward_output() -> None:
            nonlocal output_recv_seq, transcript_event_seq, translate_event_seq
            async for raw in upstream:
                if isinstance(raw, bytes):
                    output_received_at = time.monotonic()
                    record_frame("output", len(raw), output_received_at)
                    output_recv_seq += 1
                    if metrics["first_final_translate_at"] is None:
                        metrics["output_frames_before_final_translate"] += 1
                    if metrics["output_first_at"] == output_received_at:
                        mark_timing("first_output_received", output_received_at)
                    add_frame_timeline(
                        f"O{output_recv_seq}[r={elapsed_ms(output_received_at)},b={len(raw)}]"
                    )
                    browser_send_started_at = time.monotonic()
                    await ws.send_json({"type": "audio", "seq": output_recv_seq, "audio": base64.b64encode(raw).decode("ascii"), "sample_rate": 16000})
                    browser_send_completed_at = time.monotonic()
                    if metrics["first_output_forwarded_at"] is None:
                        metrics["first_output_forwarded_at"] = browser_send_completed_at
                        mark_timing("first_output_forwarded", browser_send_completed_at)
                    add_frame_timeline(
                        f"F{output_recv_seq}[s={elapsed_ms(browser_send_started_at)},"
                        f"e={elapsed_ms(browser_send_completed_at)},"
                        f"relay={round((browser_send_completed_at-browser_send_started_at)*1000.0,3)}]"
                    )
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                upstream_event_at = time.monotonic()
                transcript_item = event.get("transcript")
                if isinstance(transcript_item, dict):
                    transcript_event_seq += 1
                    metrics["transcript_events"] += 1
                    transcript_final = bool(transcript_item.get("final"))
                    if transcript_final:
                        metrics["transcript_final_events"] += 1
                        if metrics["first_final_transcript_at"] is None:
                            metrics["first_final_transcript_at"] = upstream_event_at
                    if metrics["first_transcript_at"] is None:
                        metrics["first_transcript_at"] = upstream_event_at
                    transcript_text = _safe_diagnostic_text(transcript_item.get("text"))
                    transcript_text_len = len(transcript_text)
                    logger.info(
                        "WIGVO recognized text session=%s event=%d at_ms=%s final=%s text=%s",
                        session_tag, transcript_event_seq, elapsed_ms(upstream_event_at), int(transcript_final), json.dumps(transcript_text, ensure_ascii=True),
                    )
                    transcript_duration = safe_number(transcript_item.get("duration"))
                    add_frame_timeline(
                        f"T{transcript_event_seq}[t={elapsed_ms(upstream_event_at)},"
                        f"final={int(transcript_final)},chars={transcript_text_len},"
                        f"dur={transcript_duration}]"
                    )
                translate_item = event.get("translate")
                if isinstance(translate_item, dict):
                    translate_event_seq += 1
                    metrics["translate_events"] += 1
                    translate_final = bool(translate_item.get("final"))
                    if translate_final:
                        metrics["translate_final_events"] += 1
                        if metrics["first_final_translate_at"] is None:
                            metrics["first_final_translate_at"] = upstream_event_at
                    if metrics["first_translate_at"] is None:
                        metrics["first_translate_at"] = upstream_event_at
                    translate_text = _safe_diagnostic_text(translate_item.get("text"))
                    translate_text_len = len(translate_text)
                    logger.info(
                        "WIGVO translated text session=%s event=%d at_ms=%s final=%s utterance_id=%s text=%s",
                        session_tag, translate_event_seq, elapsed_ms(upstream_event_at), int(translate_final), str(translate_item.get("utterance_id") or "")[:100], json.dumps(translate_text, ensure_ascii=True),
                    )
                    add_frame_timeline(
                        f"X{translate_event_seq}[t={elapsed_ms(upstream_event_at)},"
                        f"final={int(translate_final)},chars={translate_text_len}]"
                    )
                err = event.get("error")
                if event.get("type") == "error" or err:
                    logger.warning(
                        "Krisp browser upstream error session=%s elapsed_ms=%s code=%s status_code=%s request_id=%s",
                        session_tag, elapsed_ms(time.monotonic()),
                        str((err or {}).get("code") or event.get("error_code") or "")[:100],
                        str((err or {}).get("status_code") or "")[:30],
                        str((err or {}).get("request_id") or event.get("request_id") or "")[:100],
                    )
                    await ws.send_json({"type": "error", "stage": "krisp",
                        "message": (err or {}).get("message") or event.get("message") or "Krisp returned an error."})
                if isinstance(event.get("transcript"), dict):
                    item = event["transcript"]
                    await ws.send_json({"type": "transcript", "text": item.get("text", ""), "final": bool(item.get("final"))})
                if isinstance(event.get("translate"), dict):
                    item = event["translate"]
                    await ws.send_json({"type": "translate", "text": item.get("text", ""), "final": bool(item.get("final"))})

        tasks = [asyncio.create_task(forward_input()), asyncio.create_task(forward_output())]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in done:
            if task.exception():
                raise task.exception()
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "stage": "server", "message": str(exc)})
        except Exception:
            pass
    finally:
        now = time.monotonic()
        flush_frame_timeline()
        first_input_at = metrics["input_first_at"]
        first_output_at = metrics["output_first_at"]
        first_output_ms = round((first_output_at - metrics["started_at"]) * 1000.0, 2) if first_output_at is not None else None
        first_output_after_input_ms = round((first_output_at - first_input_at) * 1000.0, 2) if first_output_at is not None and first_input_at is not None else None
        logger.info(
            "Krisp browser audio summary duration_ms=%.2f input_frames=%d input_bytes=%d "
            "input_frame_bytes_min=%s input_frame_bytes_max=%s input_interval_ms_avg=%s "
            "input_interval_ms_min=%s input_interval_ms_max=%s output_audio_frames=%d output_audio_bytes=%d "
            "output_frame_bytes_min=%s output_frame_bytes_max=%s output_interval_ms_avg=%s "
            "output_interval_ms_min=%s output_interval_ms_max=%s first_output_audio_ms_from_session=%s "
            "first_output_audio_ms_from_first_input=%s",
            (now - metrics["started_at"]) * 1000.0,
            metrics["input_frames"], metrics["input_bytes"], metrics["input_frame_min"], metrics["input_frame_max"],
            interval_avg("input"), metrics["input_interval_min_ms"], metrics["input_interval_max_ms"],
            metrics["output_frames"], metrics["output_bytes"], metrics["output_frame_min"], metrics["output_frame_max"],
            interval_avg("output"), metrics["output_interval_min_ms"], metrics["output_interval_max_ms"],
            first_output_ms, first_output_after_input_ms,
        )
        logger.info(
            "Krisp browser latency summary session=%s duration_ms=%.2f "
            "first_input_from_session_ms=%s first_krisp_send_start_from_session_ms=%s "
            "first_krisp_send_done_from_session_ms=%s first_output_from_session_ms=%s "
            "first_output_from_ready_ms=%s first_output_after_first_krisp_send_ms=%s "
            "first_output_after_first_send_done_ms=%s first_transcript_from_session_ms=%s "
            "first_translate_from_session_ms=%s first_translate_after_first_input_ms=%s "
            "first_output_forwarded_from_session_ms=%s first_final_transcript_from_session_ms=%s "
            "first_final_translate_from_session_ms=%s transcript_events=%d transcript_final_events=%d "
            "translate_events=%d translate_final_events=%d output_frames_before_final_translate=%d "
            "playback_scheduled_frames=%d playback_end_events=%d playback_not_played_frames=%d "
            "playback_interrupted_frames=%d first_playback_schedule_client_ms=%s "
            "first_playback_scheduled_start_client_ms=%s first_playback_receive_to_start_ms=%s "
            "last_playback_end_callback_client_ms=%s playback_callback_late_max_ms=%s playback_queue_ahead_max_ms=%s",
            session_tag, (now - metrics["started_at"]) * 1000.0,
            elapsed_ms(first_input_at),
            elapsed_ms(metrics["first_krisp_send_started_at"]),
            elapsed_ms(metrics["first_krisp_send_completed_at"]),
            elapsed_ms(first_output_at),
            timing_span_ms("upstream_ready_received", "first_output_received"),
            timing_span_ms("first_krisp_send_started", "first_output_received"),
            timing_span_ms("first_krisp_send_completed", "first_output_received"),
            elapsed_ms(metrics["first_transcript_at"]),
            elapsed_ms(metrics["first_translate_at"]),
            round((metrics["first_translate_at"] - first_input_at) * 1000.0, 2)
            if metrics["first_translate_at"] is not None and first_input_at is not None else None,
            elapsed_ms(metrics["first_output_forwarded_at"]),
            elapsed_ms(metrics["first_final_transcript_at"]),
            elapsed_ms(metrics["first_final_translate_at"]),
            metrics["transcript_events"], metrics["transcript_final_events"],
            metrics["translate_events"], metrics["translate_final_events"],
            metrics["output_frames_before_final_translate"],
            metrics["playback_scheduled_frames"], metrics["playback_end_events"],
            metrics["playback_not_played_frames"], metrics["playback_interrupted_frames"],
            metrics["first_playback_schedule_client_ms"], metrics["first_playback_scheduled_start_client_ms"],
            metrics["first_playback_receive_to_start_ms"], metrics["last_playback_end_client_ms"],
            metrics["playback_callback_late_max_ms"], metrics["playback_queue_ahead_max_ms"],
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if upstream:
            try:
                await upstream.close()
            except Exception:
                pass
        try:
            await ws.close()
        except Exception:
            pass


BROWSER_HTML_PAGE = r'''<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WIGVO — Live Voice Translation</title>
<style>
*{box-sizing:border-box}body{margin:0;padding:16px;background:#0b1220;color:#edf2ff;font:16px system-ui}main{max-width:680px;margin:auto}.card{padding:16px;margin:12px 0;border:1px solid #30415f;border-radius:16px;background:#131f35}.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}label,.muted{display:block;color:#a9b8d5;font-size:13px;margin-bottom:6px}input,select,button{width:100%;padding:12px;border-radius:10px;border:1px solid #40527a;background:#0c1629;color:#fff;font-size:15px;margin-bottom:10px}button{font-weight:700}.primary{background:#315fe9}.danger{background:#6b2940}.controls{display:grid;grid-template-columns:1fr 1fr;gap:10px}.audio-controls{display:grid;grid-template-columns:1fr 1fr;gap:10px}.audio-controls button{margin-bottom:0}.audio-controls button[aria-pressed="true"]{background:#6b2940}.box{background:#0b1425;border-radius:10px;padding:12px;min-height:50px;overflow-wrap:anywhere}.note{color:#9daaca;font-size:12px;line-height:1.5}.log{font:12px monospace;white-space:pre-wrap;max-height:130px;overflow:auto}.error{color:#ff9eaf}@media(max-width:540px){.row{grid-template-columns:1fr}}
</style></head><body><main>
<div class="card"><h1 style="margin:0">WIGVO</h1><div class="muted">Live voice translation · Agora + Krisp</div></div>
<div class="card"><div class="row"><div><label>Room code</label><input id="room" value="demo-room"></div><div><label>Language you speak</label><select id="src"><option value="en-US">English (US)</option><option value="pt-BR">Português (Brasil)</option></select></div></div>
<div class="row"><div><label>Target language (what the listener hears)</label><select id="target"><option value="pt-BR">Português (Brasil)</option><option value="en-US">English (US)</option></select></div><div><label>Translated voice</label><select id="voice"><option value="female">Female</option><option value="male">Male</option></select></div></div>
<p class="note">Both people use the same room code and select the language they speak. English is translated to Brazilian Portuguese and Brazilian Portuguese to English. Only translated audio is published to Agora. Use headphones to reduce echo.</p>
<div class="controls"><button id="start" class="primary">Start call</button><button id="stop" class="danger" disabled>End call</button></div><div class="controls" style="margin-top:10px"><button id="recordStart" disabled>Start local recording</button><button id="recordStop" class="danger" disabled>Stop recording</button></div><p class="note">Local recording mixes your microphone and the other participant’s audio on this device. Start it only with everyone’s consent. Keep this page open until the recording is ready to download.</p><div id="recordingStatus" class="box" style="display:none;margin-top:8px"></div><a id="recordingDownload" style="display:none;color:#9fc0ff;overflow-wrap:anywhere" download="wigvo-call-recording.webm">Download recorded call</a><div class="audio-controls" style="margin-top:10px"><button id="micMute" disabled aria-pressed="false">Mute microphone</button><button id="micPause" disabled aria-pressed="false">Pause microphone</button><button id="myAudioMute" disabled aria-pressed="false">Mute translated audio</button><button id="otherSpeakerMute" disabled aria-pressed="false">Mute other speaker</button></div><p class="note">Microphone mute sends silence; pause microphone temporarily stops sending mic frames. “Translated audio” controls what others hear from you; “other speaker” controls the sound you hear from the other participant.</p><div class="box"><span id="status">Ready</span><span id="uid" style="float:right"></span></div></div>
<div class="card"><div class="muted">Recognized speech</div><div id="original" class="box">Waiting for speech…</div><div class="muted" style="margin-top:12px">Translation</div><div id="translated" class="box">Translated text appears here.</div></div>
<div class="card"><div class="muted">Session log</div><div id="log" class="log"></div></div>
</main><script>
let AgoraRTC=null,client=null,track=null,stream=null,micCtx=null,playCtx=null,processor=null,sourceNode=null,silent=null,destination=null,ws=null,config=null,starting=false,running=false,playAt=0,queued=[],micMuted=false,micPaused=false,myAudioMuted=false,otherSpeakerMuted=false,remoteAudioTracks=new Set(),clientSessionStartPerf=0,playbackDiagBuffer=[],lastOutputFrameFingerprint=null,lastOutputFrameAt=0,micPendingSamples=new Float32Array(0);
let recordingCtx=null,recordingDestination=null,recordingSources=new Map(),mediaRecorder=null,recordingChunks=[],recordingBlobUrl=null,recordingStartedAt=0;
const makeAudioStats=()=>({frames:0,bytes:0,samples:0,minFrameBytes:null,maxFrameBytes:null,firstAt:null,lastAt:null,intervalCount:0,intervalSum:0,minInterval:null,maxInterval:null});
let clientInputStats=makeAudioStats(),clientOutputStats=makeAudioStats();
let utteranceSeq=0,utteranceActive=false,utteranceStartPerf=0,utteranceLastVoicePerf=0,utterancePeakRms=0;
const speechRmsThreshold=0.012,speechEndSilenceMs=650,speechMinDurationMs=180;
function sendUtteranceDiag(event){if(!ws||ws.readyState!==1)return;ws.send(JSON.stringify({type:"utterance_diagnostics",events:[event]}))}
function sampleUtterance(samples,now){
 let sum=0;for(let i=0;i<samples.length;i++)sum+=samples[i]*samples[i];
 const rms=Math.sqrt(sum/Math.max(1,samples.length));
 if(rms>=speechRmsThreshold){
  if(!utteranceActive){utteranceSeq++;utteranceActive=true;utteranceStartPerf=now;utterancePeakRms=rms;sendUtteranceDiag({kind:"speech_start",utterance_id:utteranceSeq,at_ms:diagRound(now-clientSessionStartPerf),rms:diagRound(rms)})}
  utteranceLastVoicePerf=now;utterancePeakRms=Math.max(utterancePeakRms,rms);
 }else if(utteranceActive&&now-utteranceLastVoicePerf>=speechEndSilenceMs){
  const duration=now-utteranceStartPerf;
  if(duration>=speechMinDurationMs){const speechEndMs=diagRound(utteranceLastVoicePerf-clientSessionStartPerf);sendUtteranceDiag({kind:"speech_end",utterance_id:utteranceSeq,at_ms:speechEndMs,detected_ms:diagRound(now-clientSessionStartPerf),duration_ms:diagRound(duration),rms:diagRound(utterancePeakRms)})}
  utteranceActive=false;utteranceStartPerf=0;utteranceLastVoicePerf=0;utterancePeakRms=0;
 }return rms
}
function recordClientAudio(stats,bytes,samples,now=performance.now()){stats.frames++;stats.bytes+=bytes;stats.samples+=samples;stats.minFrameBytes=stats.minFrameBytes===null?bytes:Math.min(stats.minFrameBytes,bytes);stats.maxFrameBytes=stats.maxFrameBytes===null?bytes:Math.max(stats.maxFrameBytes,bytes);if(stats.firstAt===null)stats.firstAt=now;if(stats.lastAt!==null){const gap=Math.max(0,now-stats.lastAt);stats.intervalCount++;stats.intervalSum+=gap;stats.minInterval=stats.minInterval===null?gap:Math.min(stats.minInterval,gap);stats.maxInterval=stats.maxInterval===null?gap:Math.max(stats.maxInterval,gap)}stats.lastAt=now}
function avgClientInterval(stats){return stats.intervalCount?Math.round(stats.intervalSum/stats.intervalCount*100)/100:null}
function diagRound(v){return Number.isFinite(v)?Math.round(v*100)/100:null}
function pushPlaybackDiag(event){playbackDiagBuffer.push(event);flushPlaybackDiagnostics(false)}
function flushPlaybackDiagnostics(force=false){if(!ws||ws.readyState!==1)return;if(!force&&playbackDiagBuffer.length<16)return;while(playbackDiagBuffer.length){const batch=playbackDiagBuffer.splice(0,32);try{ws.send(JSON.stringify({type:"playback_diagnostics",events:batch}))}catch(e){playbackDiagBuffer=batch.concat(playbackDiagBuffer);return}if(!force&&playbackDiagBuffer.length<16)break}}
function interruptQueuedPlayback(reason){const now=performance.now(),ctx=playCtx?playCtx.currentTime:null;for(const n of queued){if(n.__wigvoSeq===undefined)continue;n.__wigvoInterrupted=true;pushPlaybackDiag({kind:"interrupted",seq:n.__wigvoSeq,at_ms:diagRound(now-clientSessionStartPerf),scheduled_start_ms:n.__wigvoScheduledStartMs,scheduled_end_ms:n.__wigvoScheduledEndMs,end_ctx_ms:ctx===null?null:diagRound(ctx*1000),remaining_ms:ctx===null?null:diagRound(Math.max(0,(n.__wigvoEndCtx-ctx)*1000)),reason})}}
function sendAudioDiagnostics(stage){if(!ws||ws.readyState!==1)return;const outputFirstAfterInput=clientOutputStats.firstAt!==null&&clientInputStats.firstAt!==null?Math.round((clientOutputStats.firstAt-clientInputStats.firstAt)*100)/100:null;ws.send(JSON.stringify({type:"audio_diagnostics",stage,playback_pending_frames:queued.length,mic_sample_rate:micCtx?micCtx.sampleRate:null,play_sample_rate:playCtx?playCtx.sampleRate:null,input_buffer_size:processor?processor.bufferSize:null,input_callback_count:clientInputStats.frames,input_samples:clientInputStats.samples,input_bytes:clientInputStats.bytes,input_frame_bytes_min:clientInputStats.minFrameBytes,input_frame_bytes_max:clientInputStats.maxFrameBytes,input_callback_interval_ms_avg:avgClientInterval(clientInputStats),input_callback_interval_ms_min:clientInputStats.minInterval===null?null:Math.round(clientInputStats.minInterval*100)/100,input_callback_interval_ms_max:clientInputStats.maxInterval===null?null:Math.round(clientInputStats.maxInterval*100)/100,output_audio_frames:clientOutputStats.frames,output_audio_bytes:clientOutputStats.bytes,output_frame_bytes_min:clientOutputStats.minFrameBytes,output_frame_bytes_max:clientOutputStats.maxFrameBytes,output_interval_ms_avg:avgClientInterval(clientOutputStats),output_interval_ms_min:clientOutputStats.minInterval===null?null:Math.round(clientOutputStats.minInterval*100)/100,output_interval_ms_max:clientOutputStats.maxInterval===null?null:Math.round(clientOutputStats.maxInterval*100)/100,first_output_audio_ms_from_first_input:outputFirstAfterInput}))}
const $=id=>document.getElementById(id),log=s=>{$("log").textContent+=s+"\n";$("log").scrollTop=$("log").scrollHeight},status=(s,bad=false)=>{$("status").textContent=s;$("status").className=bad?"error":""};
const cleanRoom=s=>String(s||"demo-room").toLowerCase().replace(/[^a-z0-9_-]/g,"").slice(0,40)||"demo-room";
const channel=()=>(config.agora_channel_prefix||"speakeasy-")+cleanRoom($("room").value);
function toPcm(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const v=Math.max(-1,Math.min(1,a[i]));o[i]=v<0?v*32768:v*32767}return o}
function play(b64,rate,seq,receivedAt=performance.now()){
 const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));
 recordClientAudio(clientOutputStats,bytes.byteLength,Math.floor(bytes.byteLength/2),receivedAt);
 const seqNo=Number.isFinite(Number(seq))?Number(seq):null,rxMs=diagRound(receivedAt-clientSessionStartPerf);
 const samples=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));
 // Drop only an exact, immediately repeated voiced PCM frame. Do not dedupe silence or near-matches.
 let peak=0,fingerprint=2166136261;
 for(let i=0;i<samples.length;i++){const v=Math.abs(samples[i]);if(v>peak)peak=v}
 for(let i=0;i<bytes.length;i++){fingerprint^=bytes[i];fingerprint=Math.imul(fingerprint,16777619)}
 fingerprint>>>=0;
 if(peak>512&&fingerprint===lastOutputFrameFingerprint&&receivedAt-lastOutputFrameAt<=160){
  pushPlaybackDiag({kind:"duplicate_dropped",seq:seqNo,at_ms:rxMs,rx_ms:rxMs,bytes:bytes.byteLength,reason:"exact_repeated_voiced_frame"});
  return
 }
 if(peak>512){lastOutputFrameFingerprint=fingerprint;lastOutputFrameAt=receivedAt}else{lastOutputFrameFingerprint=null;lastOutputFrameAt=receivedAt}
 if(!playCtx||!destination||myAudioMuted||!samples.length){
  pushPlaybackDiag({kind:"not_played",seq:seqNo,at_ms:rxMs,rx_ms:rxMs,bytes:bytes.byteLength,
   reason:!playCtx?"no_audio_context":!destination?"no_stream_destination":myAudioMuted?"muted":"empty_frame"});
  return
 }
 const b=playCtx.createBuffer(1,samples.length,rate),d=b.getChannelData(0);
 for(let i=0;i<samples.length;i++)d[i]=samples[i]/32768;
 const n=playCtx.createBufferSource();n.buffer=b;n.connect(destination);
 // Sample the AudioContext clock and performance clock together for this frame.
 // Keep all session timestamps in the performance.now() clock domain; context times are separate diagnostics only.
 const schedulePerfBefore=performance.now(),currentCtxTime=playCtx.currentTime,schedulePerfAfter=performance.now();
 const schedulePerf=(schedulePerfBefore+schedulePerfAfter)/2;
 const scheduledStart=Math.max(playAt,currentCtxTime+.02),scheduledEnd=scheduledStart+b.duration;
 const queueAheadMs=Math.max(0,(scheduledStart-currentCtxTime)*1000);
 const scheduledAtMs=diagRound(schedulePerf-clientSessionStartPerf);
 const scheduledStartMs=diagRound(scheduledAtMs+queueAheadMs);
 const frameMs=diagRound(b.duration*1000),scheduledEndMs=diagRound(scheduledStartMs+b.duration*1000);
 n.__wigvoSeq=seqNo;n.__wigvoEndCtx=scheduledEnd;
 n.__wigvoScheduledStartMs=scheduledStartMs;n.__wigvoScheduledEndMs=scheduledEndMs;
 n.onended=()=>{
  const callbackAt=performance.now(),callbackMs=diagRound(callbackAt-clientSessionStartPerf);
  const ctxNow=playCtx?playCtx.currentTime:null;
  if(!n.__wigvoInterrupted)pushPlaybackDiag({kind:"ended",seq:seqNo,at_ms:callbackMs,rx_ms:rxMs,
   scheduled_at_ms:scheduledAtMs,scheduled_start_ms:scheduledStartMs,scheduled_end_ms:scheduledEndMs,
   end_callback_ms:callbackMs,end_callback_ctx_ms:ctxNow===null?null:diagRound(ctxNow*1000),
   end_callback_late_ms:callbackMs===null||scheduledEndMs===null?null:diagRound(callbackMs-scheduledEndMs),frame_ms:frameMs});
  queued=queued.filter(x=>x!==n)
 };
 n.start(scheduledStart);playAt=scheduledEnd;queued.push(n);
 pushPlaybackDiag({kind:"scheduled",seq:seqNo,at_ms:scheduledAtMs,rx_ms:rxMs,
  scheduled_at_ms:scheduledAtMs,scheduled_start_ms:scheduledStartMs,scheduled_end_ms:scheduledEndMs,
  start_ctx_ms:diagRound(scheduledStart*1000),end_ctx_ms:diagRound(scheduledEnd*1000),
  queue_ahead_ms:diagRound(queueAheadMs),frame_ms:frameMs,bytes:bytes.byteLength});
}
async function loadAgora(){if(AgoraRTC)return;await new Promise((ok,no)=>{const s=document.createElement("script");s.src="https://download.agora.io/sdk/release/AgoraRTC_N-"+encodeURIComponent(config.agora_sdk_version)+".js";s.onload=ok;s.onerror=()=>no(Error("Could not load Agora Web SDK"));document.head.appendChild(s)});AgoraRTC=window.AgoraRTC}
function setRecordingStatus(message,visible=true){const el=$("recordingStatus");el.textContent=message;el.style.display=visible?"block":"none"}
function clearRecordingDownload(){const a=$("recordingDownload");if(recordingBlobUrl){URL.revokeObjectURL(recordingBlobUrl);recordingBlobUrl=null}a.removeAttribute("href");a.style.display="none"}
function addRecordingSource(key,mediaStreamTrack){
 if(!recordingCtx||!recordingDestination||!mediaStreamTrack||mediaStreamTrack.readyState!=="live"||recordingSources.has(key))return;
 try{const source=recordingCtx.createMediaStreamSource(new MediaStream([mediaStreamTrack]));source.connect(recordingDestination);recordingSources.set(key,source)}catch(e){log("Recording audio source: "+(e.message||e))}
}
function addRemoteRecordingSource(remoteTrack){
 if(!recordingCtx||!remoteTrack)return;
 try{const mediaTrack=typeof remoteTrack.getMediaStreamTrack==="function"?remoteTrack.getMediaStreamTrack():null;addRecordingSource(remoteTrack,mediaTrack)}catch(e){log("Remote recording source: "+(e.message||e))}
}
function removeRecordingSource(key){const source=recordingSources.get(key);if(source){try{source.disconnect()}catch(e){}recordingSources.delete(key)}}
async function startRecording(){
 if(!running||mediaRecorder)return;
 if(!window.MediaRecorder||!window.MediaRecorder.isTypeSupported){setRecordingStatus("This browser does not support local audio recording.");return}
 try{
  clearRecordingDownload();recordingChunks=[];recordingSources=new Map();
  recordingCtx=new (window.AudioContext||window.webkitAudioContext)({latencyHint:"interactive"});await recordingCtx.resume();
  recordingDestination=recordingCtx.createMediaStreamDestination();
  const micTrack=stream&&stream.getAudioTracks()[0];if(micTrack)addRecordingSource("local-microphone",micTrack);
  for(const remoteTrack of remoteAudioTracks)addRemoteRecordingSource(remoteTrack);
  if(!recordingSources.size)throw Error("No microphone or remote audio tracks are available to record.");
  const preferred=["audio/webm;codecs=opus","audio/webm","audio/ogg;codecs=opus"];
  const mimeType=preferred.find(t=>MediaRecorder.isTypeSupported(t));
  mediaRecorder=new MediaRecorder(recordingDestination.stream,mimeType?{mimeType}:undefined);
  mediaRecorder.ondataavailable=e=>{if(e.data&&e.data.size)recordingChunks.push(e.data)};
  mediaRecorder.onerror=e=>{setRecordingStatus("Recording error: "+(e.error&&e.error.message||"unknown error"));log("Recording error")};
  mediaRecorder.onstop=async()=>{
   const type=mediaRecorder&&mediaRecorder.mimeType||recordingChunks[0]?.type||"audio/webm";
   const blob=new Blob(recordingChunks,{type});recordingChunks=[];
   if(blob.size){
    recordingBlobUrl=URL.createObjectURL(blob);const a=$("recordingDownload");
    a.href=recordingBlobUrl;a.download="wigvo-call-"+new Date().toISOString().replace(/[:.]/g,"-")+(type.includes("ogg")?".ogg":".webm");a.textContent="Download recorded call ("+(blob.size/1024/1024).toFixed(2)+" MB)";a.style.display="inline-block";
    setRecordingStatus("Recording ready. Tap “Download recorded call” to save it, then upload that file here.");
   }else setRecordingStatus("No audio was captured. Please try recording again.");
   for(const source of recordingSources.values())try{source.disconnect()}catch(e){}
   recordingSources.clear();const ctx=recordingCtx;recordingCtx=null;recordingDestination=null;mediaRecorder=null;
   if(ctx)try{await ctx.close()}catch(e){}
   $("recordStart").disabled=!running;$("recordStop").disabled=true;
  };
  recordingStartedAt=Date.now();mediaRecorder.start(250);$("recordStart").disabled=true;$("recordStop").disabled=false;
  setRecordingStatus("Recording microphone + remote audio. Stop recording to prepare the download.");
  log("Local audio recording started (microphone + remote participant)");
 }catch(e){
  if(mediaRecorder&&mediaRecorder.state!=="inactive")try{mediaRecorder.stop()}catch(_){}
  if(recordingCtx)try{await recordingCtx.close()}catch(_){}
  recordingCtx=null;recordingDestination=null;recordingSources.clear();mediaRecorder=null;
  $("recordStart").disabled=!running;$("recordStop").disabled=true;setRecordingStatus("Could not start recording: "+(e.message||String(e)));log("Recording start failed: "+(e.message||String(e)))
 }
}
function stopRecording(){
 if(!mediaRecorder){$("recordStop").disabled=true;return}
 $("recordStop").disabled=true;$("recordStart").disabled=!running;
 if(mediaRecorder.state!=="inactive")mediaRecorder.stop();
}
function updateAudioButtons(){const live=running;for(const id of ["micMute","micPause","myAudioMute","otherSpeakerMute"])$(id).disabled=!live;$("recordStart").disabled=!live||!!mediaRecorder;$("recordStop").disabled=!mediaRecorder;$("micMute").textContent=micMuted?"Unmute microphone":"Mute microphone";$("micMute").setAttribute("aria-pressed",String(micMuted));$("micPause").textContent=micPaused?"Resume microphone":"Pause microphone";$("micPause").setAttribute("aria-pressed",String(micPaused));$("myAudioMute").textContent=myAudioMuted?"Unmute translated audio":"Mute translated audio";$("myAudioMute").setAttribute("aria-pressed",String(myAudioMuted));$("otherSpeakerMute").textContent=otherSpeakerMuted?"Unmute other speaker":"Mute other speaker";$("otherSpeakerMute").setAttribute("aria-pressed",String(otherSpeakerMuted))}function setRemoteSpeakerMuted(muted){otherSpeakerMuted=muted;for(const t of remoteAudioTracks){try{t.setVolume(muted?0:100)}catch(e){}}updateAudioButtons()}async function stopCall(){if(mediaRecorder)stopRecording();running=false;starting=false;micMuted=false;micPaused=false;myAudioMuted=false;otherSpeakerMuted=false;remoteAudioTracks.clear();interruptQueuedPlayback("session_stop");if(ws){try{if(ws.readyState===1){flushPlaybackDiagnostics(true);sendAudioDiagnostics("audio_summary");ws.send(JSON.stringify({type:"stop"}))}ws.close()}catch(e){}}ws=null;micPendingSamples=new Float32Array(0);if(processor)try{processor.disconnect()}catch(e){}if(sourceNode)try{sourceNode.disconnect()}catch(e){}if(silent)try{silent.disconnect()}catch(e){}processor=sourceNode=silent=null;queued.forEach(n=>{try{n.stop()}catch(e){}});queued=[];const c=client;client=null;if(track){try{if(c&&c.connectionState==="CONNECTED")await c.unpublish([track])}catch(e){}try{track.close()}catch(e){}track=null}if(c)try{if(c.connectionState!=="DISCONNECTED")await c.leave()}catch(e){}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(micCtx)try{await micCtx.close()}catch(e){}if(playCtx)try{await playCtx.close()}catch(e){}micCtx=playCtx=null;destination=null;$("start").disabled=false;$("stop").disabled=true;updateAudioButtons();status("Ready")}
async function startCall(){if(starting||running)return;clientInputStats=makeAudioStats();clientOutputStats=makeAudioStats();playbackDiagBuffer=[];lastOutputFrameFingerprint=null;lastOutputFrameAt=0;micPendingSamples=new Float32Array(0);clientSessionStartPerf=0;playAt=0;queued=[];starting=true;$("start").disabled=true;status("Connecting…");let c=null,resolveReady,rejectReady;
try{await loadAgora();c=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});client=c;const sub=async u=>{if(!u||c!==client||c.connectionState!=="CONNECTED"||!u.hasAudio)return;try{await c.subscribe(u,"audio");if(u.audioTrack){remoteAudioTracks.add(u.audioTrack);addRemoteRecordingSource(u.audioTrack);u.audioTrack.setVolume(otherSpeakerMuted?0:100);u.audioTrack.play()}}catch(e){log("Remote audio: "+(e.message||e))}};c.on("user-published",async(u,t)=>{if(t==="audio")await sub(u)});c.on("user-unpublished",(u,t)=>{if(t==="audio"&&u.audioTrack){removeRecordingSource(u.audioTrack);remoteAudioTracks.delete(u.audioTrack)}});c.on("user-left",u=>{if(u.audioTrack){removeRecordingSource(u.audioTrack);remoteAudioTracks.delete(u.audioTrack)}});const uid=Math.floor(100000+Math.random()*900000),room=channel();const r=await fetch("/api/agora-token?channel="+encodeURIComponent(room)+"&uid="+uid,{cache:"no-store"}),j=await r.json();if(!r.ok||!j.token)throw Error(j.detail||"Could not obtain Agora token");await c.join(config.agora_app_id,room,j.token,uid);stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/api/browser");clientSessionStartPerf=performance.now();ws.onmessage=e=>{const receivedAt=performance.now();try{const d=JSON.parse(e.data);if(d.type==="ready"){log("Krisp ready: "+d.source_language+" → "+d.target_language);if(resolveReady){resolveReady();resolveReady=null}}else if(d.type==="error"){log("["+d.stage+"] "+d.message);status(d.message,true);if(rejectReady){rejectReady(Error(d.message));rejectReady=null}}else if(d.type==="audio")play(d.audio,d.sample_rate||16000,d.seq,receivedAt);else if(d.type==="transcript"&&d.text)$("original").textContent=d.text;else if(d.type==="translate"&&d.text)$("translated").textContent=d.text}catch(err){log(err.message||err)}};
await new Promise((ok,no)=>{ws.onopen=ok;ws.onerror=()=>no(Error("Could not connect to WIGVO voice pipeline"))});const ready=new Promise((ok,no)=>{resolveReady=ok;rejectReady=no;setTimeout(()=>no(Error("Krisp startup timed out")),50000)});ws.send(JSON.stringify({source_language:$("src").value,target_language:$("target").value,voice:$("voice").value}));await ready;
playCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000,latencyHint:"interactive"});await playCtx.resume();destination=playCtx.createMediaStreamDestination();micCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000,latencyHint:"interactive"});await micCtx.resume();sourceNode=micCtx.createMediaStreamSource(stream);processor=micCtx.createScriptProcessor(256,1,1);silent=micCtx.createGain();silent.gain.value=0;sourceNode.connect(processor);processor.connect(silent);silent.connect(micCtx.destination);processor.onaudioprocess=e=>{if(!ws||ws.readyState!==1)return;try{const samples=e.inputBuffer.getChannelData(0),now=performance.now();if(!micPaused)sampleUtterance(samples,now);const inputSamples=micPaused?new Float32Array(samples.length):samples;const merged=new Float32Array(micPendingSamples.length+inputSamples.length);merged.set(micPendingSamples);merged.set(inputSamples,micPendingSamples.length);let offset=0;while(merged.length-offset>=320){const pcm=micPaused?new Int16Array(320):toPcm(merged.subarray(offset,offset+320));recordClientAudio(clientInputStats,pcm.byteLength,320,now);ws.send(pcm.buffer);offset+=320}micPendingSamples=merged.slice(offset)}catch(err){}};
sendAudioDiagnostics("audio_context");
track=AgoraRTC.createCustomAudioTrack({mediaStreamTrack:destination.stream.getAudioTracks()[0],encoderConfig:"speech_low_quality"});await c.publish([track]);for(const u of c.remoteUsers||[])await sub(u);running=true;starting=false;$("uid").textContent="UID "+uid;$("stop").disabled=false;updateAudioButtons();status("Live translation connected");log("Joined "+room+"; translated audio only is published to Agora")
}catch(e){log(e.message||String(e));status(e.message||String(e),true);starting=false;if(ws){try{ws.close()}catch(_){}ws=null}if(c===client){client=null;try{if(c.connectionState!=="DISCONNECTED")await c.leave()}catch(_){}}if(track){try{track.close()}catch(_){}track=null}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(micCtx)try{await micCtx.close()}catch(_){}if(playCtx)try{await playCtx.close()}catch(_){}micCtx=playCtx=null;$("start").disabled=false;$("stop").disabled=true;running=false}}
$("src").addEventListener("change",()=>{const src=$("src").value;const target=$("target");if(src===target.value)target.value=src==="en-US"?"pt-BR":"en-US"});$("target").addEventListener("change",()=>{if($("src").value===$("target").value){log("Source and target languages must be different.");$("target").value=$("src").value==="en-US"?"pt-BR":"en-US"}});async function boot(){try{const r=await fetch("/api/config",{cache:"no-store"});config=await r.json();if(!config.agora_app_id)log("AGORA_APP_ID is not configured on Render.");if(!config.krisp_configured)log("KRISP_API_KEY must be added to the Render service.");else log("Krisp Voice Translation configured")}catch(e){log("Config error: "+e.message)}}
$("start").onclick=startCall;$("stop").onclick=stopCall;$("recordStart").onclick=startRecording;$("recordStop").onclick=stopRecording;$("micMute").onclick=()=>{if(!running)return;micMuted=!micMuted;if(stream)stream.getAudioTracks().forEach(t=>t.enabled=!micMuted);updateAudioButtons();status(micMuted?"Microphone muted":"Microphone unmuted")};$("micPause").onclick=()=>{if(!running)return;micPaused=!micPaused;micPendingSamples=new Float32Array(0);updateAudioButtons();status(micPaused?"Microphone paused — sending silence to keep translation synchronized":"Microphone resumed")};$("myAudioMute").onclick=()=>{if(!running)return;myAudioMuted=!myAudioMuted;if(destination)destination.stream.getAudioTracks().forEach(t=>t.enabled=!myAudioMuted);if(myAudioMuted){interruptQueuedPlayback("muted");flushPlaybackDiagnostics(true);queued.forEach(n=>{try{n.stop()}catch(e){}});queued=[];playAt=playCtx?playCtx.currentTime:0}updateAudioButtons();status(myAudioMuted?"Your translated audio muted for other participants":"Your translated audio unmuted")};$("otherSpeakerMute").onclick=()=>{if(!running)return;setRemoteSpeakerMuted(!otherSpeakerMuted);status(otherSpeakerMuted?"Other speaker muted on this device":"Other speaker unmuted")};window.addEventListener("beforeunload",()=>{if(ws&&ws.readyState===1)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}});boot();
</script></body></html>'''


def register_browser_route(app: Any) -> None:
    app.websocket("/api/browser")(browser_translation)


def register_browser_home(app: Any) -> None:
    from fastapi.responses import HTMLResponse, JSONResponse
    headers = {"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"}

    @app.get("/", response_class=HTMLResponse)
    async def browser_root() -> HTMLResponse:
        return HTMLResponse(BROWSER_HTML_PAGE, headers=headers)

    @app.get("/browser", response_class=HTMLResponse)
    async def browser_home() -> HTMLResponse:
        return HTMLResponse(BROWSER_HTML_PAGE, headers=headers)

    @app.get("/api/config", response_class=JSONResponse)
    async def browser_config() -> JSONResponse:
        return JSONResponse({
            "transport": "agora_krisp_vt",
            "agora_app_id": AGORA_APP_ID,
            "agora_sdk_version": AGORA_SDK_VERSION,
            "agora_channel_prefix": AGORA_CHANNEL_PREFIX,
            "krisp_configured": bool(KRISP_API_KEY),
        }, headers=headers)
