from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from uuid import uuid4
from typing import Any

import websockets
from fastapi import WebSocket, WebSocketDisconnect

SONIOX_API_KEY = os.getenv("SONIOX_API_KEY", "")
SONIOX_STT_URL = os.getenv("SONIOX_STT_URL", "wss://stt-rt.soniox.com/transcribe-websocket")
SONIOX_TTS_URL = os.getenv("SONIOX_TTS_URL", "wss://tts-rt.soniox.com/tts-websocket")
SONIOX_STT_MODEL = os.getenv("SONIOX_STT_MODEL", "stt-rt-v5")
SONIOX_TTS_MODEL = os.getenv("SONIOX_TTS_MODEL", "tts-rt-v2")
SONIOX_ENDPOINT_DELAY_MS = max(500, min(3000, int(os.getenv("SONIOX_ENDPOINT_DELAY_MS", "500"))))
SONIOX_TTS_VOICE_A = os.getenv("SONIOX_TTS_VOICE_A", "Maya")
SONIOX_TTS_VOICE_B = os.getenv("SONIOX_TTS_VOICE_B", "Adrian")

async def _send_tts_chunk(tts_ws: Any, state: dict[str, Any], text: str, final: bool = False) -> None:
    if not text and not final:
        return
    stream_id = state.get("stream_id")
    if not stream_id:
        state["seq"] = int(state.get("seq", 0)) + 1
        stream_id = f"browser-{state['seq']}-{uuid4().hex[:8]}"
        state["stream_id"] = stream_id
        await tts_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_TTS_MODEL,
            "language": state["target_language"],
            "voice": state["voice"],
            "audio_format": "pcm_s16le",
            "sample_rate": 24000,
            "stream_id": stream_id,
            "speed": 1.0,
        }))
    await tts_ws.send(json.dumps({
        "stream_id": stream_id,
        "text": text,
        "text_end": final,
    }))
    if final:
        state["stream_id"] = None

async def browser_translation(ws: WebSocket) -> None:
    await ws.accept()

    if not SONIOX_API_KEY:
        await ws.send_json({"type": "error", "stage": "config", "message": "SONIOX_API_KEY is not configured on Render."})
        await ws.close(code=1011)
        return

    stt_ws = None
    tts_ws = None
    stt_task = None
    tts_task = None
    started = time.monotonic()

    try:
        cfg = await ws.receive_json()
        language_a = str(cfg.get("language_a", "en")).lower()
        language_b = str(cfg.get("language_b", "pt")).lower()
        direction = str(cfg.get("direction", "a_to_b"))

        if direction == "b_to_a":
            source_language, target_language, voice = language_b, language_a, SONIOX_TTS_VOICE_A
        else:
            source_language, target_language, voice = language_a, language_b, SONIOX_TTS_VOICE_B

        stt_ws = await websockets.connect(SONIOX_STT_URL, ping_interval=20, ping_timeout=20, close_timeout=5)
        await stt_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_STT_MODEL,
            "audio_format": "pcm_s16le",
            "sample_rate": 16000,
            "num_channels": 1,
            "language_hints": [source_language],
            "language_hints_strict": True,
            "enable_endpoint_detection": True,
            "max_endpoint_delay_ms": SONIOX_ENDPOINT_DELAY_MS,
            "translation": {"type": "one_way", "target_language": target_language},
        }))

        tts_ws = await websockets.connect(SONIOX_TTS_URL, ping_interval=20, ping_timeout=20, close_timeout=5)
        tts_state = {"stream_id": None, "seq": 0, "target_language": target_language, "voice": voice}
        pending_translation = ""
        transcript_parts = []

        await ws.send_json({
            "type": "ready",
            "source_language": source_language,
            "target_language": target_language,
        })

        async def read_stt() -> None:
            nonlocal pending_translation
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

                    for token in data.get("tokens") or []:
                        text = str(token.get("text") or "")
                        status = token.get("translation_status")
                        final = bool(token.get("is_final"))

                        if text == "<end>":
                            if pending_translation.strip():
                                await _send_tts_chunk(tts_ws, tts_state, pending_translation, True)
                                pending_translation = ""
                            await ws.send_json({"type": "utterance_end"})
                            transcript_parts.clear()
                            continue

                        if not text:
                            continue

                        if status in (None, "none", "original"):
                            transcript_parts.append(text)
                            await ws.send_json({
                                "type": "transcript",
                                "text": text,
                                "final": final,
                            })
                            if final:
                                await ws.send_json({"type": "speech_activity"})

                        elif status == "translation":
                            pending_translation += text
                            # Send complete-ish translated chunks while the speaker is still talking.
                            candidate = pending_translation
                            boundary = max(candidate.rfind(" "), candidate.rfind(","), candidate.rfind("."), candidate.rfind("?"), candidate.rfind("!"))
                            if len(candidate) >= 18 and boundary >= 8:
                                chunk = candidate[:boundary + 1]
                                pending_translation = candidate[boundary + 1:]
                                await _send_tts_chunk(tts_ws, tts_state, chunk, False)
                                await ws.send_json({"type": "translation_text", "text": chunk})

                    if data.get("finished"):
                        break
            except asyncio.CancelledError:
                pass

        async def read_tts() -> None:
            try:
                async for raw in tts_ws:
                    data = json.loads(raw)
                    if data.get("error_code") is not None:
                        await ws.send_json({
                            "type": "error",
                            "stage": "tts",
                            "message": data.get("error_message", "Soniox TTS error"),
                        })
                        continue
                    audio = data.get("audio")
                    if audio:
                        await ws.send_json({
                            "type": "audio",
                            "audio": audio,
                            "sample_rate": 24000,
                        })
                    if data.get("terminated"):
                        continue
            except asyncio.CancelledError:
                pass

        stt_task = asyncio.create_task(read_stt())
        tts_task = asyncio.create_task(read_tts())

        while True:
            if time.monotonic() - started > 600:
                await ws.send_json({"type": "info", "message": "Maximum browser test session reached."})
                break

            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break

            if msg.get("bytes") is not None:
                audio = msg["bytes"]
                if audio:
                    await stt_ws.send(audio)

            elif msg.get("text") is not None:
                try:
                    command = json.loads(msg["text"])
                except json.JSONDecodeError:
                    command = {}

                if command.get("type") == "stop":
                    break

                if command.get("type") == "barge_in":
                    # Stop currently queued translated speech at the browser immediately.
                    # TTS streams are short and will finish/cancel naturally.
                    await ws.send_json({"type": "clear_audio"})

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "stage": "server", "message": str(exc)})
        except Exception:
            pass
    finally:
        for task in (stt_task, tts_task):
            if task:
                task.cancel()
        for soniox_ws in (stt_ws, tts_ws):
            if soniox_ws:
                try:
                    await soniox_ws.close()
                except Exception:
                    pass
        try:
            await ws.close()
        except Exception:
            pass

def register_browser_route(app: Any) -> None:
    app.websocket("/api/browser")(browser_translation)

BROWSER_HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SpeakEasy Browser Translation Test</title>
<style>
*{box-sizing:border-box}body{margin:0;padding:14px;background:#080d18;color:#eef2ff;font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:720px;margin:auto}.card{background:#151e33;border:1px solid #2d3b5d;border-radius:20px;padding:18px;margin:12px 0}
h1{margin:0 0 5px;font-size:27px}.muted{color:#9daaca;font-size:14px}.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
label{display:block;font-size:13px;color:#aeb9d2;margin-bottom:5px}select,button{width:100%;padding:13px;border-radius:11px;border:1px solid #40527a;background:#0d1527;color:#fff;font-size:15px}
button{font-weight:700}.primary{background:#315fe9;border-color:#315fe9}.stop{background:#6b2940;border-color:#6b2940}
button:disabled{opacity:.45}.box{background:#0b1324;border-radius:12px;padding:13px;min-height:58px;margin-top:7px;line-height:1.5}
#translated{font-size:20px}.meter{height:7px;background:#0c1425;border-radius:20px;overflow:hidden;margin:12px 0}.meter i{display:block;height:100%;width:0;background:#6f8fff}
#log{font:12px ui-monospace,SFMono-Regular,Menlo,monospace;color:#8f9dbb;max-height:130px;overflow:auto;white-space:pre-wrap}
@media(max-width:600px){.row{grid-template-columns:1fr}body{padding:8px}.card{padding:15px}}
</style>
</head>
<body>
<main>
<div class="card"><h1>SpeakEasy</h1><div class="muted">Real-time speech-to-speech browser test — no Twilio required</div></div>

<div class="card">
<div class="row">
<div><label>Language A</label><select id="la"><option value="en" selected>English</option><option value="pt">Português</option><option value="es">Español</option><option value="fr">Français</option></select></div>
<div><label>Language B</label><select id="lb"><option value="pt" selected>Português</option><option value="en">English</option><option value="es">Español</option><option value="fr">Français</option></select></div>
</div>
<div style="margin-top:12px"><label>Direction</label><select id="dir"><option value="a_to_b">A → B (English → Portuguese)</option><option value="b_to_a">B → A (Portuguese → English)</option></select></div>
<div class="row" style="margin-top:12px"><button id="start" class="primary">Start microphone</button><button id="stop" class="stop" disabled>Stop</button></div>
<div class="meter"><i id="meter"></i></div>
<div id="status" style="font-weight:700">Ready</div>
</div>

<div class="card"><div class="muted">Original speech</div><div id="original" class="box">—</div><div class="muted" style="margin-top:13px">Translated speech</div><div id="translated" class="box">—</div></div>
<div class="card"><div class="muted">Session log</div><div id="log"></div></div>
</main>

<script>
let mediaStream=null,ctx=null,source=null,processor=null,ws=null,running=false,playAt=0,nodes=[];
const $=x=>document.getElementById(x);
function log(x){$("log").textContent+=x+"\\n";$("log").scrollTop=$("log").scrollHeight}
function status(x){$("status").textContent=x}
function downsample(a,inRate,outRate){if(inRate===outRate)return a;const ratio=inRate/outRate,len=Math.round(a.length/ratio),o=new Float32Array(len);let off=0;for(let i=0;i<len;i++){const next=Math.round((i+1)*ratio);let sum=0,n=0;for(let j=off;j<next&&j<a.length;j++){sum+=a[j];n++}o[i]=n?sum/n:0;off=next}return o}
function pcm16(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const s=Math.max(-1,Math.min(1,a[i]));o[i]=s<0?s*32768:s*32767}return o}
function clearAudio(){nodes.forEach(n=>{try{n.stop()}catch(e){}});nodes=[];if(ctx)playAt=ctx.currentTime+.02}
function play(b64,rate){if(!ctx)return;const raw=Uint8Array.from(atob(b64),c=>c.charCodeAt(0)),p=new Int16Array(raw.buffer),buf=ctx.createBuffer(1,p.length,rate),ch=buf.getChannelData(0);for(let i=0;i<p.length;i++)ch[i]=p[i]/32768;const n=ctx.createBufferSource();n.buffer=buf;n.connect(ctx.destination);playAt=Math.max(playAt,ctx.currentTime+.01);n.start(playAt);playAt+=buf.duration;nodes.push(n);n.onended=()=>nodes=nodes.filter(x=>x!==n)}
async function microphone(){if(mediaStream)return;mediaStream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true}})}
function stop(send=true){running=false;if(processor){try{processor.disconnect()}catch(e){}processor=null}if(source){try{source.disconnect()}catch(e){}source=null}clearAudio();if(ws){if(send&&ws.readyState===1)ws.send(JSON.stringify({type:"stop"}));try{ws.close()}catch(e){}ws=null}$("start").disabled=false;$("stop").disabled=true;$("meter").style.width="0";status("Stopped")}
$("start").onclick=async()=>{try{await microphone();ctx=new(window.AudioContext||window.webkitAudioContext)();await ctx.resume();ws=new WebSocket((location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/api/browser");ws.binaryType="arraybuffer";ws.onopen=()=>{ws.send(JSON.stringify({type:"start",language_a:$("la").value,language_b:$("lb").value,direction:$("dir").value}));source=ctx.createMediaStreamSource(mediaStream);processor=ctx.createScriptProcessor(4096,1,1);processor.onaudioprocess=e=>{if(!running||!ws||ws.readyState!==1)return;const input=e.inputBuffer.getChannelData(0),samples=downsample(input,ctx.sampleRate,16000),p=pcm16(samples);ws.send(p.buffer);let peak=0;for(let i=0;i<input.length;i++)peak=Math.max(peak,Math.abs(input[i]));$("meter").style.width=Math.min(100,peak*170)+"%"};source.connect(processor);processor.connect(ctx.destination);running=true;$("start").disabled=true;$("stop").disabled=false;status("Connecting…")};ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==="ready"){status(d.source_language.toUpperCase()+" → "+d.target_language.toUpperCase()+" — listening")}else if(d.type==="transcript"){if($("original").textContent==="—")$("original").textContent="";$("original").textContent+=d.text}else if(d.type==="translation_text"){if($("translated").textContent==="—")$("translated").textContent="";$("translated").textContent+=d.text}else if(d.type==="audio"){play(d.audio,d.sample_rate||24000);status("Playing translation…")}else if(d.type==="utterance_end"){status("Listening…")}else if(d.type==="clear_audio"){clearAudio()}else if(d.type==="error"){log("ERROR ["+d.stage+"] "+d.message);status("Error — see log")}else if(d.type==="info"){log(d.message)}};ws.onerror=()=>{status("Connection error");log("WebSocket connection error")};ws.onclose=()=>{if(running)stop(false)}}catch(e){status("Microphone error");log(e.message)}}
$("stop").onclick=()=>stop(true);
window.addEventListener("beforeunload",()=>stop(true));
</script>
</body>
</html>"""

def register_browser_home(app: Any) -> None:
    from fastapi.responses import HTMLResponse
    @app.get("/browser", response_class=HTMLResponse)
    async def browser_home() -> str:
        return BROWSER_HTML_PAGE
