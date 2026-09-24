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
SONIOX_ENDPOINT_DELAY_MS = max(300, min(1500, int(os.getenv("SONIOX_ENDPOINT_DELAY_MS", "500"))))
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


async def send_tts(tts_ws: Any, state: dict[str, Any], text: str) -> None:
    text = text.strip()
    if not text or tts_ws is None:
        return
    async with state["send_lock"]:
        state["seq"] += 1
        sid = f"browser-{state['seq']}-{uuid4().hex[:8]}"
        await tts_ws.send(json.dumps({
            "api_key": SONIOX_API_KEY,
            "model": SONIOX_TTS_MODEL,
            "language": state["target"],
            "voice": state["voice"],
            "audio_format": "pcm_s16le",
            "sample_rate": 24000,
            "stream_id": sid,
        }))
        await tts_ws.send(json.dumps({
            "stream_id": sid,
            "text": text,
            "text_end": True,
        }))


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
    tts_tasks: set[asyncio.Task] = set()
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
            "language_hints_strict": True,
            "enable_endpoint_detection": True,
            "endpoint_latency_adjustment_level": 2,
            "endpoint_sensitivity": 0.3,
            "max_endpoint_delay_ms": SONIOX_ENDPOINT_DELAY_MS,
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
            "spoken_chars": 0,
            "send_lock": asyncio.Lock(),
        }
        final_original = ""
        speech_started = False

        await ws.send_json({
            "type": "ready",
            "source_language": source,
            "target_language": target,
        })

        def emit_final_translation(force: bool = False) -> str:
            candidate = state["final_translation"]
            spoken = int(state["spoken_chars"])
            if len(candidate) <= spoken:
                return ""

            delta = candidate[spoken:]
            boundary = max(
                delta.rfind(" "),
                delta.rfind(","),
                delta.rfind("."),
                delta.rfind("?"),
                delta.rfind("!"),
                delta.rfind(":"),
                delta.rfind(";"),
            )

            if force and boundary < 0:
                chunk = delta.strip()
                if not chunk:
                    return ""
                state["spoken_chars"] = len(candidate)
            elif boundary >= 3:
                cut = boundary + 1
                chunk = delta[:cut].strip()
                if not chunk or (not force and len(chunk) < 6):
                    return ""
                state["spoken_chars"] = spoken + cut
            else:
                return ""

            state["translation_seq"] += 1
            task = asyncio.create_task(send_tts(tts_ws, state, chunk))
            tts_tasks.add(task)
            task.add_done_callback(tts_tasks.discard)
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
                    chunks: list[tuple[str, int]] = []

                    for token in data.get("tokens") or []:
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
                                    await ws.send_json({"type": "speech_start"})
                        elif status == "translation":
                            if is_final:
                                state["final_translation"] += token_text
                                chunk = emit_final_translation(False)
                                if chunk:
                                    chunks.append((chunk, state["translation_seq"]))
                            else:
                                preview_translation.append(token_text)

                    current_original = (
                        final_original + "".join(preview_original)
                    ).strip()
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

                    for chunk, seq in chunks:
                        await ws.send_json({
                            "type": "translation_text",
                            "text": chunk,
                            "final": True,
                            "chunk_id": f"tr-{seq}",
                        })

                    if endpoint:
                        chunk = emit_final_translation(True)
                        if chunk:
                            await ws.send_json({
                                "type": "translation_text",
                                "text": chunk,
                                "final": True,
                                "chunk_id": f"tr-{state['translation_seq']}",
                            })

                        await ws.send_json({
                            "type": "transcript",
                            "text": final_original.strip(),
                            "final": True,
                        })
                        await ws.send_json({"type": "utterance_end"})

                        final_original = ""
                        speech_started = False
                        state["final_translation"] = ""
                        state["spoken_chars"] = 0

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
                        continue

                    audio = data.get("audio")
                    if audio:
                        await ws.send_json({
                            "type": "audio",
                            "audio": audio,
                            "sample_rate": 24000,
                            "stream_id": sid,
                        })
            except asyncio.CancelledError:
                pass

        tasks.extend([
            asyncio.create_task(read_stt()),
            asyncio.create_task(read_tts()),
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
        for task in tuple(tts_tasks):
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
<div class="card"><div class="muted">Session log</div><div id="log" class="log"></div></div>
</main>
<script>
let AgoraRTC=null,client=null,localTrack=null,stream=null,ctx=null,micSource=null,processor=null,silent=null,outDestination=null,ws=null,running=false,playAt=0,staged=[],config=null;
const $=id=>document.getElementById(id);
const write=x=>{$("log").textContent+=String(x)+"\n";$("log").scrollTop=$("log").scrollHeight};
const setStatus=(x,err=false)=>{$("status").textContent=x;$("status").className=err?"error":""};
const roomName=v=>{const x=String(v||"").toLowerCase().replace(/[^a-z0-9_-]/g,"").slice(0,40);return x||"demo-room"};
const channel=v=>(config.agora_channel_prefix||"speakeasy-")+roomName(v);
function down(a,from,to){if(from===to)return a;const r=from/to,n=Math.max(1,Math.round(a.length/r)),o=new Float32Array(n);let p=0;for(let i=0;i<n;i++){const q=Math.min(a.length,Math.round((i+1)*r));let s=0,c=0;for(let j=p;j<q;j++){s+=a[j];c++}o[i]=c?s/c:0;p=q}return o}
function pcm(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const s=Math.max(-1,Math.min(1,a[i]));o[i]=s<0?s*32768:s*32767}return o}
function clearQueue(){staged.forEach(n=>{try{n.stop()}catch(e){}});staged=[];if(ctx)playAt=ctx.currentTime+.02}
function playTranslated(b64,rate){if(!ctx||!outDestination)return;const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));const s=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));const b=ctx.createBuffer(1,s.length,rate);const ch=b.getChannelData(0);for(let i=0;i<s.length;i++)ch[i]=s[i]/32768;const n=ctx.createBufferSource();n.buffer=b;n.connect(outDestination);playAt=Math.max(playAt,ctx.currentTime+.01);n.start(playAt);playAt+=b.duration;staged.push(n);n.onended=()=>staged=staged.filter(x=>x!==n);$("orb").classList.add("speaking")}
async function loadAgora(){if(AgoraRTC)return;await new Promise((ok,bad)=>{const s=document.createElement("script");s.src="https://download.agora.io/sdk/release/AgoraRTC_N-"+encodeURIComponent(config.agora_sdk_version)+".js";s.onload=ok;s.onerror=()=>bad(new Error("Could not load Agora Web SDK"));document.head.appendChild(s)});AgoraRTC=window.AgoraRTC}
async function stopCall(){running=false;if(ws)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}if(ws)try{ws.close()}catch(e){}ws=null;if(processor)try{processor.disconnect()}catch(e){}if(micSource)try{micSource.disconnect()}catch(e){}if(silent)try{silent.disconnect()}catch(e){}processor=micSource=silent=null;clearQueue();if(localTrack){try{await client?.unpublish([localTrack])}catch(e){}try{localTrack.close()}catch(e){}localTrack=null}if(client){try{await client.leave()}catch(e){}client=null}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(ctx){try{await ctx.close()}catch(e){}ctx=null}outDestination=null;$("start").disabled=false;$("stop").disabled=true;$("dir").textContent="Not connected";$("roomline").textContent="Choose languages and start";$("orb").classList.remove("speaking");setStatus("Ready")}
async function startCall(){if(running)return;try{const src=$("src").value,tgt=$("tgt").value;if(src===tgt){setStatus("Choose different languages",true);return}if(!config.agora_app_id)throw new Error("AGORA_APP_ID is not configured on Render");await loadAgora();stream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true}});ctx=new(window.AudioContext||window.webkitAudioContext)({sampleRate:24000});await ctx.resume();outDestination=ctx.createMediaStreamDestination();const translatedTrack=outDestination.stream.getAudioTracks()[0];client=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});client.on("user-published",async(user,type)=>{if(type!=="audio")return;try{await client.subscribe(user,"audio");user.audioTrack?.play()}catch(e){write("Remote audio error: "+(e?.message||e))}});client.on("user-unpublished",(u,type)=>{if(type==="audio")write("Remote translated audio stopped")});const uid=Math.floor(100000+Math.random()*900000);const channelName=channel($("room").value);const tokenResponse=await fetch("/api/agora-token?channel="+encodeURIComponent(channelName)+"&uid="+uid,{cache:"no-store"});let tokenJson={};try{tokenJson=await tokenResponse.json()}catch(e){}if(!tokenResponse.ok||!tokenJson.token)throw new Error(tokenJson.detail||"Could not obtain Agora RTC token");await client.join(config.agora_app_id,channelName,tokenJson.token,uid);localTrack=AgoraRTC.createCustomAudioTrack({mediaStreamTrack:translatedTrack});await client.publish([localTrack]);$("uid").textContent="UID "+uid;$("dir").textContent=src.toUpperCase()+" → "+tgt.toUpperCase();$("roomline").textContent="Room: "+roomName($("room").value);setStatus("Connected — listening");write("Agora joined "+channelName+" with a short-lived RTC token");ws=new WebSocket((location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/api/browser");ws.onopen=()=>{ws.send(JSON.stringify({type:"start",language:src,target_language:tgt}));micSource=ctx.createMediaStreamSource(stream);processor=ctx.createScriptProcessor(4096,1,1);processor.onaudioprocess=e=>{if(!running||!ws||ws.readyState!==1)return;const input=e.inputBuffer.getChannelData(0),p=pcm(down(input,ctx.sampleRate,16000));if(ws.bufferedAmount<262144)ws.send(p.buffer);let peak=0;for(const v of input)peak=Math.max(peak,Math.abs(v));$("meter").style.width=Math.min(100,peak*170)+"%"};micSource.connect(processor);silent=ctx.createGain();silent.gain.value=0;processor.connect(silent);silent.connect(ctx.destination);running=true;$("start").disabled=true;$("stop").disabled=false};ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==="ready")setStatus("Call connected — listening");else if(d.type==="speech_start"){$("orb").classList.add("speaking");setStatus("Speaking…")}else if(d.type==="transcript"){const t=String(d.text||"").trim();if(t)$("original").textContent=t}else if(d.type==="translation_text"){const t=String(d.text||"").trim();if(t)$("translated").textContent=(($("translated").textContent==="—"?"":$("translated").textContent+" ")+t).trim()}else if(d.type==="audio")playTranslated(d.audio,d.sample_rate||24000);else if(d.type==="utterance_end"){$("orb").classList.remove("speaking");setStatus("Call connected — listening")}else if(d.type==="error"){write("ERROR ["+d.stage+"] "+d.message);setStatus("Error — see log",true)}else if(d.type==="info"){write(d.message)}};ws.onerror=()=>{write("Translation WebSocket error");setStatus("Translation connection error",true)};ws.onclose=()=>{if(running)stopCall()}}catch(e){write(e?.message||String(e));setStatus(e?.message||"Could not start call",true);await stopCall()}}
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
