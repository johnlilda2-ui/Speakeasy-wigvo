from __future__ import annotations

import asyncio
import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any

import websockets
from fastapi import WebSocket, WebSocketDisconnect

KRISP_API_KEY = os.getenv("KRISP_API_KEY", "").strip()
KRISP_TOKEN_URL = "https://api.developers.krisp.ai/v2/sdk/tokens/session"
KRISP_VT_TOKEN_URL = "https://api.developers.krisp.ai/v2/sdk/voice-translation/session/token"
KRISP_VT_URL = "wss://streaming.krisp.ai/vt"
AGORA_APP_ID = os.getenv("AGORA_APP_ID", "").strip()
AGORA_SDK_VERSION = os.getenv("AGORA_SDK_VERSION", "4.24.8").strip()
AGORA_CHANNEL_PREFIX = os.getenv("AGORA_CHANNEL_PREFIX", "speakeasy-").strip() or "speakeasy-"


def _create_session_token() -> str:
    if not KRISP_API_KEY:
        raise RuntimeError("KRISP_API_KEY is not configured on Render.")
    body = json.dumps({"usage_type": "vt", "expiration_ttl": 60}).encode()
    headers = {"Authorization": f"api-key {KRISP_API_KEY}", "Content-Type": "application/json"}
    attempts = [
        urllib.request.Request(KRISP_VT_TOKEN_URL, method="GET", headers={"Authorization": headers["Authorization"]}),
        urllib.request.Request(KRISP_TOKEN_URL, data=body, method="POST", headers=headers),
    ]
    last_error = "Krisp did not return a session token."
    for req in attempts:
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                result = json.loads(response.read().decode())
            token = ((result.get("data") or {}).get("session_key") or "").strip()
            if token:
                return token
            last_error = str(result.get("message") or result.get("error_code") or last_error)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            last_error = f"Krisp token request failed ({exc.code}): {detail[:400]}"
        except Exception as exc:
            last_error = f"Could not create a Krisp Voice Translation session token: {exc}"
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
    try:
        cfg = await ws.receive_json()
        source = str(cfg.get("source_language") or "en-US")
        target = str(cfg.get("target_language") or ("pt-BR" if source == "en-US" else "en-US"))
        allowed_languages = {"en-US", "pt-BR"}
        if source not in allowed_languages or target not in allowed_languages:
            raise RuntimeError("Choose English (US) or Portuguese (Brazil) for both language fields.")
        if source == target:
            raise RuntimeError("The source language and target language must be different.")
        supported = await asyncio.to_thread(_get_supported_languages)
        if source not in supported or target not in supported:
            raise RuntimeError(f"Krisp account does not currently list the required language pair {source} ↔ {target}.")
        voice = str(cfg.get("voice") or "female").lower()
        if voice not in {"male", "female"}:
            voice = "female"
        token = await asyncio.to_thread(_create_session_token)
        upstream = await websockets.connect(
            KRISP_VT_URL,
            additional_headers={"Authorization": f"Api-Key {token}"},
            ping_interval=20, ping_timeout=20, close_timeout=5, max_size=8 * 1024 * 1024,
        )
        await upstream.send(json.dumps({"config": {
            "audio": {"format": "pcm_s16le", "sample_rate": 16000, "channels": 1},
            "source_language": source, "target_language": target, "voice": voice,
            "features": {"background_voice_cancellation": True},
            "transcript": {"interim": True, "final": True, "translate": True},
        }}))
        try:
            hello_raw = await asyncio.wait_for(upstream.recv(), timeout=45)
            hello = json.loads(hello_raw) if isinstance(hello_raw, str) else {}
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

        async def forward_input() -> None:
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    return
                if msg.get("bytes"):
                    audio = msg["bytes"]
                    if len(audio) <= 65536:
                        await upstream.send(audio)
                    else:
                        await ws.send_json({"type": "error", "stage": "audio", "message": "Audio frame exceeded 64 KB."})
                elif msg.get("text"):
                    try:
                        if json.loads(msg["text"]).get("type") == "stop":
                            return
                    except json.JSONDecodeError:
                        pass

        async def forward_output() -> None:
            async for raw in upstream:
                if isinstance(raw, bytes):
                    await ws.send_json({"type": "audio", "audio": base64.b64encode(raw).decode("ascii"), "sample_rate": 16000})
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                err = event.get("error")
                if event.get("type") == "error" or err:
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
*{box-sizing:border-box}body{margin:0;padding:16px;background:#0b1220;color:#edf2ff;font:16px system-ui}main{max-width:680px;margin:auto}.card{padding:16px;margin:12px 0;border:1px solid #30415f;border-radius:16px;background:#131f35}.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}label,.muted{display:block;color:#a9b8d5;font-size:13px;margin-bottom:6px}input,select,button{width:100%;padding:12px;border-radius:10px;border:1px solid #40527a;background:#0c1629;color:#fff;font-size:15px;margin-bottom:10px}button{font-weight:700}.primary{background:#315fe9}.danger{background:#6b2940}.controls{display:grid;grid-template-columns:1fr 1fr;gap:10px}.box{background:#0b1425;border-radius:10px;padding:12px;min-height:50px;overflow-wrap:anywhere}.note{color:#9daaca;font-size:12px;line-height:1.5}.log{font:12px monospace;white-space:pre-wrap;max-height:130px;overflow:auto}.error{color:#ff9eaf}@media(max-width:540px){.row{grid-template-columns:1fr}}
</style></head><body><main>
<div class="card"><h1 style="margin:0">WIGVO</h1><div class="muted">Live voice translation · Agora + Krisp</div></div>
<div class="card"><div class="row"><div><label>Room code</label><input id="room" value="demo-room"></div><div><label>Language you speak</label><select id="src"><option value="en-US">English (US)</option><option value="pt-BR">Português (Brasil)</option></select></div></div>
<div class="row"><div><label>Target language (what the listener hears)</label><select id="target"><option value="pt-BR">Português (Brasil)</option><option value="en-US">English (US)</option></select></div><div><label>Translated voice</label><select id="voice"><option value="female">Female</option><option value="male">Male</option></select></div></div>
<p class="note">Both people use the same room code and select the language they speak. English is translated to Brazilian Portuguese and Brazilian Portuguese to English. Only translated audio is published to Agora. Use headphones to reduce echo.</p>
<div class="controls"><button id="start" class="primary">Start call</button><button id="stop" class="danger" disabled>End call</button></div><div class="box"><span id="status">Ready</span><span id="uid" style="float:right"></span></div></div>
<div class="card"><div class="muted">Recognized speech</div><div id="original" class="box">Waiting for speech…</div><div class="muted" style="margin-top:12px">Translation</div><div id="translated" class="box">Translated text appears here.</div></div>
<div class="card"><div class="muted">Session log</div><div id="log" class="log"></div></div>
</main><script>
let AgoraRTC=null,client=null,track=null,stream=null,micCtx=null,playCtx=null,processor=null,sourceNode=null,silent=null,destination=null,ws=null,config=null,starting=false,running=false,playAt=0,queued=[];
const $=id=>document.getElementById(id),log=s=>{$("log").textContent+=s+"\n";$("log").scrollTop=$("log").scrollHeight},status=(s,bad=false)=>{$("status").textContent=s;$("status").className=bad?"error":""};
const cleanRoom=s=>String(s||"demo-room").toLowerCase().replace(/[^a-z0-9_-]/g,"").slice(0,40)||"demo-room";
const channel=()=>(config.agora_channel_prefix||"speakeasy-")+cleanRoom($("room").value);
function toPcm(a){const o=new Int16Array(a.length);for(let i=0;i<a.length;i++){const v=Math.max(-1,Math.min(1,a[i]));o[i]=v<0?v*32768:v*32767}return o}
function play(b64,rate){if(!playCtx||!destination)return;const bytes=Uint8Array.from(atob(b64),c=>c.charCodeAt(0)),samples=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));if(!samples.length)return;const b=playCtx.createBuffer(1,samples.length,rate),d=b.getChannelData(0);for(let i=0;i<samples.length;i++)d[i]=samples[i]/32768;const n=playCtx.createBufferSource();n.buffer=b;n.connect(destination);playAt=Math.max(playAt,playCtx.currentTime+.02);n.start(playAt);playAt+=b.duration;queued.push(n);n.onended=()=>queued=queued.filter(x=>x!==n)}
async function loadAgora(){if(AgoraRTC)return;await new Promise((ok,no)=>{const s=document.createElement("script");s.src="https://download.agora.io/sdk/release/AgoraRTC_N-"+encodeURIComponent(config.agora_sdk_version)+".js";s.onload=ok;s.onerror=()=>no(Error("Could not load Agora Web SDK"));document.head.appendChild(s)});AgoraRTC=window.AgoraRTC}
async function stopCall(){running=false;starting=false;if(ws){try{if(ws.readyState===1)ws.send(JSON.stringify({type:"stop"}));ws.close()}catch(e){}}ws=null;if(processor)try{processor.disconnect()}catch(e){}if(sourceNode)try{sourceNode.disconnect()}catch(e){}if(silent)try{silent.disconnect()}catch(e){}processor=sourceNode=silent=null;queued.forEach(n=>{try{n.stop()}catch(e){}});queued=[];const c=client;client=null;if(track){try{if(c&&c.connectionState==="CONNECTED")await c.unpublish([track])}catch(e){}try{track.close()}catch(e){}track=null}if(c)try{if(c.connectionState!=="DISCONNECTED")await c.leave()}catch(e){}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(micCtx)try{await micCtx.close()}catch(e){}if(playCtx)try{await playCtx.close()}catch(e){}micCtx=playCtx=null;destination=null;$("start").disabled=false;$("stop").disabled=true;status("Ready")}
async function startCall(){if(starting||running)return;starting=true;$("start").disabled=true;status("Connecting…");let c=null,resolveReady,rejectReady;
try{await loadAgora();c=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});client=c;const sub=async u=>{if(!u||c!==client||c.connectionState!=="CONNECTED"||!u.hasAudio)return;try{await c.subscribe(u,"audio");if(u.audioTrack)u.audioTrack.play()}catch(e){log("Remote audio: "+(e.message||e))}};c.on("user-published",async(u,t)=>{if(t==="audio")await sub(u)});const uid=Math.floor(100000+Math.random()*900000),room=channel();const r=await fetch("/api/agora-token?channel="+encodeURIComponent(room)+"&uid="+uid,{cache:"no-store"}),j=await r.json();if(!r.ok||!j.token)throw Error(j.detail||"Could not obtain Agora token");await c.join(config.agora_app_id,room,j.token,uid);stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/api/browser");ws.onmessage=e=>{try{const d=JSON.parse(e.data);if(d.type==="ready"){log("Krisp ready: "+d.source_language+" → "+d.target_language);if(resolveReady){resolveReady();resolveReady=null}}else if(d.type==="error"){log("["+d.stage+"] "+d.message);status(d.message,true);if(rejectReady){rejectReady(Error(d.message));rejectReady=null}}else if(d.type==="audio")play(d.audio,d.sample_rate||16000);else if(d.type==="transcript"&&d.text)$("original").textContent=d.text;else if(d.type==="translate"&&d.text)$("translated").textContent=d.text}catch(err){log(err.message||err)}};
await new Promise((ok,no)=>{ws.onopen=ok;ws.onerror=()=>no(Error("Could not connect to WIGVO voice pipeline"))});const ready=new Promise((ok,no)=>{resolveReady=ok;rejectReady=no;setTimeout(()=>no(Error("Krisp startup timed out")),50000)});ws.send(JSON.stringify({source_language:$("src").value,target_language:$("target").value,voice:$("voice").value}));await ready;
playCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000,latencyHint:"interactive"});await playCtx.resume();destination=playCtx.createMediaStreamDestination();micCtx=new (window.AudioContext||window.webkitAudioContext)({sampleRate:16000,latencyHint:"interactive"});await micCtx.resume();sourceNode=micCtx.createMediaStreamSource(stream);processor=micCtx.createScriptProcessor(1024,1,1);silent=micCtx.createGain();silent.gain.value=0;sourceNode.connect(processor);processor.connect(silent);silent.connect(micCtx.destination);processor.onaudioprocess=e=>{if(ws&&ws.readyState===1)try{ws.send(toPcm(e.inputBuffer.getChannelData(0)).buffer)}catch(err){}};
track=AgoraRTC.createCustomAudioTrack({mediaStreamTrack:destination.stream.getAudioTracks()[0],encoderConfig:"speech_low_quality"});await c.publish([track]);for(const u of c.remoteUsers||[])await sub(u);running=true;starting=false;$("uid").textContent="UID "+uid;$("stop").disabled=false;status("Live translation connected");log("Joined "+room+"; translated audio only is published to Agora")
}catch(e){log(e.message||String(e));status(e.message||String(e),true);starting=false;if(ws){try{ws.close()}catch(_){}ws=null}if(c===client){client=null;try{if(c.connectionState!=="DISCONNECTED")await c.leave()}catch(_){}}if(track){try{track.close()}catch(_){}track=null}if(stream){stream.getTracks().forEach(t=>t.stop());stream=null}if(micCtx)try{await micCtx.close()}catch(_){}if(playCtx)try{await playCtx.close()}catch(_){}micCtx=playCtx=null;$("start").disabled=false;$("stop").disabled=true;running=false}}
$("src").addEventListener("change",()=>{const src=$("src").value;const target=$("target");if(src===target.value)target.value=src==="en-US"?"pt-BR":"en-US"});$("target").addEventListener("change",()=>{if($("src").value===$("target").value){log("Source and target languages must be different.");$("target").value=$("src").value==="en-US"?"pt-BR":"en-US"}});async function boot(){try{const r=await fetch("/api/config",{cache:"no-store"});config=await r.json();if(!config.agora_app_id)log("AGORA_APP_ID is not configured on Render.");if(!config.krisp_configured)log("KRISP_API_KEY must be added to the Render service.");else log("Krisp Voice Translation configured")}catch(e){log("Config error: "+e.message)}}
$("start").onclick=startCall;$("stop").onclick=stopCall;window.addEventListener("beforeunload",()=>{if(ws&&ws.readyState===1)try{ws.send(JSON.stringify({type:"stop"}))}catch(e){}});boot();
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
