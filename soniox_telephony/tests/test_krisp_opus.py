import asyncio
import json
import unittest
from unittest.mock import patch, AsyncMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import browser_test as b


class PacketTests(unittest.TestCase):
    def test_duration(self):
        self.assertEqual(b.opus_packet_duration_ms(bytes([0x78, 0])), 20)
        self.assertEqual(b.opus_packet_duration_ms(bytes([0xF8, 0])), 20)
        self.assertEqual(b.opus_packet_duration_ms(bytes([0xF9, 0, 0])), 40)
        for invalid in (b'', bytes([3]), bytes([0xfb, 0x3f])):
            with self.assertRaises(ValueError):
                b.opus_packet_duration_ms(invalid)

    def test_pacing_does_not_accumulate_send_time(self):
        deadline = 100.0
        for i in range(1000):
            now = max(100.0 + i * .020, deadline)
            deadline = b.next_audio_deadline(deadline, now, .020)
        self.assertAlmostEqual(deadline, 120.0, places=6)
        self.assertAlmostEqual(b.next_audio_deadline(100, 101, .020), 101.015)


class Client:
    def __init__(self, fmt):
        self.fmt = fmt
        self.headers = {'sec-websocket-protocol': 'wigvo.opus.v1'} if fmt == 'opus' else {}
        self.selected = None
        self.events = []
        self.first = True
        self.output = asyncio.Event()
    async def accept(self, subprotocol=None): self.selected = subprotocol
    async def close(self, **kwargs): pass
    async def receive_json(self):
        return {"source_language": "en-US", "target_language": "pt-BR",
                **({"audio_format": "opus"} if self.fmt == "opus" else {})}
    async def receive(self):
        if self.first:
            self.first = False
            return {"bytes": bytes([0xF8, 0]) if self.fmt == "opus" else bytes(640)}
        await self.output.wait()
        return {"type": "websocket.disconnect"}
    async def send_json(self, event):
        self.events.append(event)
        if event.get("type") == "audio":
            self.output.set()
    async def send_bytes(self, raw):
        self.events.append(raw)
        self.output.set()


class Upstream:
    def __init__(self, fmt):
        self.fmt = fmt
        self.sent = []
        self.input = asyncio.Event()
        self.yielded = False
    async def send(self, data):
        self.sent.append(data)
        if isinstance(data, bytes):
            self.input.set()
    async def recv(self):
        return '{"type":"ready"}'
    def __aiter__(self): return self
    async def __anext__(self):
        if self.yielded:
            await asyncio.Future()
        await self.input.wait()
        self.yielded = True
        return bytes([0xF8, 0]) if self.fmt == "opus" else bytes(640)
    async def close(self): pass


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_pcm_and_opus_sessions(self):
        for fmt in ("pcm_s16le", "opus"):
            with self.subTest(fmt=fmt):
                client, upstream = Client(fmt), Upstream(fmt)
                with patch.object(b, "KRISP_API_KEY", "fake"), patch.object(b, "_create_session_token", return_value="fake"), patch.object(b.websockets, "connect", new=AsyncMock(return_value=upstream)):
                    await asyncio.wait_for(b.browser_translation(client), 3)
                self.assertEqual(json.loads(upstream.sent[0])["config"]["audio"]["format"], fmt)
                self.assertEqual(client.selected, "wigvo.opus.v1" if fmt == "opus" else None)
                self.assertEqual(client.events[0]["type"], "ready")
                self.assertEqual(client.events[0]["audio_format"], fmt)
                if fmt == "opus":
                    self.assertEqual(client.events[1], bytes([0xF8, 0]))
                else:
                    self.assertEqual(client.events[1]["type"], "audio")
                    self.assertEqual(client.events[1]["sample_rate"], 16000)


    async def test_codec_mismatch_rejected_before_token_creation(self):
        client = Client("opus")
        client.headers = {}
        with patch.object(b, "KRISP_API_KEY", "fake"), patch.object(b, "_create_session_token") as token:
            await asyncio.wait_for(b.browser_translation(client), 3)
        token.assert_not_called()
        self.assertTrue(any(isinstance(e, dict) and e.get("type") == "error" for e in client.events))
