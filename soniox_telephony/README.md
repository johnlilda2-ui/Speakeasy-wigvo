# Soniox WIGVO telephony service

This directory contains the FastAPI application currently launched by the Render
`speakeasy-wigvo` service.

## PSTN call provider

The `POST /api/calls` request accepts an optional `provider` field:

- `twilio` (default): preserves the existing Twilio call flow.
- `signalwire`: dials both participants through the SignalWire Compatibility API.

The two-party PSTN test interface is available at `/phone-call`. The root path
continues to serve the existing browser translation demo.

## SignalWire environment variables

Set these on the Render `speakeasy-wigvo` service before selecting SignalWire:

| Variable | Purpose |
|---|---|
| `SIGNALWIRE_SPACE_URL` | HTTPS SignalWire Space origin, e.g. `https://example.signalwire.com` |
| `SIGNALWIRE_PROJECT_ID` | SignalWire Project UUID |
| `SIGNALWIRE_API_TOKEN` | SignalWire API token used for Compatibility API requests |
| `SIGNALWIRE_PHONE_NUMBER` | Purchased SignalWire caller ID in E.164 format |
| `SIGNALWIRE_SIGNING_KEY` | Project webhook signing key for HTTP callback verification |
| `SIGNALWIRE_STREAM_TOKEN` | Random secret of at least 32 ASCII bytes, used as the Media Stream bearer token |

Keep all six values server-side. Do not place API tokens, signing keys, or stream tokens
in browser JavaScript.

The SignalWire call flow is:

1. The browser posts `provider: "signalwire"` to `/api/calls`.
2. WIGVO creates both PSTN legs through the SignalWire Compatibility API.
3. SignalWire posts signed callbacks to `/signalwire/voice/{session_id}/{leg}`.
4. WIGVO returns bidirectional cXML with PCMU/8 kHz and an `authBearerToken`.
5. SignalWire opens `/signalwire/stream/{session_id}/{leg}`; WIGVO validates the
   bearer token before accepting the WebSocket.
6. Signed call/stream terminal callbacks close both legs and release Soniox streams.

Twilio continues to use its existing `/twilio/voice`, `/twilio/status`, and
`/twilio/stream` endpoints. Calls default to Twilio unless the browser or API client
explicitly selects SignalWire.
