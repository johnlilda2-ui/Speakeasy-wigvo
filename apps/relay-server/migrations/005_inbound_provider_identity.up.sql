-- Namespace inbound provider call IDs and retain Twilio as the legacy default.
BEGIN;

ALTER TABLE inbound_call_dispatch
  ADD COLUMN IF NOT EXISTS provider text NOT NULL DEFAULT 'twilio';

ALTER TABLE inbound_call_dispatch
  DROP CONSTRAINT IF EXISTS inbound_call_dispatch_provider_call_sid_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_inbound_dispatch_provider_call_sid
  ON inbound_call_dispatch (provider, provider_call_sid)
  WHERE provider_call_sid IS NOT NULL;

ALTER TABLE inbound_call_dispatch
  ADD CONSTRAINT inbound_call_dispatch_provider_check
  CHECK (provider IN ('twilio', 'signalwire'));

COMMIT;
