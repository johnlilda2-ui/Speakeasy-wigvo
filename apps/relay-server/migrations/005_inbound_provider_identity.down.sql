-- Reverse 005_inbound_provider_identity.up.sql.
BEGIN;
DROP INDEX IF EXISTS uq_inbound_dispatch_provider_call_sid;
ALTER TABLE inbound_call_dispatch
  DROP CONSTRAINT IF EXISTS inbound_call_dispatch_provider_check;
ALTER TABLE inbound_call_dispatch
  DROP COLUMN IF EXISTS provider;
ALTER TABLE inbound_call_dispatch
  ADD CONSTRAINT inbound_call_dispatch_provider_call_sid_key UNIQUE (provider_call_sid);
COMMIT;
