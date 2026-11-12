BEGIN;
-- Agents registered before roles existed are workers.
ALTER TABLE agents ADD COLUMN role TEXT NOT NULL DEFAULT 'worker' CHECK(role IN ('worker','verifier'));
-- Set when the operator reopens a task. NULL means the request's sent_at.
ALTER TABLE tasks ADD COLUMN clock_start TEXT;
CREATE TABLE verifications (
    report_msg TEXT NOT NULL UNIQUE REFERENCES messages(id),
    claimant_task TEXT NOT NULL REFERENCES tasks(id),
    state TEXT NOT NULL CHECK(state IN ('pending','running','settled')),
    verifier TEXT REFERENCES agents(id),
    task TEXT UNIQUE REFERENCES tasks(id),
    verifier_family TEXT,
    claimant_family TEXT,
    snapshot_clean INTEGER CHECK(snapshot_clean IN (0,1)),
    verdict_msg TEXT REFERENCES messages(id),
    result TEXT,
    CHECK(state<>'pending' OR task IS NULL),
    CHECK(state<>'running' OR (task IS NOT NULL AND verifier IS NOT NULL)),
    CHECK((state='settled') = (result IS NOT NULL))
);
CREATE INDEX verifications_state ON verifications(state);
-- A task that a version 4 build left verifying gets a pending row, so a
-- verifier or the operator can still settle it.
INSERT INTO verifications(report_msg,claimant_task,state)
    SELECT json_quote(json_extract(state,'$.Verifying.report')),id,'pending'
    FROM tasks WHERE json_extract(state,'$.Verifying') IS NOT NULL;
PRAGMA user_version=5;
COMMIT;
