BEGIN;
-- The true answer of a seeded task. The agents never see it.
CREATE TABLE seeds (
    task TEXT PRIMARY KEY REFERENCES tasks(id),
    expected TEXT NOT NULL CHECK(expected IN ('feasible','infeasible'))
);
-- Whether a claimant or a verifier was right about one report. A claimant
-- row names the agent; a verifier row names the verifier's family.
CREATE TABLE calibration_events (
    report_msg TEXT NOT NULL REFERENCES messages(id),
    role TEXT NOT NULL CHECK(role IN ('claimant','verifier')),
    agent TEXT REFERENCES agents(id),
    family TEXT,
    score TEXT NOT NULL CHECK(score IN ('correct','wrong','abstain')),
    source TEXT NOT NULL CHECK(source IN ('seed','operator')),
    UNIQUE(report_msg, role),
    CHECK((role='claimant') = (agent IS NOT NULL)),
    CHECK((role='verifier') = (family IS NOT NULL))
);
CREATE TRIGGER calibration_events_no_update BEFORE UPDATE ON calibration_events
    BEGIN SELECT RAISE(ABORT, 'calibration events are append-only'); END;
CREATE TRIGGER calibration_events_no_delete BEFORE DELETE ON calibration_events
    BEGIN SELECT RAISE(ABORT, 'calibration events are append-only'); END;
PRAGMA user_version=6;
COMMIT;
