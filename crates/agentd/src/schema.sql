BEGIN;
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),
    epoch INTEGER NOT NULL DEFAULT 0 CHECK(epoch >= 0)
);
CREATE UNIQUE INDEX IF NOT EXISTS active_agent_names ON agents(name) WHERE retired=0;
CREATE TABLE IF NOT EXISTS messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    sender TEXT NOT NULL,
    client_key TEXT NOT NULL,
    recipient TEXT NOT NULL REFERENCES agents(id),
    conversation TEXT NOT NULL,
    in_reply_to TEXT REFERENCES messages(id),
    hop INTEGER NOT NULL CHECK(hop BETWEEN 0 AND 65535),
    act TEXT NOT NULL,
    task TEXT REFERENCES tasks(id) DEFERRABLE INITIALLY DEFERRED,
    exposure TEXT,
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 65536),
    sent_at TEXT NOT NULL,
    UNIQUE(sender,client_key)
);
CREATE INDEX IF NOT EXISTS recipient_mail ON messages(recipient,seq);
CREATE INDEX IF NOT EXISTS conversation_mail ON messages(conversation,seq);
CREATE TABLE IF NOT EXISTS deliveries (
    message_id TEXT PRIMARY KEY REFERENCES messages(id),
    state TEXT NOT NULL CHECK(state IN ('queued','offered','consumed','uncertain','held','dead')),
    incarnation INTEGER,
    turn_started INTEGER NOT NULL DEFAULT 0 CHECK(turn_started IN (0,1)),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    reason TEXT,
    CHECK((state IN ('offered','uncertain')) = (incarnation IS NOT NULL)),
    CHECK((state IN ('held','dead')) = (reason IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    parent TEXT REFERENCES tasks(id),
    owner TEXT NOT NULL REFERENCES agents(id),
    requested_by TEXT NOT NULL,
    request_msg TEXT NOT NULL UNIQUE REFERENCES messages(id),
    deliverable TEXT NOT NULL,
    stakes TEXT NOT NULL,
    budget TEXT NOT NULL,
    spent TEXT NOT NULL,
    state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exposures (
    agent TEXT NOT NULL REFERENCES agents(id),
    epoch INTEGER NOT NULL CHECK(epoch >= 0),
    seq INTEGER NOT NULL CHECK(seq > 0),
    source_kind TEXT NOT NULL CHECK(source_kind='message'),
    source_id TEXT NOT NULL REFERENCES messages(id),
    PRIMARY KEY(agent,epoch,seq)
);
PRAGMA user_version=1;
COMMIT;
