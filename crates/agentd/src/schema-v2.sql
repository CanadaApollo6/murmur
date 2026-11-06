BEGIN;
ALTER TABLE deliveries ADD COLUMN msg_seq INTEGER;
UPDATE deliveries SET msg_seq=(SELECT seq FROM messages WHERE id=message_id);
CREATE TABLE mail_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    nonce TEXT NOT NULL,
    message_id TEXT NOT NULL,
    prev_state TEXT,
    state TEXT NOT NULL,
    turn_started INTEGER NOT NULL,
    incarnation INTEGER,
    attempts INTEGER NOT NULL,
    reason TEXT,
    at TEXT NOT NULL
);
CREATE TABLE mail_edges (
    sender TEXT NOT NULL, recipient TEXT NOT NULL, state TEXT NOT NULL, n INTEGER NOT NULL,
    PRIMARY KEY(sender,recipient,state)
) WITHOUT ROWID;
CREATE INDEX deliveries_state ON deliveries(state,msg_seq);
CREATE INDEX messages_pair ON messages(sender,recipient,seq);
INSERT INTO mail_edges SELECT m.sender,m.recipient,d.state,COUNT(*)
    FROM messages m JOIN deliveries d ON d.message_id=m.id GROUP BY m.sender,m.recipient,d.state;
CREATE TRIGGER mail_events_insert AFTER INSERT ON deliveries BEGIN
    INSERT INTO mail_events(nonce,message_id,prev_state,state,turn_started,incarnation,attempts,reason,at)
        VALUES(lower(hex(randomblob(16))),NEW.message_id,NULL,NEW.state,NEW.turn_started,NEW.incarnation,NEW.attempts,NEW.reason,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
    INSERT INTO mail_edges SELECT sender,recipient,NEW.state,1 FROM messages WHERE id=NEW.message_id
        ON CONFLICT(sender,recipient,state) DO UPDATE SET n=n+1;
END;
CREATE TRIGGER mail_events_update AFTER UPDATE ON deliveries
WHEN OLD.state IS NOT NEW.state OR OLD.turn_started IS NOT NEW.turn_started
    OR OLD.incarnation IS NOT NEW.incarnation OR OLD.attempts IS NOT NEW.attempts OR OLD.reason IS NOT NEW.reason
BEGIN
    INSERT INTO mail_events(nonce,message_id,prev_state,state,turn_started,incarnation,attempts,reason,at)
        VALUES(lower(hex(randomblob(16))),NEW.message_id,OLD.state,NEW.state,NEW.turn_started,NEW.incarnation,NEW.attempts,NEW.reason,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
    UPDATE mail_edges SET n=n-1 WHERE OLD.state IS NOT NEW.state AND state=OLD.state
        AND (sender,recipient)=(SELECT sender,recipient FROM messages WHERE id=NEW.message_id);
    INSERT INTO mail_edges SELECT sender,recipient,NEW.state,1 FROM messages
        WHERE id=NEW.message_id AND OLD.state IS NOT NEW.state
        ON CONFLICT(sender,recipient,state) DO UPDATE SET n=n+1;
END;
PRAGMA user_version=2;
COMMIT;
