-- =============================================================================
-- M321 Chat-App — Datenbank-Schema
-- Wird beim ersten Start des PostgreSQL-Containers automatisch ausgefuehrt.
-- =============================================================================

-- Chat-Raeume
CREATE TABLE IF NOT EXISTS room (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Mitglieder in Raeumen (fuer spaetere Zugriffskontrolle)
CREATE TABLE IF NOT EXISTS room_member (
    room_id UUID NOT NULL REFERENCES room(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (room_id, user_id)
);

-- Chat-Nachrichten (gespeichert vom batch-writer via Bulk-INSERT)
-- room_id hat bewusst KEINEN Fremdschluessel auf room(id): Raeume legt noch
-- niemand an, und der chat-service nimmt jede roomId an. Mit Fremdschluessel
-- scheitert jede Nachricht fuer einen unbekannten Raum und mit ihr der ganze
-- Stapel. Ob ein Raum existiert, prueft spaeter die Stelle, die Nachrichten
-- annimmt. Begruendung: docs/spec-batch-writer.md, Abschnitt 3.2.
CREATE TABLE IF NOT EXISTS message (
    id UUID PRIMARY KEY,
    room_id UUID NOT NULL,
    sender_id VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL
);

-- Index fuer die Abfrage der Chat-Historie ("letzte 50 Nachrichten eines Raums")
CREATE INDEX IF NOT EXISTS idx_message_room_sent ON message (room_id, sent_at DESC);

-- Standard-Raum fuer den Schulungsbetrieb anlegen
INSERT INTO room (id, name)
VALUES ('3f2b1c4e-0000-0000-0000-000000000001', 'General')
ON CONFLICT (id) DO NOTHING;
