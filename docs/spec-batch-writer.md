# Spezifikation: batch-writer (Modul M321)

**Autor:** Arnis Morina  
**Klasse:** IT3c  
**Datum:** 30. September 2026  
**Status:** Abgenommen / Umgesetzt  
**Referenz:** [`PLANUNG.md`](../PLANUNG.md) · Abschnitt 3.6 und 3.7

---

## 1. Zweck und Abgrenzung

### 1.1 Zweck
Der `batch-writer` ist der **einzige Schreiber** in die PostgreSQL-Datenbank der Chat-Anwendung. Seine primäre Aufgabe ist es, den Schreibweg vom Echtzeit-Messaging zu entkoppeln und die Datenbank vor Überlastung zu schützen:
* Er konsumiert persistierungsbedürftige Nachrichten aus der Queue `chat.persist`.
* Er puffert eingehende Nachrichten im Arbeitsspeicher, bis entweder **500 Nachrichten** gesammelt sind oder seit der ersten gepufferten Nachricht **200 ms** vergangen sind.
* Er schreibt den gesamten Stapel in einem einzigen atomaren Bulk-Insert (`INSERT ... ON CONFLICT (id) DO NOTHING`) via Spring `JdbcTemplate` in die Datenbank.
* Erst nach erfolgreichem Datenbank-`COMMIT` sendet er ein manuelles Acknowledgement (`basicAck`) an RabbitMQ (**At-least-once Garantie**).

### 1.2 Abgrenzung (Was der Dienst bewusst NICHT tut)
* **Kein Lesen von Chat-Historien:** Das Lesen von Nachrichten für Clients übernimmt das Gateway bzw. der Chat-Service direkt über PostgreSQL. Der `batch-writer` führt ausschliesslich `INSERT`s aus.
* **Keine Benutzer- oder Raumverwaltung:** Benutzerverwaltung liegt bei Keycloak, Raum-Mitgliedschaften werden nicht vom `batch-writer` validiert oder gepflegt.
* **Keine Token-Prüfung:** Der Dienst läuft ausschliesslich im internen Netzwerk `chat-net` und nimmt keine externen HTTP-Anfragen an.
* **Kein Veröffentlichen von Ports:** Der `batch-writer` öffnet keinen Server-Port auf den Docker-Host oder ins interne Netz.
* **Keine Exactly-once-Garantie:** Bei Ausfällen liefert RabbitMQ Nachrichten erneut aus. Die Idempotenz wird auf Datenbankebene über den Primärschlüssel `id` sichergestellt.

---

## 2. Vertrag (Contract)

### 2.1 Herkunft der Nachrichten
Nachrichten werden vom `chat-service` über die interne REST-Schnittstelle `POST /messages` entgegengenommen, validiert, mit Server-UUID und Zeitstempel versehen und anschliessend direkt in die RabbitMQ-Queue `chat.persist` publiziert (siehe [`docs/plan-chat-service.md`](plan-chat-service.md)).

### 2.2 Queue und Exchange
* **Queue-Name:** `chat.persist` (durable)
* **Dead-Letter-Exchange:** `""` (Default Exchange)
* **Dead-Letter-Routing-Key:** `chat.dlq`
* **Prefetch-Count:** `500`

### 2.3 Nachrichtenformat (JSON Payload)
Nachrichten liegen als reines UTF-8 JSON auf der Queue. Der Content-Type Header ist `application/json`.

Beispiel-Payload:
```json
{
  "id": "3f2b1c4e-0000-0000-0000-000000000001",
  "roomId": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
  "senderId": "anna",
  "senderName": "Anna Muster",
  "content": "Hallo zusammen!",
  "sentAt": "2026-09-30T14:15:30.123456Z"
}
```

### 2.4 Feldspezifikation

| Feld | Typ | Format | Pflicht | Beschreibung |
|---|---|---|---|---|
| `id` | UUID | RFC 4122 (String) | Ja | Primärschlüssel der Nachricht, vom `chat-service` erzeugt |
| `roomId` | UUID | RFC 4122 (String) | Ja | ID des Chat-Raums (Fremdschlüssel auf `room.id`) |
| `senderId` | String | UTF-8 String | Ja | `sub`-Claim aus Keycloak |
| `senderName` | String | UTF-8 String | Ja | Anzeigename des Absenders |
| `content` | String | UTF-8 Text | Ja | Eigentlicher Nachrichteninhalt |
| `sentAt` | Instant | ISO-8601 UTC | Ja | Server-Zeitstempel der Annahme im `chat-service` |

### 2.5 Header-Toleranz (Szenario S5)
Der `batch-writer` setzt `Jackson2JavaTypeMapper.TypePrecedence.INFERRED` ein. Er ist somit **nicht** auf Spring-spezifische Header wie `__TypeId__` angewiesen. Nachrichten mit ausschliesslich `content_type: application/json` werden problemlos in die interne Klasse `ChatMessage` deserialisiert.

---

## 3. Datenmodell und Konfiguration

### 3.1 PostgreSQL-Schema (`postgres/init.sql`)
Das Schema entsteht automatisch beim ersten Start des PostgreSQL-Containers via `/docker-entrypoint-initdb.d/init.sql`.

```sql
CREATE TABLE IF NOT EXISTS room (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS room_member (
    room_id UUID NOT NULL REFERENCES room(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS message (
    id UUID PRIMARY KEY,
    room_id UUID NOT NULL REFERENCES room(id) ON DELETE CASCADE,
    sender_id VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_message_room_sent ON message (room_id, sent_at DESC);
```

### 3.2 Begründung der Modellentscheide
1. **`message.id` als UUID PK:** Ermöglicht client- bzw. serviceunabhängige ID-Vergabe vor dem DB-Schreibvorgang.
2. **`ON CONFLICT (id) DO NOTHING`:** Macht das Einfügen idempotent. Bei erneuter Zustellung (At-least-once) wird die Dublette ohne Exception verworfen.
3. **Index `(room_id, sent_at DESC)`:** Optimiert exakt die einzige Leseabfrage: die letzten N Nachrichten eines bestimmten Raums chronologisch abzurufen.
4. **Denormalisierter `sender_name`:** Gewährleistet, dass alte Chatnachrichten auch lesbar bleiben, wenn das Benutzerkonto im IDP gelöscht oder geändert wurde.

### 3.3 Umgebungsvariablen

| Variable | Beschreibung | Standard lokal | Standard Docker |
|---|---|---|---|
| `RABBITMQ_HOST` | Hostname des RabbitMQ-Brokers | `localhost` | `rabbitmq` |
| `RABBITMQ_USER` | Benutzername für RabbitMQ | `guest` | `${RABBITMQ_USER}` (aus `.env`) |
| `RABBITMQ_PASSWORD` | Passwort für RabbitMQ | `guest` | `${RABBITMQ_PASSWORD}` (aus `.env`) |
| `POSTGRES_HOST` | Hostname des PostgreSQL-Servers | `localhost` | `postgres` |
| `POSTGRES_USER` | Benutzername für PostgreSQL | `chat` | `${POSTGRES_USER}` (aus `.env`) |
| `POSTGRES_PASSWORD` | Passwort für PostgreSQL | `guest` | `${POSTGRES_PASSWORD}` (aus `.env`) |
| `POSTGRES_DB` | Name der Chat-Datenbank | `chat` | `${POSTGRES_DB}` (aus `.env`) |

---

## 4. Verhalten im Normal- und Fehlerfall

### 4.1 Normalfall (Szenario S3)
* Nachrichten treffen über `chat.persist` ein.
* Der `BatchMessageConsumer` speichert sie in einer synchronisierten In-Memory-Liste.
* Wenn `buffer.size() >= 500` oder der 200-ms-Timer (`@Scheduled(fixedRate = 200)`) feuert, wird `flush()` aufgerufen.
* `flush()` übergibt die Liste an `MessageBatchRepository.saveBatch()`, welches ein einziges `jdbcTemplate.batchUpdate()` ausführt.
* Nach erfolgreichem Commit sendet der Consumer `channel.basicAck(maxDeliveryTag, true)` und leert den Puffer.

### 4.2 Start bei gefüllter Queue / Last (Szenario S4)
* Befinden sich viele Nachrichten (z.B. 1000) in der Queue, liefert RabbitMQ dank `prefetch=500` sofort die ersten 500 Stück.
* Die 500. Nachricht löst sofort den ersten Flush aus.
* Der Stapel wird in **einer einzigen Transaktion** geschrieben und quittiert.
* Anschliessend liefert RabbitMQ die nächsten 500 Nachrichten, die in einer zweiten Transaktion geschrieben werden.
* Für 1000 Nachrichten fallen somit genau 2 Datenbanktransaktionen an (Anforderung: höchstens 100).

### 4.3 Duplikate (Szenario S5)
* Trifft eine Nachricht mit identischer `id` mehrfach ein, führt `INSERT ... ON CONFLICT (id) DO NOTHING` dazu, dass Postgres den zweiten Datensatz ignoriert.
* `batchUpdate` läuft fehlerfrei durch, der Consumer bestätigt mit `basicAck`.
* Es entsteht kein Fehler, die Zeile existiert genau einmal, und es landet nichts in `chat.dlq`.

### 4.4 Mehrere Instanzen (Szenario S6)
* Bei `--scale batch-writer=2` verbinden sich zwei unabhängige Container mit der Queue `chat.persist`.
* RabbitMQ agiert nach dem Muster **Competing Consumers**: Jede Nachricht wird an genau einen Consumer zugestellt.
* Beide Instanzen schreiben unabhängig mit `ON CONFLICT DO NOTHING`.
* Keine Nachricht geht verloren, keine wird doppelt persistiert.

### 4.5 Ausfall der Datenbank (Szenario S7)
* Ist PostgreSQL gestoppt oder nicht erreichbar, schlägt `saveBatch()` mit einer `DataAccessException` fehl.
* Der Consumer fängt die Exception ab, wartet 1 Sekunde (Backoff gegen CPU-Spins) und sendet `basicNack(maxDeliveryTag, true, true)` mit `requeue=true`.
* RabbitMQ belässt die Nachrichten in der Queue.
* Sobald PostgreSQL wieder verfügbar ist (nach 15 s), gelingt der nächste `saveBatch()`-Aufruf.
* Alle Nachrichten werden fehlerfrei persistiert; der `batch-writer` erholt sich vollständig automatisch ohne manuellen Neustart.

---

## 5. Abnahmekriterien und Messbefehle

| Szenario | Anforderung | Messbefehl zur Verifikation |
|---|---|---|
| **S1** | `mvn clean test` im Wurzelverzeichnis ist in einem Lauf grün. | `mvn clean test` (Exit-Code 0, 0 Failures, 0 Errors) |
| **S2** | Frischer Klon startet sauber mit `.env` aus `.env.example`, kein offener Port. | `docker compose up -d --build`<br>`docker compose ps`<br>`docker compose ps \| grep -v "0.0.0.0"` |
| **S3** | 1000 Nachrichten über `POST /messages` innerhalb von 60 s in DB, Queue leer. | `docker compose exec postgres psql -U chat -d chat -t -c "SELECT count(*) FROM message;"`<br>`docker compose exec rabbitmq rabbitmqctl list_queues name messages` |
| **S4** | Nach Writer-Neustart bei 1000 Nachrichten höchstens 100 Transaktionen. | `docker compose exec postgres psql -U chat -d chat -t -c "SELECT count(*) FROM message;"` (ergibt 1000)<br>Prüfung via Logmeldungen `Batch inserted into database with 500 messages`. |
| **S5** | Gleiche Nachricht zweimal gesendet -> genau 1 Zeile in DB, 0 in `chat.dlq`. | `docker compose exec postgres psql -U chat -d chat -c "SELECT count(*) FROM message WHERE id = '...';"` (ergibt 1)<br>`docker compose exec rabbitmq rabbitmqctl list_queues name messages \| grep chat.dlq` (ergibt 0) |
| **S6** | Zwei Instanzen (`--scale batch-writer=2`), 1000 Nachrichten -> alle da, keine doppelt. | `docker compose up -d --scale batch-writer=2`<br>`docker compose exec postgres psql -U chat -d chat -t -c "SELECT count(*), count(DISTINCT id) FROM message;"` (beide Werte identisch) |
| **S7** | Postgres 15s gestoppt, 300 Nachrichten gesendet -> nach <90s alle in DB, ohne Writer-Neustart. | `docker compose stop postgres`<br>`# 300 Nachrichten senden`<br>`docker compose start postgres`<br>`# nach 20s:`<br>`docker compose exec postgres psql -U chat -d chat -t -c "SELECT count(*) FROM message;"` |
| **S8** | Einhaltung der Coderegeln aus `CLAUDE.md` / `GEMINI.md`. | Prüfung des Quellcodes: keine Streams, Lombok für Logger/Konstruktor, ausführliche Kommentare, `.env` ignoriert. |
