# Spezifikation: batch-writer (Modul M321)

**Autor:** Arnis Morina  
**Klasse:** IT3c  
**Datum:** 30. September 2026, überarbeitet am 2. Oktober 2026  
**Status:** Umgesetzt  
**Referenz:** [`PLANUNG.md`](../PLANUNG.md) · Abschnitt 3.5, 3.6 und 3.7

> **Überarbeitung vom 2. Oktober 2026:** Bei der Abschlussprüfung mit zufälligen `roomId`s
> landete keine einzige Nachricht in der Datenbank. Ursache war der Fremdschlüssel
> `message.room_id → room.id` (Abschnitt 3.2, Entscheid 5). Dazu kamen zwei Fehlerfälle,
> die die erste Fassung nicht abgedeckt hat: unvollständige und nicht speicherbare
> Nachrichten (Abschnitt 4.6 und 4.7).

---

## 1. Zweck und Abgrenzung

### 1.1 Zweck
Der `batch-writer` ist der **einzige Schreiber** in die PostgreSQL-Datenbank der Chat-Anwendung. Seine Aufgabe ist es, den Schreibweg vom Echtzeit-Messaging zu entkoppeln und die Datenbank vor Überlastung zu schützen. PLANUNG.md rechnet mit 1'667 Nachrichten pro Sekunde; einzeln geschrieben wären das 1'667 Transaktionen pro Sekunde.
* Er konsumiert Nachrichten aus der Queue `chat.persist`.
* Er puffert sie im Arbeitsspeicher, bis **500 Nachrichten** gesammelt sind, spätestens aber bis der **200-ms-Timer** feuert.
* Er schreibt den ganzen Stapel mit einem Bulk-Insert (`INSERT ... ON CONFLICT (id) DO NOTHING`) in **genau einer Transaktion**.
* Erst nach dem erfolgreichen `COMMIT` bestätigt er die Nachrichten bei RabbitMQ (`basicAck`). Das ist **At-least-once**.
* Was er nie speichern kann, legt er in die Dead-Letter-Queue `chat.dlq`, statt es endlos zu wiederholen.

### 1.2 Abgrenzung (was der Dienst bewusst NICHT tut)
* **Kein Lesen von Chat-Historien:** Der `batch-writer` führt ausschliesslich `INSERT`s aus.
* **Keine Raum- und Mitgliederverwaltung:** Er prüft nicht, ob ein Raum existiert oder ob der Absender Mitglied ist. Das ist Aufgabe der Stelle, die Nachrichten annimmt (später Gateway bzw. `chat-service`).
* **Keine Token-Prüfung:** Der Dienst läuft nur im internen Netz `chat-net` und nimmt keine HTTP-Anfragen an.
* **Kein Port:** Der `batch-writer` öffnet keinen Server-Port, weder auf den Host noch ins interne Netz. Er verbindet sich nur ausgehend zu RabbitMQ und PostgreSQL.
* **Kein Exactly-once:** Bei Ausfällen liefert RabbitMQ Nachrichten erneut aus. Duplikate richten wegen des Primärschlüssels `id` keinen Schaden an.
* **Keine Auswertung der DLQ:** Was in `chat.dlq` liegt, bleibt dort liegen, bis ein Mensch es anschaut.

---

## 2. Vertrag

### 2.1 Woher ich den Vertrag kenne
Ich habe den Vertrag nicht aus der Planung abgeschrieben, sondern an zwei Stellen nachgeprüft:

1. **Im Quellcode des `chat-service`:**
   * `MessagePublisher.publish()` ruft `rabbitTemplate.convertAndSend(QueueNames.PERSIST_QUEUE, message)` auf. Ohne Exchange-Angabe geht die Nachricht über den Default-Exchange `""` mit dem Routing-Key `chat.persist` direkt in die Queue.
   * `RabbitConfig.jsonMessageConverter()` setzt den `Jackson2JsonMessageConverter` mit dem `ObjectMapper` von Spring Boot ein. Daraus folgt: Der Body ist JSON, die Feldnamen sind die Namen der Record-Komponenten von `chat-service/.../dto/ChatMessage.java`, und `Instant` wird als ISO-8601-Text geschrieben.
   * `MessageService.accept()` vergibt `id` (`UUID.randomUUID()`) und `sentAt` (`Instant.now()`). Die übrigen Felder kommen aus `SendMessageRequest`, dort mit `@NotNull`/`@NotBlank` validiert.
2. **Auf der laufenden Queue:** `batch-writer` gestoppt, eine Nachricht über `POST /messages` geschickt und sie mit `rabbitmqadmin get` angeschaut (Befehl in Abschnitt 5). Ergebnis vom 2. Oktober 2026:

```json
{
  "exchange": "",
  "routing_key": "chat.persist",
  "properties": {
    "delivery_mode": 2,
    "headers": { "__TypeId__": "ch.benedict.m321.chatservice.dto.ChatMessage" },
    "content_encoding": "UTF-8",
    "content_type": "application/json"
  },
  "payload": "{\"id\":\"6e59eeb3-15c1-461f-8b53-83791524eac9\",\"roomId\":\"a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d\",\"senderId\":\"anna\",\"senderName\":\"Anna Muster\",\"content\":\"Hallo zusammen!\",\"sentAt\":\"2026-10-02T13:26:59.737112982Z\"}"
}
```

### 2.2 Queue
* **Queue-Name:** `chat.persist` (durable). Beide Dienste deklarieren sie mit **identischen** Argumenten, sonst lehnt RabbitMQ die zweite Deklaration ab (`PRECONDITION_FAILED`).
* **Dead-Letter-Exchange:** `""` (Default-Exchange), **Dead-Letter-Routing-Key:** `chat.dlq`. Was der `batch-writer` mit `requeue=false` ablehnt, landet so in `chat.dlq`.
* **delivery_mode 2:** Die Nachrichten sind persistent und überleben einen Neustart von RabbitMQ.

### 2.3 Nachrichtenformat (Body)

| Feld | JSON-Typ | Format | Pflicht | Spalte |
|---|---|---|---|---|
| `id` | String | UUID | Ja | `id` |
| `roomId` | String | UUID | Ja | `room_id` |
| `senderId` | String | Text, max. 255 Zeichen | Ja | `sender_id` |
| `senderName` | String | Text, max. 255 Zeichen | Ja | `sender_name` |
| `content` | String | Text | Ja | `content` |
| `sentAt` | String | ISO-8601 in UTC, bis Nanosekunden | Ja | `sent_at` |

`sentAt` kommt mit Nanosekunden (9 Stellen). PostgreSQL speichert Mikrosekunden; die letzten drei Stellen fallen beim Speichern weg. Für die Sortierung eines Chats ist das unerheblich.

### 2.4 Header
* `content_type: application/json` ist der einzige Header, auf den sich der `batch-writer` verlässt.
* Der Header `__TypeId__` enthält den Klassennamen **des chat-service** (`ch.benedict.m321.chatservice.dto.ChatMessage`). Diese Klasse gibt es im `batch-writer` nicht. Deshalb steht der Konverter auf `TypePrecedence.INFERRED`: Der Zieltyp kommt aus der Signatur der Listener-Methode, der Header wird ignoriert. Das gilt auch, wenn der Header ganz fehlt (Szenario S5).

### 2.5 Was der batch-writer NICHT voraussetzt
Weil jeder mit Zugang zum Broker direkt in `chat.persist` schreiben kann (Szenario S5 tut genau das), verlässt sich der `batch-writer` nicht auf die Validierung im `chat-service`. Er prüft selbst, ob alle sechs Pflichtfelder vorhanden sind (Abschnitt 4.6).

---

## 3. Datenmodell und Konfiguration

### 3.1 Wo das Schema entsteht
Das Schema steht in [`postgres/init.sql`](../postgres/init.sql). `docker-compose.yml` hängt die Datei nach `/docker-entrypoint-initdb.d/init.sql`. Der Postgres-Container führt sie **nur beim ersten Start mit leerem Datenverzeichnis** aus. Ein `docker compose stop/start` (Szenario S7) behält Daten und Schema, ein `docker compose down` mit neuem `up` beginnt leer.

Die Integrationstests hängen **dieselbe Datei** an dieselbe Stelle im Testcontainer. So können Test-Schema und Stack-Schema nicht auseinanderlaufen.

```sql
CREATE TABLE IF NOT EXISTS message (
    id UUID PRIMARY KEY,
    room_id UUID NOT NULL,
    sender_id VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_message_room_sent ON message (room_id, sent_at DESC);
```

Die Tabellen `room` und `room_member` aus PLANUNG.md 3.7 legt `init.sql` ebenfalls an, der `batch-writer` benutzt sie nicht.

### 3.2 Begründung der Modellentscheide
1. **`id` als UUID-Primärschlüssel:** Der `chat-service` vergibt die ID, bevor die Nachricht in der Datenbank ist. Die Datenbank muss keine ID erzeugen.
2. **`ON CONFLICT (id) DO NOTHING`:** Macht das Einfügen idempotent. Kommt eine Nachricht ein zweites Mal (At-least-once, Szenario S5), verwirft PostgreSQL die Dublette ohne Fehler.
3. **Index `(room_id, sent_at DESC)`:** Passt genau zur einzigen Leseabfrage aus PLANUNG.md: „die letzten 50 Nachrichten eines Raums". Sonst gibt es bewusst keinen Index, denn jeder Index macht jeden der 1'667 Inserts pro Sekunde teurer.
4. **Denormalisierter `sender_name`:** Alte Nachrichten bleiben lesbar, auch wenn das Konto in Keycloak gelöscht wird.
5. **Kein Fremdschlüssel von `room_id` auf `room.id`:** PLANUNG.md 3.7 zeichnet die Beziehung, die erste Fassung hatte deshalb `REFERENCES room(id)`. Gemessen am 2. Oktober 2026: 1000 Nachrichten mit zufälliger `roomId` über `POST /messages`, **0 Zeilen** in der Tabelle, 502 fehlgeschlagene Stapel in Endlosschleife. Gründe für den Entscheid:
   * Räume sind nicht Teil dieser Aufgabe. Niemand legt Räume an, der `chat-service` nimmt jede `roomId` an.
   * Der Fremdschlüssel prüft bei jedem Insert die Tabelle `room` und kostet bei 1'667 Inserts pro Sekunde zusätzlich Zeit.
   * Ob ein Raum existiert und ob der Absender darin schreiben darf, gehört an die Stelle, die die Nachricht annimmt, nicht an den letzten Schritt des Schreibwegs. Dort kann man dem Absender noch antworten, hier nicht mehr.
   * Die Spalte `room_id` und der Index bleiben, die Beziehung besteht also weiterhin, nur prüft die Datenbank sie nicht.

### 3.3 Umgebungsvariablen

| Variable | Bedeutung | Vorgabe ohne Docker | Wert im Stack |
|---|---|---|---|
| `RABBITMQ_HOST` | Hostname des Brokers | `localhost` | `rabbitmq` (fest in `docker-compose.yml`) |
| `RABBITMQ_USER` | Benutzer für RabbitMQ | `guest` | aus `.env` |
| `RABBITMQ_PASSWORD` | Passwort für RabbitMQ | `guest` | aus `.env` |
| `POSTGRES_HOST` | Hostname der Datenbank | `localhost` | `postgres` (fest in `docker-compose.yml`) |
| `POSTGRES_USER` | Benutzer für PostgreSQL, legt der Postgres-Container beim ersten Start an | `chat` | aus `.env` |
| `POSTGRES_PASSWORD` | Passwort für PostgreSQL | `guest` | aus `.env` |
| `POSTGRES_DB` | Name der Datenbank | `chat` | aus `.env` |

`.env.example` enthält Beispielwerte für alle Variablen aus `.env`. Die echte `.env` steht in `.gitignore`.

### 3.4 Feste Werte im Code
Diese Werte sind bewusst **keine** Umgebungsvariablen. Sie hängen zusammen und sollen nicht einzeln verstellt werden.

| Wert | Wo | Warum genau dieser Wert |
|---|---|---|
| Stapelgrösse 500 | `BatchMessageConsumer.BATCH_SIZE` | PLANUNG.md 3.6. 1'667 Nachrichten/s werden so zu etwa 4 Transaktionen/s |
| Prefetch 500 | `RabbitConfig.PREFETCH_COUNT` | Gleich gross wie der Stapel: RabbitMQ liefert genug für einen vollen Stapel, aber nie mehr, als bei einem Absturz erneut zugestellt werden muss |
| Timer 200 ms | `@Scheduled(fixedRate = 200)` | Bei wenig Last wartet keine Nachricht länger als etwa 200 ms im Puffer |
| Pause 1 s nach Datenbankfehler | `BatchMessageConsumer.RETRY_PAUSE_MILLISECONDS` | Verhindert, dass der Dienst bei einem Ausfall im Millisekundentakt Fehler produziert |
| Verbindungs-Timeout 5 s | `spring.datasource.hikari.connection-timeout` in `application.yml` | Ist die Datenbank weg, merkt der Dienst das nach 5 s statt nach den vorgegebenen 30 s und gibt die Nachrichten an die Queue zurück |

---

## 4. Verhalten im Normal- und Fehlerfall

### 4.1 Normalfall (Szenario S3)
* Nachrichten treffen über `chat.persist` ein. `onMessage()` prüft die Pflichtfelder und legt die Nachricht zusammen mit Kanal und Delivery-Tag in den Puffer.
* Bei 500 Nachrichten ruft `onMessage()` selbst `flush()` auf, sonst tut es der 200-ms-Timer.
* `flush()` nimmt den ganzen Puffer heraus und übergibt die Nachrichten an `MessageBatchRepository.saveBatch()`. Dieses führt ein `jdbcTemplate.batchUpdate()` innerhalb von `@Transactional` aus: **ein Stapel, eine Transaktion**.
* Nach dem Commit bestätigt `flush()` pro Kanal den höchsten Delivery-Tag mit `basicAck(tag, multiple=true)`. Das bestätigt alle Nachrichten des Stapels mit einem Aufruf.

### 4.2 Start bei gefüllter Queue (Szenario S4)
* Liegen 1000 Nachrichten in der Queue, liefert RabbitMQ wegen `prefetch=500` sofort bis zu 500 Stück.
* Ein Stapel wird geschrieben, sobald 500 im Puffer liegen oder der Timer feuert. Der Timer kann also auch einen Teilstapel schreiben.
* Erwartung: 1000 Nachrichten ergeben eine Handvoll Stapel und damit eine Handvoll Transaktionen. Die Grenze von 100 Transaktionen wäre erst bei durchschnittlich weniger als 10 Nachrichten pro Stapel überschritten.
* Zum Vergleich: ohne Puffer wären es 1000 Transaktionen.

### 4.3 Duplikate (Szenario S5)
* Trifft eine Nachricht mit derselben `id` zweimal ein, verwirft `ON CONFLICT (id) DO NOTHING` die zweite.
* `batchUpdate` läuft ohne Fehler durch, beide Nachrichten werden mit `basicAck` bestätigt.
* Ergebnis: genau eine Zeile, nichts in `chat.dlq`.
* Das gilt auch, wenn die beiden Nachrichten im selben Stapel stehen: PostgreSQL arbeitet die Inserts eines Stapels nacheinander ab, der zweite sieht den ersten.

### 4.4 Mehrere Instanzen (Szenario S6)
* Bei `--scale batch-writer=2` hängen zwei Consumer an derselben Queue `chat.persist`. RabbitMQ stellt jede Nachricht genau einem von beiden zu (**Competing Consumers**, PLANUNG.md 3.5).
* Jede Instanz bestätigt nur die Nachrichten auf ihrem eigenen Kanal. Deshalb merkt sich der Puffer zu jeder Nachricht den Kanal.
* Stürzt eine Instanz ab, gibt RabbitMQ deren unbestätigte Nachrichten an die andere. Was die erste schon geschrieben hatte, verwirft `ON CONFLICT`. Es geht nichts verloren und nichts wird doppelt.

### 4.5 Datenbank nicht erreichbar (Szenario S7)
* Ist PostgreSQL gestoppt, scheitert schon das Öffnen der Transaktion (nach höchstens 5 s Verbindungs-Timeout) oder der Insert auf einer abgerissenen Verbindung.
* `flush()` fängt den Fehler ab, wartet 1 s und gibt **alle** Nachrichten des Stapels mit `basicNack(tag, multiple=true, requeue=true)` an die Queue zurück.
* RabbitMQ liefert sie sofort wieder aus, der nächste Versuch beginnt. Das wiederholt sich, solange die Datenbank weg ist. Im Log steht pro Versuch eine Warnung `Database not reachable ...`.
* Während des Ausfalls landet **nichts** in `chat.dlq`. Ein Ausfall ist vorübergehend, die Nachrichten sind in Ordnung.
* Ist PostgreSQL wieder da, baut der Verbindungspool (HikariCP) neue Verbindungen auf, der nächste Versuch gelingt. Ein Neustart des `batch-writer` ist nicht nötig.
* Erwartung für S7: 15 s Ausfall plus höchstens 5 s Timeout plus 1 s Pause, also alle 300 Nachrichten nach etwa 25 s in der Tabelle.

### 4.6 Unvollständige oder unlesbare Nachricht
* **Kein gültiges JSON** (oder ein Feld im falschen Format, z.B. `id` ist keine UUID): Die Umwandlung scheitert, bevor `onMessage()` aufgerufen wird. Der `ConditionalRejectingErrorHandler` von Spring AMQP lehnt die Nachricht mit `requeue=false` ab, sie landet in `chat.dlq`.
* **Pflichtfeld fehlt** (z.B. kein `content`): `onMessage()` prüft die sechs Felder. Fehlt eines, lehnt es die Nachricht sofort mit `basicReject(tag, requeue=false)` ab, sie landet in `chat.dlq` und kommt gar nicht erst in den Puffer.
* Begründung: Eine solche Nachricht wird auch beim hundertsten Versuch nicht gültig. Ohne Ablehnung würde sie den ganzen Stapel, in dem sie steht, für immer blockieren.

### 4.7 Nachricht, die die Datenbank ablehnt
* Beispiel: `senderName` mit mehr als 255 Zeichen. Der `chat-service` begrenzt die Länge nicht, die Spalte schon.
* Dann scheitert der ganze Stapel mit einer `DataIntegrityViolationException`. `flush()` erkennt diesen Fehlertyp und schreibt die Nachrichten des Stapels **einzeln**, jede in ihrer eigenen Transaktion.
* Was einzeln gelingt, wird bestätigt. Was einzeln mit `DataIntegrityViolationException` scheitert, wird mit `basicReject(tag, requeue=false)` in `chat.dlq` gelegt. Scheitert eine einzelne Nachricht aus einem anderen Grund (Datenbank gerade weg), geht sie mit `requeue=true` zurück in die Queue.
* So kostet eine kaputte Nachricht ihren Stapel einmal 500 Einzel-Transaktionen, blockiert ihn aber nicht.
* **Abweichung von PLANUNG.md 3.5:** Dort steht „nach 3 fehlgeschlagenen Versuchen" in die DLQ. Ich lege Datenfehler beim ersten Mal in die DLQ, weil derselbe Insert mit denselben Daten jedes Mal gleich scheitert. Datenbankausfälle (4.5) wiederhole ich dagegen ohne Grenze, weil S7 verlangt, dass nach dem Ausfall alle Nachrichten in der Tabelle sind.

### 4.8 Absturz des batch-writer
* Stürzt der Dienst ab, sind die Nachrichten im Puffer noch nicht bestätigt. RabbitMQ stellt sie nach dem Verbindungsabbruch erneut zu (an die andere Instanz oder nach dem Neustart). Geschriebene, aber noch nicht bestätigte Nachrichten verwirft `ON CONFLICT`.

---

## 5. Abnahmekriterien und Messbefehle

Alle Befehle laufen im Wurzelverzeichnis des Repositories mit einer `.env` aus `.env.example` (Benutzer und Datenbank heissen dort `chat`). Nachrichten schicke ich aus einem Hilfscontainer im Netz `chat-net`, weil kein Dienst einen Port veröffentlicht:

```bash
# eine Nachricht über POST /messages, mit zufälliger roomId
docker run --rm --network chat-net curlimages/curl:8.10.1 -s -X POST http://chat-service:8080/messages \
  -H "Content-Type: application/json" \
  -d '{"roomId":"'"$(uuidgen)"'","senderId":"anna","senderName":"Anna","content":"Hallo"}'

# Zähler in der Datenbank und in den Queues
docker compose exec postgres psql -U chat -d chat -tA -c "SELECT count(*) FROM message;"
docker compose exec rabbitmq rabbitmqctl -q list_queues name messages messages_unacknowledged consumers
```

| Szenario | Kriterium | Messbefehl |
|---|---|---|
| **S1** | `mvn clean test` im Wurzelverzeichnis ist in einem Lauf grün. | `mvn clean test` → `BUILD SUCCESS`, `Failures: 0, Errors: 0` |
| **S2** | Frischer Klon, `.env` aus `.env.example`: alle vier Dienste laufen, keiner veröffentlicht einen Port. | `docker compose up -d --build --wait` → Exit-Code 0<br>`docker compose ps --format "{{.Service}} {{.Ports}}"` → in keiner Zeile ein `->` |
| **S3** | 1000 Nachrichten über `POST /messages` sind nach spätestens 60 s alle in der Tabelle, Queue leer. | `SELECT count(*) FROM message;` steigt um 1000<br>`list_queues` → `chat.persist 0 0` |
| **S4** | `batch-writer` gestoppt, 1000 Nachrichten gesendet, dann gestartet: alle 1000 gespeichert, höchstens 100 Transaktionen. | `docker compose stop batch-writer`, senden, dann vorher/nachher:<br>`SELECT xact_commit FROM pg_stat_database WHERE datname = 'chat';`<br>Differenz ≤ 100 (die Messabfragen selbst zählen mit) |
| **S5** | Dieselbe Nachricht zweimal direkt in `chat.persist`, nur mit `content_type`: genau eine Zeile, `chat.dlq` leer. | `docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> publish exchange=amq.default routing_key=chat.persist payload='<json>' properties='{"content_type":"application/json"}'` (zweimal)<br>`SELECT count(*) FROM message WHERE id = '<id>';` → `1`<br>`list_queues` → `chat.dlq 0` |
| **S6** | `--scale batch-writer=2`, 1000 Nachrichten: beide Instanzen hängen an der Queue, alle da, keine doppelt. | `docker compose up -d --scale batch-writer=2 --wait`<br>`list_queues` → `chat.persist` mit `consumers 2`<br>`SELECT count(*), count(DISTINCT id) FROM message;` → beide Werte gleich, um 1000 gestiegen |
| **S7** | Postgres gestoppt, 300 Nachrichten gesendet, Postgres nach 15 s gestartet: nach spätestens 90 s alle 300 in der Tabelle, `batch-writer` ohne Neustart. | `docker compose stop postgres`, senden, 15 s warten, `docker compose start postgres`<br>`SELECT count(*) FROM message;` steigt um 300<br>`docker compose ps batch-writer` → `Up` seit vor dem Ausfall |
| **S8** | Regeln aus `CLAUDE.md`: keine Streams, Kommentar über jeder Klasse und Methode, `.env` nicht im Repo. | `grep -rnE "\.stream\(|Stream\.|Collectors" batch-writer/src` → keine Treffer<br>`git ls-files .env` → keine Ausgabe<br>Kommentare: Durchsicht jeder Datei unter `batch-writer/src` |
| **Vertrag** | Format auf der Queue wie in Abschnitt 2.1. | `docker compose stop batch-writer`, eine Nachricht senden, dann<br>`docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> -f raw_json get queue=chat.persist ackmode=reject_requeue_true` |

### 5.1 Welcher automatische Test welches Verhalten prüft

| Verhalten | Test |
|---|---|
| Spring-Kontext startet mit echter Queue und Datenbank | `BatchWriterApplicationTest.contextLoads` |
| Bulk-Insert speichert einen Stapel | `MessageBatchRepositoryIntegrationTest.insertsBatchOfMessagesSuccessfully` |
| Ein Stapel ist genau eine Transaktion (S4) | `MessageBatchRepositoryIntegrationTest.writesWholeBatchInOneTransaction` |
| Duplikat mit gleicher `id` wird verworfen | `MessageBatchRepositoryIntegrationTest.ignoresDuplicateMessagesWithSameId` |
| Nachricht für einen unbekannten Raum wird gespeichert | `MessageBatchRepositoryIntegrationTest.savesMessageForRoomThatIsNotInRoomTable` |
| Nachricht im Format des chat-service (mit dessen `__TypeId__`) kommt in die Tabelle (S3) | `BatchMessageConsumerIntegrationTest.consumesMessageInChatServiceFormatAndWritesToDatabase` |
| Duplikat nur mit `content_type` (S5) | `BatchMessageConsumerIntegrationTest.handlesDuplicateMessagesWithoutErrorAndDoesNotRouteToDeadLetterQueue` |
| Datenbankausfall mit echtem Verbindungsabbruch (S7) | `BatchMessageConsumerIntegrationTest.recoversFromDatabaseOutageWithoutManualRestart` |
| Unvollständige Nachricht → DLQ (4.6) | `BatchMessageConsumerIntegrationTest.movesIncompleteMessageToDeadLetterQueue` |
| Kein gültiges JSON → DLQ (4.6) | `BatchMessageConsumerIntegrationTest.movesInvalidJsonToDeadLetterQueue` |
| Datenfehler → nur die kaputte Nachricht in die DLQ (4.7) | `BatchMessageConsumerIntegrationTest.movesUnsavableMessageToDeadLetterQueueAndSavesTheRest` |
