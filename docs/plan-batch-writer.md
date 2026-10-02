# batch-writer — Implementation Plan

> **Vorbild:** [`docs/plan-chat-service.md`](plan-chat-service.md)  
> **Spezifikation:** [`docs/spec-batch-writer.md`](spec-batch-writer.md)  
> **Architektur:** [`../PLANUNG.md`](../PLANUNG.md) · Abschnitt 3.6 und 3.7

**Ziel:** Der `batch-writer` konsumiert Chat-Nachrichten aus der RabbitMQ-Queue `chat.persist`, sammelt sie in einem Puffer (500 Stück oder 200 ms) und schreibt sie per atomarem Bulk-Insert (`INSERT ... ON CONFLICT (id) DO NOTHING`) via Spring `JdbcTemplate` in PostgreSQL. Erst nach erfolgreichem DB-Commit erfolgt das manuelle ACK an RabbitMQ.

**Architektur:** Klassische Trennung `consumer → service → repository`, dazu `config` fuer RabbitMQ. Der Dienst hat keinen Server-Port nach aussen und nimmt keine HTTP-Anfragen an.

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring AMQP, Spring JDBC (`JdbcTemplate`), PostgreSQL 16, JUnit 5, Testcontainers, Maven Multi-Modul.

---

## Globale Vorgaben

Diese Punkte gelten fuer **jede** Aufgabe in diesem Plan:
* **Java 21**, Spring Boot **3.5.16**.
* **Code auf Englisch** — Klassen, Methoden, Variablen, Dateinamen und Log-Meldungen.
* **Alles andere auf Deutsch** — Kommentare, Javadoc, Commit-Messages, Dokumentation.
* **Keine verschachtelten Aufrufe** — ein Ergebnis pro Zeile, in eine benannte Variable.
* **Viele Kommentare** — jede Klasse und jede Methode erklaert *warum* sie existiert.
* **Keine Streams** — einfache, lesbare `for`-Schleifen.
* **Kein JPA** — bewusst Spring `JdbcTemplate` mit `batchUpdate` fuer maximale Transaktionseffizienz.
* **Lombok fuer Boilerplate** — `@Slf4j` fuer den Logger, `@RequiredArgsConstructor` fuer Konstruktor-Injektion. Records fuer Datenklassen.
* **Kein Port nach aussen** in `docker-compose.yml`.
* **Keine Geheimnisse im Repository** — Passwoerter ausschliesslich in `.env`.

---

## Umsetzungsreihenfolge und Aufgaben

### Task 1: Spezifikation (docs/spec-batch-writer.md)
* **Warum zuerst:** Vor dem Schreiben von Code muessen Zweck, Vertrag, Fehlerverhalten und messbare Abnahmekriterien feststehen.
* **Ergebnis:** [`docs/spec-batch-writer.md`](spec-batch-writer.md) mit Abdeckung aller Szenarien S1 bis S8.
* **Commit:** `docs: Spezifikation fuer batch-writer`

---

### Task 2: Umsetzungsplan (docs/plan-batch-writer.md)
* **Warum an dieser Stelle:** Nach der Spezifikation wird der Implementierungsablauf in kleine, testbare Schritte zerlegt.
* **Ergebnis:** [`docs/plan-batch-writer.md`](plan-batch-writer.md) als Leitfaden.
* **Commit:** `docs: Umsetzungsplan fuer batch-writer`

---

### Task 3: PostgreSQL-Schema und Umgebungsdaten
* **Warum an dieser Stelle:** Bevor der Code auf eine Datenbank zugreift, muss das Schema definiert und in der Infrastruktur verankert sein.
* **Dateien:**
  * Anlegen: `postgres/init.sql`
  * Erweitern: `.env.example`, `.env`
* **Test:** Manuelle Validierung der DDL-Syntax.
* **Commit:** `feat: PostgreSQL-Schema und Umgebungsdaten vorbereiten`

---

### Task 4: Maven-Modul batch-writer und Anwendungsstart
* **Warum an dieser Stelle:** Das Grundgeruest des neuen Microservices muss im Maven-Elternprojekt verankert sein und fehlerfrei starten koennen.
* **Dateien:**
  * Erweitern: `pom.xml` (Eltern-POM und Compiler-Plugin)
  * Anlegen: `batch-writer/pom.xml`
  * Anlegen: `batch-writer/src/main/resources/application.yml`
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/BatchWriterApplication.java`
  * Test: `batch-writer/src/test/java/ch/benedict/m321/batchwriter/BatchWriterApplicationTest.java`
* **Test:** `mvn test -pl batch-writer -Dtest=BatchWriterApplicationTest` mit Testcontainers (Postgres + RabbitMQ).
* **Commit:** `feat: Maven-Modul batch-writer anlegen`

---

### Task 5: Datenmodell und Bulk-Repository mit JdbcTemplate
* **Warum an dieser Stelle:** Die Persistenzlogik mit `batchUpdate` und `ON CONFLICT DO NOTHING` ist der Kern des Schreibwegs und muss isoliert verifiziert werden.
* **Dateien:**
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/dto/ChatMessage.java`
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/repository/MessageBatchRepository.java`
  * Test: `batch-writer/src/test/java/ch/benedict/m321/batchwriter/repository/MessageBatchRepositoryIntegrationTest.java`
* **Test:** Prueft erfolgreichen Bulk-Insert sowie Idempotenz bei Duplikaten gegen PostgreSQL Testcontainers.
* **Commit:** `feat: ChatMessage Record und MessageBatchRepository mit JdbcTemplate`

---

### Task 6: RabbitMQ-Konfiguration und BatchMessageConsumer
* **Warum an dieser Stelle:** Verbindet die Message Queue mit dem Repository. Hier wird die Pufferlogik (500 Stk / 200 ms) und das manuelle ACK-Handling implementiert.
* **Dateien:**
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/config/QueueNames.java`
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/config/RabbitConfig.java` (mit `TypePrecedence.INFERRED` fuer S5)
  * Anlegen: `batch-writer/src/main/java/ch/benedict/m321/batchwriter/service/BatchMessageConsumer.java`
  * Test: `batch-writer/src/test/java/ch/benedict/m321/batchwriter/service/BatchMessageConsumerIntegrationTest.java`
* **Test:** Prueft das Zusammenspiel von RabbitMQ-Queue und automatischer Persistierung in PostgreSQL.
* **Commit:** `feat: BatchMessageConsumer mit Pufferlogik und manuellem ACK implementieren`

---

### Task 7: Dedizierte Tests fuer Duplikate (S5) und Datenbankausfall (S7)
* **Warum an dieser Stelle:** Vor der Containerisierung muessen die beiden im Bewertungsraster explizit geforderten Fehlerszenarien automatisiert nachgewiesen werden.
* **Dateien:**
  * Erweitern: `batch-writer/src/test/java/ch/benedict/m321/batchwriter/service/BatchMessageConsumerIntegrationTest.java`
* **Tests:**
  * Test 1 (S5): Identische Nachricht zweimal nur mit Header `content_type: application/json` gesendet -> genau 1 Zeile in DB, 0 in DLQ.
  * Test 2 (S7): Simulation des DB-Ausfalls -> kein Datenverlust, NACK/Retry mit Backoff, automatische Erholung.
* **Commit:** `test: Integrationstests fuer Duplikate und Datenbankausfall ergaenzen`

---

### Task 8: Docker-Containerisierung und docker-compose
* **Warum an dieser Stelle:** Erst wenn alle Unit- und Integrationstests gruen sind, wird das Image gebaut und der Dienst im Multi-Container-Stack verankert.
* **Dateien:**
  * Anlegen: `batch-writer/Dockerfile`
  * Aktualisieren: `chat-service/Dockerfile` (fuer Multi-Modul-Build)
  * Erweitern: `docker-compose.yml` (`postgres` und `batch-writer` im Netz `chat-net`)
* **Test:** `docker compose build` und `docker compose up -d`.
* **Commit:** `feat: Docker-Integration und docker-compose fuer batch-writer`

---

### Task 9: Dokumentation nachfuehren und Abschlusspruefung
* **Warum zum Schluss:** Nach erfolgreichem Gesamtstack wird die README-Tabelle aktualisiert und alle 8 Szenarien S1 bis S8 werden endgueltig verifiziert.
* **Dateien:**
  * Aktualisieren: `README.md`
* **Test:** Vollstaendiger Lauf aller Szenarien S1 bis S8.
* **Commit:** `docs: README nachfuehren fuer batch-writer`

---

## Nacharbeiten nach der Abschlusspruefung (2. Oktober 2026)

Die Abschlusspruefung aus Task 9 habe ich am 2. Oktober 2026 auf einem frischen Klon
nachgestellt, diesmal mit **zufaelligen** `roomId`s, weil das Pruefskript keinen Raum aus
meinem `init.sql` kennen kann. Ergebnis: S3 bis S7 nicht bestanden, **0 Zeilen** in der
Tabelle. Ursache und Entscheide stehen in der Spezifikation (Abschnitt 3.2 Punkt 5,
Abschnitt 4.6 und 4.7). Die folgenden Aufgaben beheben das.

### Task 10: Tests verwenden das Schema aus postgres/init.sql
* **Warum zuerst:** Die Tests haben das Schema bisher selbst angelegt, eine Kopie von `init.sql`. Deshalb blieb der Fremdschluessel-Fehler in den Tests unsichtbar: die Tests haben vor jedem Lauf den passenden Raum eingefuegt. Bevor ich das Schema aendere, muessen die Tests das echte Schema benutzen, sonst pruefen sie die Aenderung nicht.
* **Dateien:**
  * Aendern: `MessageBatchRepositoryIntegrationTest.java`, `BatchMessageConsumerIntegrationTest.java`, `BatchWriterApplicationTest.java`
* **Test:** `mvn test -pl batch-writer` bleibt gruen, die `CREATE TABLE` in den Tests sind weg.
* **Commit:** `test: Tests verwenden das Schema aus postgres/init.sql`

### Task 11: Fremdschluessel auf room entfernen
* **Warum an dieser Stelle:** Das ist der Fehler, der S3 bis S7 zum Scheitern bringt. Zuerst ein Test, der mit dem echten Schema rot ist, dann die Aenderung an `init.sql`.
* **Dateien:**
  * Aendern: `postgres/init.sql`
  * Test: `MessageBatchRepositoryIntegrationTest.savesMessageForRoomThatIsNotInRoomTable`
* **Test:** Der neue Test ist vor der Aenderung rot und danach gruen.
* **Commit:** `fix: Nachrichten fuer unbekannte Raeume speichern`

### Task 12: Ein Stapel ist genau eine Transaktion
* **Warum an dieser Stelle:** S4 zaehlt Transaktionen. Ohne `@Transactional` laeuft `batchUpdate` im Autocommit, und der PostgreSQL-Treiber teilt einen Stapel in mehrere Transaktionen auf. Erst mit genau einer Transaktion pro Stapel ist der Stapel auch atomar, und darauf baut Task 14 auf.
* **Dateien:**
  * Aendern: `MessageBatchRepository.java`
  * Test: `MessageBatchRepositoryIntegrationTest.writesWholeBatchInOneTransaction` (alle Zeilen haben dieselbe Transaktions-ID `xmin`)
* **Commit:** `feat: einen Stapel in genau einer Transaktion schreiben`

### Task 13: BatchMessageConsumer in kurze Methoden aufteilen
* **Warum an dieser Stelle:** `flush()` war zu lang und enthielt die Suche nach dem hoechsten Delivery-Tag zweimal. Task 14 braucht einen dritten Weg (einzeln schreiben). Vorher aufraeumen, damit die neue Logik in eine kurze, eigene Methode passt. Verhalten aendert sich nicht.
* **Dateien:**
  * Aendern: `BatchMessageConsumer.java`, `RabbitConfig.java` (Imports statt voller Klassennamen, Konstante fuer Prefetch), `MessageBatchRepository.java` (Kommentare an den Methoden der anonymen Klasse, CLAUDE.md)
* **Test:** Die bestehenden Tests bleiben gruen.
* **Commit:** `refactor: BatchMessageConsumer in kurze Methoden aufteilen`

### Task 14: Nicht speicherbare Nachrichten in die DLQ legen
* **Warum an dieser Stelle:** Ohne diesen Schritt blockiert eine einzige kaputte Nachricht ihren ganzen Stapel fuer immer. Braucht die atomaren Stapel aus Task 12 und die kurzen Methoden aus Task 13.
* **Dateien:**
  * Aendern: `BatchMessageConsumer.java` (JSON selbst lesen, Pflichtfelder pruefen, Datenfehler einzeln nachschreiben)
  * Aendern: `RabbitConfig.java` (`Jackson2JsonMessageConverter` entfernen)
  * Test: `BatchMessageConsumerIntegrationTest` mit drei neuen Tests: unvollstaendige Nachricht, kein gueltiges JSON, zu langer `senderName` neben einer gueltigen Nachricht
* **Nachtrag waehrend der Umsetzung:** Der Test mit ungueltigem JSON fand 4 statt 1 Nachricht in der DLQ. Der Konverter von Spring lehnt bei einem Fehler alle unbestaetigten Nachrichten des Kanals ab (Spezifikation 2.4). Deshalb liest der Listener das JSON jetzt selbst.
* **Commit:** `feat: nicht speicherbare Nachrichten in die DLQ legen`

### Task 15: Ausfall-Test mit echtem Verbindungsabbruch
* **Warum an dieser Stelle:** Der bisherige Ausfall-Test hat die Tabelle umbenannt. Das ist ein SQL-Fehler, kein Ausfall: die Verbindung blieb bestehen. Jetzt trennt der Test alle Verbindungen und verbietet neue, wie bei `docker compose stop postgres`. Dazu kommt ein Verbindungs-Timeout von 5 s, damit der Dienst den Ausfall schnell merkt. Kommt nach Task 14, weil der Test auch prueft, dass waehrend des Ausfalls nichts in der DLQ landet.
* **Dateien:**
  * Aendern: `batch-writer/src/main/resources/application.yml`
  * Aendern: `BatchMessageConsumerIntegrationTest.recoversFromDatabaseOutageWithoutManualRestart`
* **Commit:** `test: Datenbankausfall mit echtem Verbindungsabbruch pruefen`

### Task 16: README nachfuehren und Szenarien erneut pruefen
* **Warum zum Schluss:** Erst wenn alles gruen ist, stimmt die Beschreibung. Danach laufen S1 bis S8 noch einmal auf einem frischen Klon.
* **Dateien:**
  * Aendern: `README.md` (Startbefehl und Testbeschreibung mit Postgres)
* **Test:** Vollstaendiger Lauf S1 bis S8 auf einem frischen Klon.
* **Commit:** `docs: README fuer den Stack mit Postgres nachfuehren`
