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
