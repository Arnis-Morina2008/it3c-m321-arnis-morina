# M321 — Chat-App (Klasse IT3c)

Lernprojekt zum Modul **M321 Verteilte Systeme / Microservices**. Wir bauen gemeinsam eine
Chat-Anwendung aus mehreren Services, die über eine Message Queue miteinander reden und mit
docker-compose gestartet werden.

## Für Lernende: so startest du

1. Dieses Repository **forken** (Button «Fork» oben rechts).
2. Deinen Fork klonen:
   ```bash
   git clone https://github.com/<dein-benutzername>/it3c-m321.git
   cd it3c-m321
   ```
3. Voraussetzungen installieren: **Java 21**, **Maven**, **Docker Desktop**, **Git**.
4. Lokale Umgebungsdatei anlegen und die Werte anpassen:
   ```bash
   cp .env.example .env
   ```
5. Die Planung lesen (siehe unten) — erst verstehen, dann programmieren.

Alle Aufgaben werden in **deinem Fork** gelöst. Das Original-Repository bleibt die Referenz.

## Bauen, testen, starten

```bash
mvn clean test                   # alle Tests, RabbitMQ und PostgreSQL kommen per Testcontainers
docker compose up -d --build     # rabbitmq, chat-service, postgres und batch-writer im Netz chat-net
docker compose up -d --scale batch-writer=2   # zwei batch-writer an derselben Queue
docker compose up -d --scale chat-service=2   # zwei chat-service, jeder mit eigener Zustell-Queue
```

Kein Dienst veröffentlicht einen Port auf den Host. Der einzige offene Port des Gesamtsystems
gehört später dem Gateway. Zum Ausprobieren schickt man Nachrichten deshalb aus einem
Hilfscontainer im selben Netz:

```bash
docker run --rm --network chat-net curlimages/curl:8.10.1 -s -X POST http://chat-service:8080/messages \
  -H "Content-Type: application/json" \
  -d '{"roomId":"3f2b1c4e-0000-0000-0000-000000000001","senderId":"anna","senderName":"Anna","content":"Hallo"}'
docker compose exec postgres psql -U chat -d chat -c "SELECT sender_name, content, sent_at FROM message;"
```

Wie der `batch-writer` arbeitet und wie man jedes Szenario misst, steht in
[`docs/spec-batch-writer.md`](docs/spec-batch-writer.md).

### Nachrichten live mitlesen

Jede Nachricht geht zusätzlich per Server-Sent Events an alle, die gerade im Raum sind.
Einen Stream öffnet man ebenfalls aus einem Hilfscontainer im Netz `chat-net`. `-N` sorgt
dafür, dass curl jede Zeile sofort ausgibt, statt zu puffern:

```bash
docker run --rm --network chat-net curlimages/curl:8.10.1 -sN \
  "http://chat-service:8080/stream?roomId=3f2b1c4e-0000-0000-0000-000000000001"
```

In einem zweiten Terminal eine Nachricht für denselben Raum senden (Befehl oben). Im ersten
erscheint sie sofort:

```
:connected

id:6e59eeb3-15c1-461f-8b53-83791524eac9
data:{"id":"6e59eeb3-…","roomId":"3f2b1c4e-…","senderId":"anna","senderName":"Anna","content":"Hallo","sentAt":"…"}
```

Wer nach einer Unterbrechung mit dem Header `Last-Event-ID: <letzte id>` neu verbindet,
bekommt zuerst die verpassten Nachrichten. Mit zwei Instanzen
(`docker compose up -d --scale chat-service=2`) erreicht man jede einzeln über ihren
Containernamen (`docker compose ps`). Alle Szenarien von Bewertung 2 prüft
`bash docs/abnahme.sh`; Verhalten und Grenzen stehen in
[`docs/spec-live-zustellung.md`](docs/spec-live-zustellung.md).

## Was gebaut wird

| Baustein | Technologie | Aufgabe | Stand |
|---|---|---|---|
| chat-service | Spring Boot 3, Java 21 | Nimmt Nachrichten per `POST /messages` an, legt sie auf Queue und Fanout-Exchange. Liefert sie live per `GET /stream?roomId=` (Server-Sent Events) an die Clients des Raums, jede Instanz mit eigener Queue an `chat.delivery` | vorhanden |
| rabbitmq | RabbitMQ 3.13 | Message Queue zwischen den Services | vorhanden |
| batch-writer | Spring Boot 3, Java 21 | Einziger Schreiber in die Datenbank: liest `chat.persist`, schreibt Stapel von bis zu 500 Nachrichten | vorhanden |
| postgres | PostgreSQL | Speichert den Chat-Verlauf | vorhanden |
| keycloak | Keycloak | Login (OIDC) | folgt |
| web-gateway | nginx | Einziger nach aussen offener Port | folgt |
| Web-UI | React | Browser-Client | folgt |

Alles unterhalb des Gateways läuft in einem internen Docker-Netzwerk und ist von aussen nicht
erreichbar.

## Dokumente

- [`PLANUNG.md`](PLANUNG.md) — Auftrag, Stack, Architektur, Nachrichtenfluss, Queues, Datenmodell,
  Umsetzungsreihenfolge. Das ist die Grundlage für alles Weitere.
- [`docs/design/2026-08-28-chat-app-planung.html`](docs/design/2026-08-28-chat-app-planung.html)
  — grafische Fassung der Planung, lokal im Browser öffnen.
- [`docs/plan-chat-service.md`](docs/plan-chat-service.md) — Schritt-für-Schritt-Plan, nach dem
  der `chat-service` gebaut wurde. Jeder Schritt mit Test.
- [`docs/spec-batch-writer.md`](docs/spec-batch-writer.md) und
  [`docs/plan-batch-writer.md`](docs/plan-batch-writer.md) — Spezifikation und Umsetzungsplan
  des `batch-writer`.
- [`docs/spec-live-zustellung.md`](docs/spec-live-zustellung.md) und
  [`docs/plan-live-zustellung.md`](docs/plan-live-zustellung.md) — Spezifikation und
  Umsetzungsplan der Live-Zustellung, [`docs/abnahme.sh`](docs/abnahme.sh) prüft sie.
- [`CLAUDE.md`](CLAUDE.md) — Codestil-Regeln für dieses Projekt. Gelten auch für dich.
- [`docs/flipchart-chat-app.png`](docs/flipchart-chat-app.png) — das Flipchart aus der Lektion,
  von dem die Planung ausgeht.

## Codestil, kurz

Der Massstab ist: **kann eine lernende Person jede Zeile vorlesen und sagen, was sie tut?**

- Eine Anweisung pro Zeile, Zwischenresultate in benannte Variablen.
- `for`-Schleife statt Stream, `if` statt verschachteltem Ternary.
- Sprechende Namen in ganzen Wörtern.
- Über jeder Methode ein bis zwei Sätze: was sie tut und warum es sie gibt.
- Kommentare auf Deutsch, als Erklärung an eine Mitlernende.

Die vollständigen Regeln stehen in [`CLAUDE.md`](CLAUDE.md).
