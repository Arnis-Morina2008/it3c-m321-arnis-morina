# Live-Zustellung – Umsetzungsplan

> **Spezifikation:** [`docs/spec-live-zustellung.md`](spec-live-zustellung.md)
> **Vorbild:** [`docs/plan-batch-writer.md`](plan-batch-writer.md)
> **Architektur:** [`../PLANUNG.md`](../PLANUNG.md) · Abschnitt 3.4 und 3.5

**Ziel:** Jede Instanz des `chat-service` bindet eine eigene Queue an den Fanout-Exchange
`chat.delivery` und schiebt die Nachrichten per Server-Sent Events an die offenen
Verbindungen ihres Raums (`GET /stream?roomId=...`). Nach einem Verbindungsabbruch holt
der Client mit `Last-Event-ID` genau die fehlenden Nachrichten nach.

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring AMQP, Spring MVC `SseEmitter`,
JUnit 5, Testcontainers (RabbitMQ).

---

## Regeln für diesen Plan

* Der Plan liegt **committet und gepusht** auf GitHub, bevor Schritt 1 beginnt.
* **Ein Schritt = ein Commit.** Commit-Message auf Deutsch, wie im Schritt angegeben.
* Ab dem ersten Schritt wird am bestehenden Text **nichts mehr geändert**. Muss ich
  umplanen, bleibt der alte Text stehen; die Änderung kommt als eigener Eintrag mit
  **Änderungsgrund** ans Ende („Änderungen am Plan“), in einem eigenen Commit.
* Einzige Ausnahme: das Feld **«Geprüft und korrigiert»**. Ich fülle es nach jedem Schritt
  aus: was ich am Vorschlag der KI geändert habe und warum.

## Vorgaben für jeden Auftrag an die KI

Jeder Auftrag unten beginnt sinngemäss mit diesem Satz, damit ich ihn nicht jedes Mal
wiederholen muss:

> Halte dich an `CLAUDE.md` (Code Englisch, Kommentare Deutsch, keine Streams, keine
> verschachtelten Aufrufe, Kommentar über jeder Klasse und Methode, Lombok `@Slf4j` und
> `@RequiredArgsConstructor`, Records für Datenklassen). Lies zuerst
> `docs/spec-live-zustellung.md` und die genannten Dateien. Ändere nur, was der Schritt
> verlangt, und zeig mir am Schluss den Testlauf.

---

## Schritt 1: Vertrag am Zustell-Exchange als Test festhalten

* **Ziel:** Ein automatischer Test beweist, dass am Exchange `chat.delivery` dasselbe JSON
  ankommt wie in `chat.persist` (Spezifikation 2.1).
* **Warum an dieser Stelle:** Der ganze Stream schickt diesen Body unverändert weiter
  (Spezifikation 2.2). Bevor Code darauf aufbaut, soll ein Test rot werden, falls jemand
  das Format später ändert. Den Vertrag habe ich am 12. November 2026 von Hand geprüft;
  der Test macht diese Prüfung wiederholbar.
* **Auftrag an die KI:**
  > Erweitere `chat-service/src/test/java/ch/benedict/m321/chatservice/service/MessagePublisherIntegrationTest.java`
  > um einen Test `deliveryBodyEqualsPersistBody`. Der Test bindet eine eigene Queue an
  > `chat.delivery`, veröffentlicht eine Nachricht mit `MessagePublisher.publish()`, holt
  > die Rohnachricht aus dieser Queue und aus `chat.persist` (mit `rabbitTemplate.receive`,
  > ohne Konverter) und prüft: beide Bodies sind als Text gleich, `content_type` ist
  > `application/json`, der Body enthält die sechs Felder `id`, `roomId`, `senderId`,
  > `senderName`, `content`, `sentAt`. Kein Produktionscode ändern.
* **Test:** `mvn test -pl chat-service -Dtest=MessagePublisherIntegrationTest` → grün.
* **Commit:** `test: Vertrag am Zustell-Exchange festhalten`
* **Geprüft und korrigiert:**
  * **Geprüft:** Den neuen Test `deliveryBodyEqualsPersistBody` und die zwei Hilfsmethoden
    gelesen. `mvn test -pl chat-service -Dtest=MessagePublisherIntegrationTest` → 3 Tests,
    0 Fehler. Gegenprobe: In `MessagePublisher` kurz den Zustellweg so geändert, dass er
    `senderName` als `null` schickt → der Test wird rot (`expected: ..."senderName":"Anna Muster"...
    but was: ..."senderName":null...`). Danach `MessagePublisher` mit `git checkout`
    wiederhergestellt. Der Test schlägt also an, wenn sich das Format auf nur einem Weg ändert.
  * **Geändert:** Drei Zeilen im Vorschlag der KI hatten verschachtelte Aufrufe
    (`new String(message.getBody(), ...)`, `receive(queue.getName(), 5000)`,
    `getMessageProperties().getContentType()`). Das widerspricht CLAUDE.md («ein Ergebnis pro
    Zeile») und würde in S8 auffallen. Ich habe sie in benannte Variablen aufgeteilt
    (`deliveryQueueName`, `persistBytes`, `deliveryBytes`, `deliveryProperties`). Danach
    erneut 3 Tests, 0 Fehler. Produktionscode ist unverändert.
  * **Aufgefallen:**
    * `bindProbeQueueToDeliveryExchange()` wiederholt den Anfang von
      `publishesToDeliveryExchange`. Bewusst so gelassen: Der Schritt soll einen Test
      ergänzen; den alten Test umzubauen wäre ein zweites Thema im selben Commit.
    * Die Feldprüfung schaut nur, ob die sechs Felder vorhanden sind, nicht ob sie einen
      Wert haben. In der Gegenprobe hat deshalb nur der Vergleich der Bodies angeschlagen.
      Reicht für den Vertrag, weil der Vergleich jede einseitige Änderung findet.

---

## Schritt 2: Instanz-Queue und Listener

* **Ziel:** Jede Instanz legt beim Start `chat.delivery.<UUID>` an (nicht durable,
  exclusive, nicht auto-delete, `x-max-length` 900, `x-overflow` drop-head), bindet sie an
  `chat.delivery` und hängt genau einen Consumer mit Prefetch 100 daran. Der Listener liest
  vorerst nur `roomId` und `id` aus dem JSON und loggt sie.
* **Warum an dieser Stelle:** Ohne Queue kommt keine Nachricht in der Instanz an, alles
  Weitere baut darauf auf. Die Queue-Eigenschaften entscheiden über S4 und S6 und lassen
  sich allein testen, ohne HTTP.
* **Auftrag an die KI:**
  > Erweitere `chat-service/.../config/RabbitConfig.java` um die Instanz-Queue aus
  > Spezifikation 3.1 und 3.3: Name `chat.delivery.` plus eine UUID, die **einmal** beim
  > Start erzeugt wird; nicht durable, exclusive, nicht auto-delete, `x-max-length` 900,
  > `x-overflow` `drop-head`; dazu die Bindung an `chat.delivery`. Lege eine
  > Listener-Container-Factory mit Prefetch 100 und genau einem Consumer an (Spezifikation 3.5).
  > Neue Klasse `service/DeliveryListener` mit `@RabbitListener` auf diese Queue: Sie bekommt
  > die Nachricht als `Message`, liest den Body als String und mit dem `ObjectMapper` die
  > Felder `roomId` und `id`, und loggt beide. Ungültiges JSON wird geloggt und verworfen,
  > nicht wiederholt. Setze in `application.yml` `spring.rabbitmq.requested-heartbeat: 10s`.
  > Schreib einen Integrationstest `DeliveryQueueIntegrationTest` mit Testcontainers.
* **Test:**
  * `DeliveryQueueIntegrationTest`: Queue existiert, Name beginnt mit `chat.delivery.`,
    Argumente `x-max-length` 900 und `x-overflow` `drop-head`, genau ein Consumer.
  * Im selben Test: Verbindung zu RabbitMQ trennen (`connectionFactory.resetConnection()`),
    warten, prüfen: **dieselbe** Queue ist wieder da und wieder an `chat.delivery` gebunden
    (Spezifikation 3.5).
  * Von Hand am Stack: `docker compose up -d --build`, dann
    `rabbitmqctl -q list_queues name consumers arguments | grep '^chat\.delivery\.'`
    → eine Queue, 1 Consumer.
* **Commit:** `feat: eigene Zustell-Queue pro chat-service-Instanz`
* **Geprüft und korrigiert:**
  * **Geprüft:** `RabbitConfig`, `DeliveryListener`, `QueueNames`, `application.yml` und
    `DeliveryQueueIntegrationTest` gelesen. `mvn test -pl chat-service` → 17 Tests, 0 Fehler.
    Zwei Gegenproben an `RabbitConfig` (danach jedes Mal aus einer Sicherung wiederhergestellt):
    * `x-max-length` 1000 statt 900 → `declaresInstanceQueueWithLimits` wird rot
      (`Argumente: [{"x-overflow","drop-head"},{"x-max-length",1000}]`). Gut.
    * `.exclusive()` entfernt → `redeclaresSameQueueAfterConnectionLoss` blieb **grün**.
      Eine Queue, die nicht exclusive ist, überlebt den Verbindungsabbruch einfach; der Test
      fand sie danach wieder und war zufrieden. Er hat also nicht bewiesen, dass die Queue
      gelöscht und neu angelegt wird.
  * **Geändert:** `redeclaresSameQueueAfterConnectionLoss` liest jetzt zusätzlich mit
    `rabbitmqctl list_queues name owner_pid`, welcher Verbindung die Queue gehört
    (neue Hilfsmethode `readOwnerConnection`). Vor dem Abbruch darf das Feld nicht leer sein
    (sonst ist die Queue nicht exclusive), und danach muss es eine andere Verbindung sein.
    Mit dieser Prüfung wird die Gegenprobe ohne `.exclusive()` rot
    (`Queue hat keine Besitzer-Verbindung, ist also nicht exclusive`). Beim ersten Versuch
    stürzte die neue Methode bei leerem `owner_pid` mit `StringIndexOutOfBounds` ab, weil
    `trim()` vor `substring` den Tab entfernte; Reihenfolge umgedreht. Danach wieder
    17 Tests, 0 Fehler.
  * **Bewusst gelassen:**
    * Die Kommentare «web-gateway-Instanzen» → «chat-service-Instanzen» in `RabbitConfig`
      und `QueueNames` und der angepasste Kommentar in `ChatServiceApplicationTest`. Sie
      gehen über den Auftrag hinaus, aber die alten Kommentare waren seit diesem Schritt
      falsch (Spezifikation 1.2). Nur Kommentare, kein Code.
    * `log.debug` für jede Nachricht im `DeliveryListener`. Bei Last viel Log, aber der
      Listener sendet ab Schritt 3 an die Clients, dann fällt die Zeile weg.
    * «Connection refused» im Testlog: Spring behält die Test-Kontexte, deren Listener
      versuchen nach dem Testende den gestoppten RabbitMQ-Container zu erreichen. Harmlos,
      die Tests bleiben grün.
  * **Nachtrag zu Schritt 1:** Der Commit `d1a16e6` hat keine Zeile
    `Co-Authored-By`, obwohl der Test von der KI stammt. Er ist schon gepusht; ihn zu ändern
    hiesse Force-Push, darum bleibt er so. Ab diesem Schritt trägt jeder Code-Commit die Zeile.

---

## Schritt 3: GET /stream mit Raumtabelle

* **Ziel:** `GET /stream?roomId=<uuid>` liefert `text/event-stream`; jede Nachricht geht
  als `id:` + `data:` nur an die Clients ihres Raums (Spezifikation 2.2, 4.1 bis 4.3).
  Keepalive alle 15 s.
* **Warum an dieser Stelle:** Jetzt kommen die Nachrichten in der Instanz an (Schritt 2);
  dieser Schritt bringt sie zum Client. Erst damit sind S3 und S5 überhaupt messbar.
  Das Nachholen (Schritt 4) braucht die Raumtabelle aus diesem Schritt.
* **Auftrag an die KI:**
  > Neue Klasse `service/StreamRegistry`: verwaltet `roomId → Liste von SseEmitter`.
  > Methoden `register(roomId, emitter)`, `unregister(roomId, emitter)`,
  > `sendToRoom(roomId, messageId, json)`. `sendToRoom` schickt ein Ereignis mit `id` =
  > Nachrichten-ID und `data` = unverändertem JSON-Text, nur an die Emitter dieses Raums.
  > Senden an einen Emitter ist `synchronized` auf diesen Emitter. Schlägt es mit
  > `IOException` fehl, wird nur dieser Emitter abgemeldet. Keepalive: eine
  > `@Scheduled`-Methode schickt alle 15 s einen Kommentar `keepalive` an alle Emitter.
  > Neuer `controller/StreamController` mit `GET /stream?roomId=`: `roomId` als UUID, sonst
  > 400; `new SseEmitter(0L)`; `onCompletion`, `onTimeout`, `onError` melden ab.
  > `DeliveryListener` ruft `sendToRoom` auf. Kein `event:`-Feld. Tests siehe unten.
* **Test:**
  * `StreamControllerTest`: fehlende/ungültige `roomId` → 400; gültige → 200 mit
    `Content-Type: text/event-stream`.
  * `TwoInstanceDeliveryIntegrationTest` (Pflichttest S3): zwei Spring-Kontexte gegen
    **denselben** RabbitMQ-Container, je auf zufälligem Port. An Instanz 1 ein Client in
    Raum A und einer in Raum B, an Instanz 2 ein Client in Raum A. Je 20 Nachrichten für
    Raum A über jede Instanz. Erwartet: beide A-Clients je 40 Ereignisse, 40 verschiedene
    IDs, Inhalte je Absender aufsteigend; B-Client 0 Ereignisse.
  * Von Hand: S3 und S5 mit den Befehlen aus Spezifikation 6.
* **Commit:** `feat: Nachrichten per Server-Sent Events an die Clients eines Raums`
* **Geprüft und korrigiert:** _wird nach dem Schritt ausgefüllt_

---

## Schritt 4: Nachhol-Puffer mit Last-Event-ID

* **Ziel:** Pro Raum die letzten 1000 Nachrichten im Speicher, höchstens 100 Räume.
  Neuverbindung mit `Last-Event-ID` liefert genau die fehlenden Nachrichten, danach live
  (Spezifikation 4.6).
* **Warum an dieser Stelle:** Braucht die Raumtabelle und den Sendeweg aus Schritt 3. Die
  Sperre muss Listener und Neuverbindung gemeinsam umfassen, darum wird beides in einem
  Schritt verbunden.
* **Auftrag an die KI:**
  > Neue Klasse `service/ReplayBuffer`: `LinkedHashMap` mit `accessOrder = true` und
  > `removeEldestEntry` für höchstens 100 Räume; pro Raum eine Liste der letzten 1000
  > Nachrichten (ID und JSON-Text). Alle Zugriffe über `synchronized`-Methoden.
  > Ändere `DeliveryListener` und `StreamController` so, dass Spezifikation 4.6 Schritt 1
  > bis 6 genau so abläuft: Unter **einer** Sperre pro Raum hängt der Listener die Nachricht
  > an den Puffer und sendet sie; dieselbe Sperre nimmt der Controller bei einer
  > Neuverbindung, sucht die `Last-Event-ID` im Puffer, sendet alles danach, meldet den
  > Emitter an und gibt die Sperre frei. Unbekannte ID: nichts nachliefern, Warnung loggen.
  > Ohne Header verhält sich alles wie bisher.
* **Test:**
  * `ReplayBufferTest` (Unit): nach ID suchen, alles danach in Reihenfolge; unbekannte ID →
    leer; 1001. Nachricht verdrängt die älteste; 101. Raum verdrängt den am längsten
    unbenutzten.
  * `ReconnectIntegrationTest` (Pflichttest S7): Client verbindet, bekommt 1 Nachricht,
    trennt; 50 Nachrichten; Neuverbindung mit `Last-Event-ID`; erwartet genau 50 Ereignisse,
    Inhalte 1..50 in Reihenfolge, keine ID doppelt; danach 1 neue Nachricht → kommt live an.
  * Von Hand: S7 mit den Befehlen aus Spezifikation 6.
* **Commit:** `feat: verpasste Nachrichten mit Last-Event-ID nachliefern`
* **Geprüft und korrigiert:** _wird nach dem Schritt ausgefüllt_

---

## Schritt 5: Herunterfahren und Heartbeat

* **Ziel:** Beim Herunterskalieren ist die Queue der gestoppten Instanz nach wenigen
  Sekunden weg (Spezifikation 3.4, S4).
* **Warum an dieser Stelle:** Erst seit Schritt 3 gibt es offene SSE-Verbindungen, die das
  Herunterfahren aufhalten können. Vorher wäre das Problem nicht sichtbar gewesen.
* **Auftrag an die KI:**
  > Setze in `chat-service/src/main/resources/application.yml` `server.shutdown: immediate`
  > mit einem Kommentar, warum (Spezifikation 3.4: offene Streams würden das graceful
  > Herunterfahren bis zu 30 s aufhalten, Docker schickt nach 10 s SIGKILL). Prüfe, dass
  > `spring.rabbitmq.requested-heartbeat: 10s` aus Schritt 2 noch drin ist. Sonst nichts ändern.
* **Test:** Von Hand am Stack:
  * `docker compose up -d --scale chat-service=2 --wait`, an jede Instanz einen Client hängen.
  * `time docker compose up -d --scale chat-service=1` → unter 10 s.
  * Gleich danach `rabbitmqctl -q list_bindings source_name destination_name | grep -c '^chat\.delivery[[:space:]]'` → 1.
  * `docker compose logs chat-service` → kein SIGKILL-Abbruch, Log „Shutdown“ der Instanz 2.
* **Commit:** `fix: offene Streams halten das Herunterfahren nicht auf`
* **Geprüft und korrigiert:** _wird nach dem Schritt ausgefüllt_

---

## Schritt 6: Abnahmeskript für S1 bis S8

* **Ziel:** `docs/abnahme.sh` führt alle Szenarien in der Reihenfolge des Auftrags auf
  einem Stack aus und gibt pro Szenario den gemessenen Wert aus.
* **Warum an dieser Stelle:** Erst jetzt ist alles gebaut. Das Skript ist die Prüfung, die
  die Lehrperson auch macht; was hier nicht besteht, besteht dort auch nicht.
* **Auftrag an die KI:**
  > Schreib `docs/abnahme.sh` (bash) mit den Befehlen aus Spezifikation Kapitel 6, ein
  > Abschnitt pro Szenario S1 bis S8, in dieser Reihenfolge, auf demselben Stack. Sende
  > Lastnachrichten aus **einem** Hilfscontainer mit einer Schleife, nicht mit einem
  > `docker run` pro Nachricht. Passwörter nur aus `.env`. Am Ende pro Szenario eine Zeile
  > `S<n>: erwartet ... gemessen ...`. Keine Logik raten: wo die Spezifikation einen Befehl
  > vorgibt, genau diesen verwenden.
* **Test:** Frischer Klon in einem anderen Ordner, `.env` aus `.env.example`,
  `bash docs/abnahme.sh` → alle acht Szenarien bestanden. Ergebnis in «Geprüft und korrigiert».
* **Commit:** `test: Abnahmeskript fuer die Live-Zustellung`
* **Geprüft und korrigiert:** _wird nach dem Schritt ausgefüllt_

---

## Schritt 7: README und PLANUNG.md nachführen

* **Ziel:** README-Tabelle «Stand» zeigt die Live-Zustellung; PLANUNG.md sagt, dass der
  `chat-service` `chat.delivery` konsumiert (Spezifikation 1.2).
* **Warum zum Schluss:** Erst wenn S1 bis S8 bestehen, stimmt die Beschreibung.
* **Auftrag an die KI:**
  > Ergänze in `README.md` die Tabelle «Stand»: chat-service liefert zusätzlich
  > `GET /stream?roomId=` (Server-Sent Events). Ergänze einen Abschnitt, wie man einen
  > Stream im Netz `chat-net` mit curl öffnet. In `PLANUNG.md` 3.4 und 3.5 einen Hinweis
  > (nicht den alten Text löschen), dass der Zustellweg jetzt am `chat-service` endet.
  > `.env.example` bleibt unverändert (Spezifikation 5: keine neuen Variablen).
* **Test:** `mvn clean test` im Wurzelverzeichnis ein Lauf grün (S1);
  `git ls-files .env` → leer.
* **Commit:** `docs: README und Planung fuer die Live-Zustellung nachfuehren`
* **Geprüft und korrigiert:** _wird nach dem Schritt ausgefüllt_

---

## Änderungen am Plan

_Noch keine. Jede Änderung kommt hier als eigener Eintrag mit Datum, betroffenem Schritt
und Änderungsgrund dazu, in einem eigenen Commit._
