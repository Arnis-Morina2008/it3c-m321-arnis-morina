Spezifikation Live-Zustellung

1 Zweck und Abgrenzung:

    1.1 Zweck:

        Jede Instanz des chat-service bindet beim Start eine eigene Queue an chat.delivery.

        Clients öffnen GET /stream?roomId=<uuid> und bekommen die Nachrichten ihres Raums live als Server-Sent Events (SSE).

        Nach einem Verbindungsabbruch meldet sich der Client mit dem Header Last-Event-ID zurück und bekommt genau die Nachrichten, die ihm fehlen.

    1.2 Abgrenzungen:

        Keine Datenbank im Anzeigeweg. Die Live-Zustellung funktioniert auch, wenn batch-writer und postgres gestoppt sind (S5). Darum liest der chat-service für das Nachholen nicht aus der Datenbank (Begründung in 4.6).

        Keine Historie beim ersten Öffnen. Wer sich ohne Last-Event-ID verbindet, bekommt nur neue Nachrichten. Die „letzten 50 Nachrichten“ aus PLANUNG.md sind ein späterer Schritt.

        Keine Rechteprüfung. Wer /stream erreicht, darf jeden Raum lesen. Nur das interne Netz erreicht den Dienst; später prüft das Gateway das Token.

        Kein Exactly-once. Wie in Bewertung 1 gilt At-least-once auf dem Broker. Innerhalb einer Instanz sorgt die Konstruktion dafür, dass ein Client jede Nachricht genau einmal bekommt (4.1). Nach einem Absturz mitten in der Zustellung kann es Duplikate geben; der Client erkennt sie an der id.

        Abweichung von PLANUNG.md 3.4/3.5: Dort konsumiert das web-gateway chat.delivery. Der Auftrag verlangt den Stream am chat-service. Ein späteres Gateway reicht /stream nur noch durch.

2 Vertrag:

    2.1 Was am Exchange ankommt und woher ich das weiss:

        Im Code: MessagePublisher.publish() ruft rabbitTemplate.convertAndSend(QueueNames.DELIVERY_EXCHANGE, "", message) auf, direkt nach dem Senden in chat.persist. Es ist dasselbe ChatMessage-Objekt mit demselben Jackson2JsonMessageConverter. Also ist der Body Byte für Byte gleich aufgebaut wie in chat.persist (Format: spec-batch-writer.md 2.3).

        Am laufenden Stack, geprüft am 12. November 2026 mit dem Stack aus Bewertung 1 (noch ohne neuen Code): Eine Test-Queue von Hand binden, eine Nachricht senden, anschauen:

        "docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> declare queue name=probe durable=false
        docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> declare binding source=chat.delivery destination=probe
        # Nachricht per POST /messages senden (Befehl in Abschnitt 6)
        docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> -f raw_json get queue=probe
        docker compose exec rabbitmq rabbitmqadmin -u chat -p <passwort> delete queue name=probe"

        Erwartet: exchange: "chat.delivery", routing_key: "", content_type: application/json, Header __TypeId__, Payload mit id, roomId, senderId, senderName, content, sentAt.

        Ergebnis (gekürzt auf die wichtigen Felder):

        "{
          "exchange": "chat.delivery",
          "routing_key": "",
          "properties": {
            "delivery_mode": 2,
            "headers": { "__TypeId__": "ch.benedict.m321.chatservice.dto.ChatMessage" },
            "content_encoding": "UTF-8",
            "content_type": "application/json"
          },
          "payload": "{\"id\":\"c7141be2-dc2e-465a-b1fc-e0a66600a9b6\",\"roomId\":\"11111111-1111-1111-1111-111111111111\",\"senderId\":\"anna\",\"senderName\":\"Anna Muster\",\"content\":\"Hallo zusammen!\",\"sentAt\":\"2026-11-12T18:01:07.117972678Z\"}"
        }"

        Das ist genau das Format aus chat.persist (spec-batch-writer.md 2.1): dieselben sechs Felder in derselben Reihenfolge, sentAt mit Nanosekunden, derselbe Header. Unterschied nur: exchange ist chat.delivery statt "" und routing_key ist leer.

    2.2 Was am Stream hinausgeht:

        Endpunkt: GET /stream?roomId=<uuid> am chat-service, Port 8080, nur im Netz chat-net.

        Fall: roomId fehlt oder ist keine UUID, Antwort: 400 Bad Request
        Fall: sonst, Antwort: 200, Content-Type: text/event-stream, Verbindung bleibt offen

        Ein Ereignis pro Nachricht:

        id:6e59eeb3-15c1-461f-8b53-83791524eac9
        data:{"id":"6e59eeb3-...","roomId":"a1b2c3d4-...","senderId":"anna","senderName":"Anna","content":"Hallo","sentAt":"2026-11-12T10:00:00.123456789Z"}

        Spring (SseEmitter) schreibt id: und data: ohne Leerzeichen nach dem Doppelpunkt. Laut SSE-Standard ist das gleichwertig mit id: 6e59…: Ein einzelnes Leerzeichen direkt nach dem Doppelpunkt gehört nicht zum Wert, ein Browser liest beides gleich. Auch grep '^id:' in Kapitel 6 findet beide Schreibweisen. Wer die Zeilen selbst zerlegt, muss ein Leerzeichen also erlauben, aber nicht verlangen.

        id: ist die Nachrichten-ID. Der Browser (EventSource) merkt sie sich und schickt sie bei der Neuverbindung automatisch als Header Last-Event-ID.

        data: ist der unveränderte Body aus der Queue. Der Listener liest das JSON nur, um roomId und id herauszuholen, schickt aber den Originaltext weiter. So kann das Format nicht von chat.persist abweichen – es wird gar nicht neu erzeugt.
        Kein event:-Feld: Ereignisse heissen damit standardmässig message.

        Lebenszeichen: Alle 15 s eine Kommentarzeile : keepalive. Kommentare sind keine Ereignisse, ein Client sieht sie nicht. Sie dienen dazu, tote Verbindungen zu bemerken (das Schreiben schlägt fehl, die Verbindung wird abgemeldet).

        Verbindungsbestätigung: Gleich beim Verbinden schickt der Dienst eine Kommentarzeile :connected. Grund: Spring schickt Status 200 und Content-Type: text/event-stream erst mit dem ersten Schreiben. Ohne diesen Kommentar sähe ein Client bis zur ersten Nachricht oder bis zum ersten Keepalive (bis zu 15 s) nicht einmal die Antwort-Kopfzeilen und wüsste nicht, ob er verbunden ist. Ein Test oder Prüfskript wüsste dann nicht, ab wann er senden darf, ohne dass die ersten Nachrichten verloren gehen. Wie :keepalive ist :connected kein Ereignis und wird von Clients nicht angezeigt.

        Neuverbindung: gleicher Endpunkt, zusätzlich Header Last-Event-ID: <uuid> (Verhalten 4.6).

3 Queues:

    3.1 Übersicht:

        Name: chat.delivery (Exchange, fanout), Wer legt an: chat-service, Lebensdauer: durable, Grenze: -, beim Erreichen: -

        Name: chat.delivery.<zufällige UUID> , Wer legt an: jede chat-service-Instanz beim Start, Lebensdauer: so lange wie die Verbindung der Instanz zu RabbitMQ, Grenze: x-max-length 900 bereit + Prefetch 100, beim Erreichen: älteste Nachricht wird verworfen (x-overflow: drop-head)

        Name: chat.persist, chat.dlq, Wer legt an: unverändert aus Bewertung 1, Lebensdauer: durable, Grenze: -, beim Erreichen: -

    3.2 Warum jede Instanz eine eigene Queue braucht:

        Ein SSE-Client hängt an genau einer Instanz. Würden beide Instanzen an einer gemeinsamen Queue hängen, wäre das Competing Consumers wie beim batch-writer: jede Nachricht ginge nur an eine Instanz, und die Clients der anderen Instanz sähen sie nie (S3 wäre nicht bestanden). Mit einer Queue pro Instanz legt der Fanout-Exchange eine Kopie in jede Queue. Jede Instanz sieht jede Nachricht und entscheidet selbst, welchen ihrer Clients sie sie gibt.

    3.3 Eigenschaften der Instanz-Queue und warum:

        Name chat.delivery. + UUID: Mit rabbitmqctl erkennt man sofort, wozu die Queue gehört. Die UUID wird bei jedem Start neu erzeugt. Mit dem Hostnamen als Namen könnte ein neu gestarteter Container auf die Queue seines toten Vorgängers treffen, die RabbitMQ noch nicht aufgeräumt hat (RESOURCE_LOCKED).

        nicht durable: Ohne Instanz gibt es keine Clients, für die die Nachrichten bestimmt wären. Nach einem Neustart von RabbitMQ wäre die Queue ohnehin nutzlos.

        exclusive: Die Queue gehört der Verbindung, die sie angelegt hat. RabbitMQ löscht sie, sobald diese Verbindung weg ist. Genau das verlangt S4.

        nicht auto-delete: auto-delete löscht die Queue, sobald der letzte Consumer abmeldet. Spring startet einen Consumer nach einem Fehler neu; in dieser Lücke wäre die Queue samt Bindung weg. exclusive allein genügt.

        Ein Consumer pro Queue (concurrency 1): Nur so kommen die Nachrichten in der Reihenfolge der Queue bei den Clients an (S3, Reihenfolge eines Absenders).

    3.4 Was mit der Queue passiert, wenn die Instanz weg ist (S4)

        Sauberes Stoppen (--scale chat-service=1 schickt SIGTERM): Spring schliesst beim Herunterfahren die Verbindung, RabbitMQ löscht die exclusive Queue sofort.

        Hartes Ende (SIGKILL nach 10 s, Absturz): Die TCP-Verbindung wird nicht sauber geschlossen. RabbitMQ bemerkt das erst über den Heartbeat. Der Vorgabewert ist 60 s, nach zwei verpassten Heartbeats wäre das zu spät für die 30 s aus S4. Deshalb spring.rabbitmq.requested-heartbeat: 10s → RabbitMQ erkennt eine tote Verbindung nach höchstens etwa 20 s und löscht die Queue.

        Warum hartes Ende realistisch ist: Spring Boot 3.5 fährt standardmässig „graceful“ herunter und wartet bis zu 30 s auf laufende Anfragen. Offene SSE-Verbindungen sind laufende Anfragen und enden nie von selbst. Docker schickt aber schon nach 10 s SIGKILL. Danach braucht RabbitMQ über den Heartbeat noch etwa 20 s, zusammen also etwa 30 s: genau an der Grenze von S4, zu knapp.

        Ein @PreDestroy, das die Streams schliesst, hilft nicht. Spring fährt in dieser Reihenfolge herunter: 1. ContextClosedEvent, 2. Webserver graceful stoppen (wartet auf offene Anfragen), 3. erst dann Beans zerstören und @PreDestroy aufrufen. @PreDestroy käme also erst nach dem Warten dran. Die Streams müssen vor dem Webserver geschlossen werden.

        Entscheid: server.shutdown: immediate in application.yml. Der Webserver trennt beim Herunterfahren alle Verbindungen sofort. Danach stoppt Spring den Listener und schliesst die Verbindung zu RabbitMQ sauber, RabbitMQ löscht die exclusive Queue sofort. Der Container ist nach etwa 1–2 s weg, weit unter den 10 s bis SIGKILL. Der Heartbeat bleibt trotzdem auf 10 s, für echte Abstürze.

        Verworfen: Streams beim ContextClosedEvent schliessen. Das kommt zwar vor dem Webserver, hat aber eine Lücke: Ein Browser mit EventSource verbindet sich nach dem Schliessen automatisch neu. Kommt diese neue Verbindung an, bevor der Webserver keine Anfragen mehr annimmt, wartet das graceful Herunterfahren wieder auf einen offenen Stream. Ausserdem braucht es dafür eine eigene Klasse, immediate ist eine Zeile Konfiguration.

        Preis von immediate: Ein POST /messages, der genau im Moment des Herunterfahrens läuft, bricht ab. Der Absender bekommt keine 202 und weiss nicht, ob die Nachricht veröffentlicht wurde. Weil ein POST nur Millisekunden dauert, ist das selten, und der Absender kann es erneut versuchen (schlimmstenfalls ein Duplikat mit neuer id). Das nehme ich in Kauf.

        Dass der Container wirklich innerhalb von 10 s stoppt und die Queue danach sofort weg ist, wird in Schritt 5 des Plans geprüft (docker compose up -d --scale chat-service=1, dann docker compose logs und rabbitmqctl list_queues).

    3.5 Die Grenze: was eine hängende Instanz mit RabbitMQ macht (S6)

        Eine angehaltene Instanz (docker pause) holt nichts mehr ab. Ihre Queue wächst mit jeder Nachricht, die irgendeine andere Instanz annimmt. Ohne Grenze füllt sie den Speicher von RabbitMQ. Ab dem Speicher-Alarm blockiert RabbitMQ alle Publisher – dann nimmt auch die gesunde Instanz keine Nachrichten mehr an. Eine hängende Instanz würde so das ganze System anhalten.

        Grenze: x-max-length = 900 und prefetch = 100. rabbitmqctl list_queues messages zählt bereite und unbestätigte Nachrichten, x-max-length zählt nur bereite. 900 + 100 = höchstens 1000 (S6).

        x-overflow: drop-head (Vorgabe von RabbitMQ): Ist die Queue voll, fällt die älteste Nachricht heraus. Verworfen, nicht in eine DLQ: Die Nachricht ist über chat.persist ohnehin gespeichert. Nur die Live-Kopie für diese eine Instanz fehlt.

        Verworfen: reject-publish. Dann würde die neueste Nachricht abgewiesen. Mit Publisher Confirms bekäme der Absender einen Fehler, obwohl die anderen Instanzen die Nachricht problemlos zustellen könnten. Für einen Chat ist die neueste Nachricht wichtiger als die älteste.

        Nach dem Fortsetzen (docker unpause): Zwei Fälle, beide in Ordnung:

            Die Pause war kürzer als ca. 20 s: Die Verbindung lebt noch, die Instanz arbeitet die bis zu 1000 Nachrichten in ihrer Queue ab und stellt danach neue zu.

            Die Pause war länger: RabbitMQ hat die Verbindung über den Heartbeat geschlossen und die Queue gelöscht. Spring baut die Verbindung neu auf und legt die Queue samt Bindung neu an (RabbitAdmin deklariert beim Wiederverbinden erneut). Neue Nachrichten kommen wieder an. Die UUID für den Namen wird einmal beim Start erzeugt, nicht bei jeder Deklaration. Dass die Queue beim Neuverbinden wirklich denselben Namen behält und wieder an chat.delivery gebunden ist, wird in Schritt 2 des Plans geprüft.

4 Verhalten:

    4.1 Normalfall:

        1. Client öffnet GET /stream?roomId=A. Der Controller erzeugt einen SseEmitter ohne Timeout (new SseEmitter(0L); der Vorgabewert von Spring MVC würde die Verbindung nach 30 s schliessen) und meldet ihn in einer Tabelle roomId → Liste von Emittern an.

        2. Bei onCompletion, onTimeout, onError meldet sich der Emitter selbst wieder ab.

        3. Der Listener an der Instanz-Queue bekommt eine Nachricht, liest roomId und id, hängt sie an den Nachhol-Puffer des Raums (4.6) und schickt sie an jeden Emitter dieses Raums – und nur an diese.

        4. Schlägt das Senden an einen Emitter fehl (IOException, Client weg), wird nur dieser Emitter abgemeldet. Die anderen Clients bekommen die Nachricht trotzdem.

    4.2 Nur die Clients des Raums (S3, Client in Raum B):

        Die Tabelle aus 4.1 ist nach roomId aufgeteilt. Eine Nachricht für Raum A wird nur an die Liste von Raum A geschickt. Ein Client in Raum B steht in einer anderen Liste und bekommt nichts – ausser den Keepalive-Kommentaren, die keine Ereignisse sind.

        Warum nicht pro Raum eine eigene Queue oder ein Topic-Exchange? Das würde RabbitMQ-seitig filtern, bräuchte aber pro Raum und Instanz eine Bindung, die beim Verbinden und Trennen von Clients angelegt und gelöscht werden muss. Bei der Last aus PLANUNG.md kostet das Filtern im Speicher fast nichts. Der einfache Weg ist hier der richtige.

    4.3 Zwei Instanzen (S3):

        Zwei Instanzen → zwei Queues am Fanout → jede Nachricht liegt in beiden.

        Je 200 Nachrichten über jede Instanz: Beide Instanzen bekommen alle 400, jeder ihrer Raum-A-Clients bekommt alle 400.

        Genau einmal: Jeder Emitter steht genau einmal in der Liste; jede Nachricht liegt genau einmal in der Queue der Instanz.

        Reihenfolge eines Absenders: Der Absender schickt nacheinander (wartet auf 202). Der chat-service veröffentlicht also in dieser Reihenfolge, die Queue ist FIFO, ein einziger Consumer arbeitet sie der Reihe nach ab, und das Senden an einen Emitter ist synchronized (sonst könnten sich Listener und Keepalive-Thread beim Schreiben überholen). Zwischen verschiedenen Absendern gibt es keine garantierte Reihenfolge (PLANUNG.md, offener Punkt 3).

    4.4 Skalieren zurück auf eine Instanz (S4):

        Siehe 3.4. Vorher zwei Queues mit je einem Consumer. Beim sauberen Stoppen ist die zweite nach wenigen Sekunden weg (server.shutdown: immediate), bei einem Absturz nach höchstens etwa 20 s (Heartbeat). Beides liegt unter den 30 s aus S4. chat.persist und chat.dlq sind durable und werden nicht angefasst.

    4.5 batch-writer und postgres gestoppt (S5):

        MessagePublisher.publish() legt die Nachricht in chat.persist und in chat.delivery. chat.persist hat keinen Verbraucher und füllt sich – das ist gewollt, es ist der Puffer aus Bewertung 1. Der Anzeigeweg hängt an keiner Stelle an der Datenbank: Die Nachricht ist nach Millisekunden beim Client. Nach dem Neustart arbeitet der batch-writer chat.persist ab, alle 100 stehen in der Tabelle (Verhalten aus spec-batch-writer.md 4.2).

    4.6 Neuverbindung mit Last-Event-ID (S7):

        Nachhol-Puffer: Jede Instanz hält pro Raum die letzten 1000 Nachrichten im Speicher, in der Reihenfolge, in der sie aus der Queue kamen. Weil jede Instanz über den Fanout jede Nachricht bekommt, hat jede Instanz denselben Puffer – der Client darf sich also auch bei einer anderen Instanz neu verbinden.

        Ablauf bei GET /stream?roomId=A mit Last-Event-ID: X:

            1. Sperre auf den Raum A setzen (dieselbe Sperre nimmt der Listener in 4.1 Schritt 3).

            2. Im Puffer von A die Nachricht X suchen.

            3. Gefunden: alle Nachrichten nach X in Pufferreihenfolge senden.

            4. Nicht gefunden (zu alt oder Instanz neu gestartet): nichts nachliefern und eine Warnung loggen. Alles zu senden würde Duplikate erzeugen; der Client muss dann die Historie neu laden (nicht Teil dieses Auftrags).

            5. Den Emitter für Raum A anmelden.

            6. Sperre lösen.

        Warum unter einer Sperre: Ohne Sperre könnte zwischen Schritt 3 und 5 eine Nachricht ankommen. Sie wäre weder im Nachholen noch live – sie fehlte. Oder sie käme in beidem – doppelt. Mit der Sperre gibt es diesen Zwischenraum nicht. Ergebnis für S7: genau die 50 fehlenden, in Reihenfolge, keine doppelt, danach live.

        Warum Speicher und nicht die Datenbank:

            Die Live-Zustellung soll ohne Datenbank laufen (S5). Mit Nachholen aus der Datenbank wäre das Nachholen bei einem Datenbankausfall kaputt.

            Die Datenbank ist dem Live-Strom hinterher (Stapel alle bis zu 200 ms). Beim Wechsel von „aus der Datenbank“ zu „live“ entstünde genau die Lücke bzw. Dopplung von oben – nur viel schwerer zu schliessen.

            Der chat-service bräuchte Datenbank-Zugangsdaten und einen Verbindungspool.

        Grenzen des Puffers (ehrlich benannt):

            Nach einem Neustart der Instanz ist er leer.

            Hat eine hängende Instanz Nachrichten per drop-head verloren (3.5), fehlen sie auch in ihrem Puffer.

            Speicher: 1000 Nachrichten × ~300 Byte = ~300 KB pro Raum. Ohne Grenze wüchse das mit jedem neuen Raum weiter; 1000 Räume wären schon ~300 MB.

        Entscheid: höchstens 100 Räume im Nachhol-Puffer, also höchstens ~30 MB. Die Puffer liegen in einer LinkedHashMap mit Zugriffsreihenfolge (accessOrder = true). Kommt ein 101. Raum dazu, fällt der Raum heraus, der am längsten keine Nachricht bekommen hat und nach dem am längsten niemand gefragt hat (removeEldestEntry). Weil eine LinkedHashMap mit Zugriffsreihenfolge sich schon beim Lesen verändert, laufen alle Zugriffe über synchronized-Methoden einer einzigen Klasse.

        Folgen des Entscheids:

            Herausgeworfen wird nur der Nachhol-Puffer, nicht die Tabelle der offenen Streams aus 4.1. Live-Clients eines herausgeworfenen Raums bekommen weiter alle neuen Nachrichten; der Puffer des Raums beginnt mit der nächsten Nachricht neu.

            Verbindet sich ein Client eines herausgeworfenen Raums neu, findet er seine Last-Event-ID nicht. Das ist der Fall «nicht gefunden» aus Schritt 4: nichts nachliefern, live weiter.

            Warum 100: Die Szenarien brauchen zwei Räume (A und B). 100 lassen viel Reserve und halten den Speicher klein. Wenn es später echte Räume aus der Tabelle room gibt, muss die Zahl neu überlegt werden.

    4.7 RabbitMQ nicht erreichbar:

        Der Listener-Container von Spring versucht die Verbindung alle paar Sekunden neu und legt beim Erfolg die Instanz-Queue neu an. Offene Streams bleiben offen, bekommen aber in der Zwischenzeit nichts. Das Senden (POST /messages) antwortet wie bisher mit 503.

    4.8 Langsamer Client

        Ein Client, der nicht liest, füllt irgendwann den Sendepuffer seiner TCP-Verbindung. Dann blockiert send() und mit ihm der eine Listener-Thread – und damit alle Clients der Instanz. Die Queue wächst dann bis zur Grenze aus 3.5, die Instanz verhält sich wie eine hängende.

        Entscheid: bewusst nicht gelöst, aus drei Gründen:

            RabbitMQ ist trotzdem geschützt. Die Grenze aus 3.5 (900 + 100) gilt auch hier; die anderen Instanzen und der Schreibweg laufen weiter.

            Die Szenarien haben keinen langsamen Client. Die Clients des Prüfskripts lesen sofort.

            Eine Lösung wäre nicht einfach: Damit ein Client die anderen nicht aufhält, bräuchte jeder Client eine eigene Warteschlange und einen eigenen Sende-Thread, plus eine Regel, wann ein Client getrennt wird. Das ist mehr Code als die ganze restliche Zustellung und gehört nicht in diesen Auftrag (CLAUDE.md: keine Vorrats-Abstraktionen).

        Was ein langsamer Client heute anrichtet: Die Clients derselben Instanz bekommen Nachrichten verspätet oder, wenn die Queue überläuft, gar nicht (drop-head). Clients an anderen Instanzen merken nichts. Das steht als offener Punkt in Kapitel 7.

5 Konfiguration:

    Keine neuen Umgebungsvariablen. Alle neuen Werte hängen zusammen (900 + 100 = 1000) und sollen nicht einzeln verstellt werden. .env.example bleibt unverändert.

    Wert: x-max-length 900, Wo: Deklaration der Instanz-Queue, Warum: zusammen mit Prefetch ≤ 1000 (S6)

    Wert: Prefetch 100, Wo: Listener-Container der Instanz-Queue, Warum:siehe oben; klein, weil Zustellung schnell ist

    Wert: x-overflow drop-head, Wo: Instanz-Queue, Warum: älteste Live-Kopie opfern, nie den Absender blockieren

    Wert: Heartbeat 10 s, Wo: spring.rabbitmq.requested-heartbeat, Warum: tote Instanz nach ~20 s erkannt (S4)

    Wert: Emitter-Timeout 0 (keiner), Wo: new SseEmitter(0L), Warum: Vorgabe 30 s würde Streams abbrechen

    Wert: Keepalive 15 s, Wo: @Scheduled, Warum: tote Clients bemerken

    Wert: Puffer 1000 pro Raum, Wo: Nachhol-Puffer, Warum: 	deckt S7 (50) mit Reserve

    Wert: höchstens 100 Räume im Nachhol-Puffer, Wo: Nachhol-Puffer (removeEldestEntry), Warum: Speicher bleibt unter ~30 MB (4.6)

    Wert: server.shutdown: immediate, Wo: application.yml, Warum: Streams halten das Herunterfahren nicht auf, Queue ist sofort weg (3.4, S4)

    docker-compose.yml: chat-service hat keinen container_name und kein ports:. Compose nummeriert die Instanzen: it3c-m321-arnis-morina-chat-service-1, -2. Über diese Namen ist jede Instanz im Netz chat-net einzeln erreichbar.

6 Abnahmekriterien und Messbefehle:

    Alle Befehle im Wurzelverzeichnis, .env aus .env.example. Projektname = Ordnername, also Container it3c-m321-arnis-morina-chat-service-1 usw. (docker compose ps zeigt sie). Die rabbitmqctl-Befehle aus S4 und S6 sind am 12. November 2026 am Stack aus Bewertung 1 gelaufen, mit zwei von Hand angelegten Queues chat.delivery.probe1 und chat.delivery.probe2 als Ersatz für die Instanz-Queues: Zählung der Bindungen 2, nach dem Löschen 0; chat.persist und chat.dlq gefunden (2); die S6-Schleife zeigte nach 3 gesendeten Nachrichten Maximum: 3. rabbitmqctl gibt eine Kopfzeile aus (source_name destination_name), die grep wegfiltert. Gegen den fertigen Code laufen die Befehle erst in Schritt 6 des Plans, zusammengefasst zu einem Skript docs/abnahme.sh.

    "# Hilfsvariablen
    CS1=it3c-m321-arnis-morina-chat-service-1
    CS2=it3c-m321-arnis-morina-chat-service-2
    ROOM_A=11111111-1111-1111-1111-111111111111
    ROOM_B=22222222-2222-2222-2222-222222222222

    # Client an einer bestimmten Instanz, schreibt alle Ereignisse in eine Datei
    docker run --rm --network chat-net curlimages/curl:8.10.1 -sN \
        "http://$CS1:8080/stream?roomId=$ROOM_A" > client-a1.txt &

    # Nachricht über eine bestimmte Instanz
    docker run --rm --network chat-net curlimages/curl:8.10.1 -s -X POST "http://$CS1:8080/messages" \
        -H "Content-Type: application/json" \
        -d '{"roomId":"'$ROOM_A'","senderId":"s1","senderName":"S1","content":"1"}'

    # Queues und Bindungen
    docker compose exec rabbitmq rabbitmqctl -q list_bindings source_name destination_name
    docker compose exec rabbitmq rabbitmqctl -q list_queues name messages consumers"

    Für Lastszenarien (200, 5000 Nachrichten) lohnt sich ein einziger Hilfscontainer mit einer Schleife statt 5000 docker run.

    S1: Kriterium: mvn clean test ein Lauf, grün, mit echtem RabbitMQ, Messbefehl: mvn clean test → BUILD SUCCESS, Failures: 0, Errors: 0

    S2: Kriterium: frischer Klon, .env aus .env.example: alle Dienste laufen, keiner veröffentlicht einen Port, Messbefehl: docker compose up -d --build --wait → Exit 0; docker compose ps --format "{{.Service}} {{.Ports}}" → kein ->

    S3: Kriterium: 	2 Instanzen; Client A1 an CS1, A2 an CS2, B an CS1. Je 200 Nachrichten für A über CS1 (Absender s1, Inhalt 1..200) und CS2 (s2). A1 und A2: je 400 Ereignisse, 400 verschiedene id, Inhalte von s1 und s2 je aufsteigend. B: 0 Ereignisse, Messbefehl: grep -c '^id:' client-a1.txt → 400; grep '^id:' client-a1.txt | sort -u | wc -l → 400; grep -c '^id:' client-b.txt → 0

        Reihenfolge prüfen (für s1 und s2 je einmal, für client-a1.txt und client-a2.txt): Die Inhalte eines Absenders herausziehen und kontrollieren, dass sie streng aufsteigend sind. sort -n -c -u gibt nichts aus und endet mit 0, wenn die Liste sortiert ist und keine Zahl doppelt vorkommt; sonst meldet es die erste Zeile, die nicht passt («disorder»).

        "grep '^data:.*\"senderId\":\"s1\"' client-a1.txt | sed 's/.*\"content\":\"\([0-9]*\)\".*/\1/' > s1-a1.txt
        wc -l < s1-a1.txt                                  → 200
        sort -n -c -u s1-a1.txt && echo aufsteigend        → aufsteigend"

    S4: Kriterium: 	vorher 2 Queues an chat.delivery mit je 1 Consumer; nach --scale chat-service=1 nach ≤ 30 s nur noch 1; chat.persist, chat.dlq vorhanden, Messbefehl:

        Vorher, bei zwei Instanzen: Bindungen an chat.delivery zählen, dann die Instanz-Queues mit ihren Consumern anzeigen.

        "docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name | grep -c '^chat\.delivery[[:space:]]'
        → 2
        docker compose exec -T rabbitmq rabbitmqctl -q list_queues name consumers | grep '^chat\.delivery\.'
        → 2 Zeilen, jede endet mit 1"

        Herunterskalieren und 30 s lang alle 5 s zählen. Spätestens die letzte Ausgabe muss 1 sein.

        "docker compose up -d --scale chat-service=1
        for i in 1 2 3 4 5 6; do
          sleep 5
          docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name | grep -c '^chat\.delivery[[:space:]]'
        done
        → letzte Zeile: 1"

        chat.persist und chat.dlq sind noch da:

        "docker compose exec -T rabbitmq rabbitmqctl -q list_queues name | grep -cE '^chat\.(persist|dlq)$'
        → 2"

    S5: Kriterium: docker compose stop batch-writer postgres, 100 Nachrichten: nach ≤ 5 s 100 Ereignisse beim Client; nach docker compose start postgres batch-writer 100 neue Zeilen, Messbefehl: grep -c '^id:'; docker compose exec postgres psql -U chat -d chat -tA -c "SELECT count(*) FROM message;"

    S6: Kriterium: docker pause $CS2, 5000 Nachrichten über CS1, docker unpause $CS2: Queue von CS2 nie > 1000; Client an CS1 bekommt 5000; danach stellt CS2 neue Nachrichten zu, Messbefehl:

        Queue-Länge messen: Die Schleife in einem zweiten Terminal starten, bevor gesendet wird. Sie fragt 120 s lang jede Sekunde alle Instanz-Queues ab und merkt sich den grössten Wert. Welche Queue zu CS2 gehört, muss man dafür nicht wissen: Die Queue der laufenden Instanz bleibt nahe bei 0, das Maximum kommt also immer von der angehaltenen. Das Senden der 5000 Nachrichten muss innerhalb dieser 120 s fertig sein.

        "max=0
        for i in $(seq 1 120); do
          n=$(docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages | grep '^chat\.delivery\.' | awk '{print $2}' | sort -n | tail -1)
          n=${n:-0}
          echo "$i s: $n"
          if [ "$n" -gt "$max" ]; then max=$n; fi
          sleep 1
        done
        echo "Maximum: $max"
        → Maximum: höchstens 1000"

        Client an CS1: grep -c '^id:' client-s6.txt → 5000

        Nach docker unpause $CS2: neuen Client an CS2 starten (Befehl oben, Datei client-s6-cs2.txt), 1 Nachricht über CS1 senden, dann grep -c '^id:' client-s6-cs2.txt → 1

        Warum docker pause und nicht docker compose pause: docker compose pause chat-service hält alle Instanzen des Dienstes an. Dann gäbe es keine laufende Instanz mehr, über die die 5000 Nachrichten gesendet und an deren Client sie empfangen werden könnten. docker pause mit dem Containernamen hält genau eine Instanz an.

    S7: Kriterium: Client verbindet, bekommt 1 Nachricht (ID merken), trennt; 50 Nachrichten; nach 10 s Neuverbindung mit Header: genau 50, in Reihenfolge, keine doppelt, danach live, Messbefehl: curl -sN -H "Last-Event-ID: <id>" "http://$CS1:8080/stream?roomId=$ROOM_A" → 50 id:-Zeilen, Inhalte 1..50 aufsteigend; dann 1 Nachricht senden → 51. Ereignis

    S8: Kriterium: CLAUDE.md: keine Streams, Kommentar über jeder Klasse/Methode, .env nicht im Repo, Messbefehl: grep -rnE "\.stream\(|Stream\.|Collectors" chat-service/src → nichts (Achtung: SseEmitter/event-stream erzeugen keinen Treffer, .stream( schon); git ls-files .env → leer

    Vertrag: Kriterium: Ereignis: id: = id im JSON, data: = Body wie in chat.persist, Messbefehl: Ereignis aus S3 neben rabbitmqadmin get queue=chat.persist (bei gestopptem batch-writer) legen

    6.1 Automatische Tests (chat-service/src/test, Testcontainers-RabbitMQ):

        Verhalten: Instanz-Queue ist exclusive, nicht durable, x-max-length 900, an chat.delivery gebunden, Test: DeliveryQueueIntegrationTest

        Verhalten: Zwei Instanzen, Raum A/B, alles genau einmal und in Reihenfolge (S3), Test: TwoInstanceDeliveryIntegrationTest: zwei Spring-Kontexte gegen denselben RabbitMQ-Container, je ein SSE-Client

        Verhalten: Nachrichten anderer Räume kommen nicht an, Test: im selben Test oder als Unit-Test der Raumtabelle

        Verhalten: Neuverbindung mit Last-Event-ID liefert genau die fehlenden (S7), Test: ReconnectIntegrationTest

        Verhalten: Unbekannte Last-Event-ID → nichts nachliefern, live weiter, Test: Unit-Test des Nachhol-Puffers

        Verhalten: roomId fehlt/ungültig → 400, Test: StreamControllerTest

7 Offene Punkte und Prüfungen im Umsetzungsplan:

    7.1 Prüfungen, die erst mit laufendem Code gehen (gehören in docs/plan-live-zustellung.md):

        Schritt 1: Vertrag am Exchange nachprüfen (2.1). Bereits am 12. November 2026 erledigt, Ergebnis steht in 2.1. Im Plan wird der Schritt beim ersten Code noch einmal wiederholt: Die echte Instanz-Queue muss dasselbe zeigen wie die Probe-Queue.

        Schritt 2: Instanz-Queue und Listener. Prüfen: Queue ist exclusive, nicht durable, x-max-length 900, an chat.delivery gebunden; nach einem Verbindungsabbruch zu RabbitMQ hat sie wieder denselben Namen und dieselbe Bindung (3.5).

        Schritt 3: GET /stream mit Raumtabelle. Prüfen: Content-Type text/event-stream, Nachricht kommt nur im richtigen Raum an, 400 bei fehlender roomId.

        Schritt 4: Nachhol-Puffer mit Last-Event-ID und Grenze von 100 Räumen (4.6). Prüfen: genau die fehlenden, keine doppelt; unbekannte ID liefert nichts nach.

        Schritt 5: Herunterfahren mit server.shutdown: immediate und Heartbeat 10 s (3.4). Prüfen: Container stoppt in unter 10 s, Queue ist sofort weg.

        Schritt 6: Abnahmeskript docs/abnahme.sh mit S1 bis S8, einmal vollständig ausgeführt (6).

    7.2 Bewusst offen:

        Langsamer Client (4.8): blockiert die Zustellung seiner Instanz.

        Nachhol-Puffer nach Neustart leer und bei drop-head lückenhaft (4.6).

        PLANUNG.md nachführen: Konsument von chat.delivery ist jetzt der chat-service, nicht das web-gateway (1.2).
