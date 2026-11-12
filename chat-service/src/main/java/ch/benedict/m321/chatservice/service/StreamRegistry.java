package ch.benedict.m321.chatservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Die Raumtabelle dieser Instanz: welche offenen Stream-Verbindungen
 * (SseEmitter) gehören zu welchem Raum (Spezifikation 4.1 und 4.2).
 *
 * RabbitMQ filtert nicht nach Raum, der Fanout-Exchange schickt jede
 * Nachricht an jede Instanz. Gefiltert wird hier: eine Nachricht geht nur
 * an die Liste ihres Raums.
 *
 * Auf die Tabelle greifen mehrere Threads zu: HTTP-Threads melden an und
 * ab, der Listener-Thread und der Keepalive-Thread lesen. Darum sind alle
 * Methoden, die die Tabelle anfassen, synchronized. Gesendet wird aber
 * ausserhalb dieser Sperre, auf einer Kopie der Liste: ein langsamer
 * Client soll nicht das An- und Abmelden aller anderen blockieren.
 */
@Service
@Slf4j
public class StreamRegistry {

    private final Map<UUID, List<SseEmitter>> emittersByRoom = new HashMap<>();

    private final Map<UUID, Object> locksByRoom = new HashMap<>();

    /**
     * Die Sperre eines Raums (Spezifikation 4.6). Der Listener nimmt sie,
     * während er eine Nachricht in den Nachhol-Puffer legt und sendet; der
     * StreamController nimmt sie, während er nachliefert und den neuen
     * Client anmeldet. So kann keine Nachricht zwischen "nachgeliefert" und
     * "angemeldet" durchrutschen (Lücke) oder in beidem landen (doppelt).
     *
     * Die Sperren werden nie entfernt. Würde eine Sperre ersetzt, während
     * jemand die alte hält, gäbe es für denselben Raum zwei Sperren. Ein
     * leeres Objekt pro Raum kostet fast nichts.
     */
    public synchronized Object roomLock(UUID roomId) {
        Object lock = locksByRoom.get(roomId);
        if (lock == null) {
            lock = new Object();
            locksByRoom.put(roomId, lock);
        }
        return lock;
    }

    /** Trägt eine neue Stream-Verbindung in die Liste ihres Raums ein. */
    public synchronized void register(UUID roomId, SseEmitter emitter) {
        List<SseEmitter> emitters = emittersByRoom.get(roomId);
        if (emitters == null) {
            emitters = new ArrayList<>();
            emittersByRoom.put(roomId, emitters);
        }
        emitters.add(emitter);
        log.debug("Stream for room {} registered, {} open in this room", roomId, emitters.size());
    }

    /**
     * Entfernt eine Stream-Verbindung. Ist der Raum danach leer, fliegt er
     * ganz aus der Tabelle, damit sie nicht mit leeren Räumen wächst.
     */
    public synchronized void unregister(UUID roomId, SseEmitter emitter) {
        List<SseEmitter> emitters = emittersByRoom.get(roomId);
        if (emitters == null) {
            return;
        }
        emitters.remove(emitter);
        if (emitters.isEmpty()) {
            emittersByRoom.remove(roomId);
        }
        log.debug("Stream for room {} unregistered", roomId);
    }

    /**
     * Schickt eine Nachricht an alle Clients ihres Raums — und nur an diese.
     *
     * id: ist die Nachrichten-ID, data: der unveränderte JSON-Text aus der
     * Queue (Spezifikation 2.2). Kein event:-Feld, damit heisst das
     * Ereignis im Browser standardmässig "message".
     */
    public void sendToRoom(UUID roomId, String messageId, String json) {
        List<SseEmitter> emitters = copyEmittersOf(roomId);
        for (SseEmitter emitter : emitters) {
            sendMessage(roomId, emitter, messageId, json);
        }
    }

    /**
     * Schickt eine Nachricht an eine einzelne Verbindung. Gebraucht für das
     * Nachliefern an einen Client, der sich mit Last-Event-ID neu verbindet.
     */
    public void sendMessage(UUID roomId, SseEmitter emitter, String messageId, String json) {
        SseEmitter.SseEventBuilder event = SseEmitter.event();
        event.id(messageId);
        event.data(json);
        send(roomId, emitter, event);
    }

    /**
     * Schickt einen Kommentar an eine einzelne Verbindung. Kommentare sind
     * keine Ereignisse, ein Client sieht sie nicht.
     */
    public void sendComment(UUID roomId, SseEmitter emitter, String comment) {
        SseEmitter.SseEventBuilder event = SseEmitter.event();
        event.comment(comment);
        send(roomId, emitter, event);
    }

    /**
     * Lebenszeichen alle 15 s an jede offene Verbindung (Spezifikation 2.2).
     *
     * Ohne Schreiben merkt der Server nie, dass ein Client weg ist. Schlägt
     * das Schreiben fehl, meldet send() die Verbindung ab.
     */
    @Scheduled(fixedRate = 15000)
    public void sendKeepalive() {
        List<UUID> roomIds = copyRoomIds();
        for (UUID roomId : roomIds) {
            List<SseEmitter> emitters = copyEmittersOf(roomId);
            for (SseEmitter emitter : emitters) {
                sendComment(roomId, emitter, "keepalive");
            }
        }
    }

    /**
     * Schreibt ein Ereignis an eine Verbindung.
     *
     * synchronized auf den Emitter: Listener und Keepalive-Thread dürfen
     * nicht gleichzeitig in dieselbe Verbindung schreiben, sonst könnten
     * sich ihre Zeilen vermischen oder überholen (Spezifikation 4.3).
     *
     * Schlägt das Schreiben fehl, ist der Client weg. Dann wird nur diese
     * eine Verbindung abgemeldet, die anderen bekommen ihre Nachricht
     * trotzdem (Spezifikation 4.1, Schritt 4). IllegalStateException kommt,
     * wenn die Verbindung schon geschlossen wurde, aber noch in der Kopie
     * der Liste stand — dasselbe Ergebnis: Client weg, abmelden.
     */
    private void send(UUID roomId, SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        synchronized (emitter) {
            try {
                emitter.send(event);
            } catch (IOException | IllegalStateException exception) {
                log.debug("Stream for room {} closed while sending: {}", roomId, exception.getMessage());
                unregister(roomId, emitter);
            }
        }
    }

    /** Kopie der Liste eines Raums, damit ausserhalb der Sperre gesendet werden kann. */
    private synchronized List<SseEmitter> copyEmittersOf(UUID roomId) {
        List<SseEmitter> emitters = emittersByRoom.get(roomId);
        if (emitters == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(emitters);
    }

    /** Kopie aller Räume mit offenen Verbindungen, für den Keepalive. */
    private synchronized List<UUID> copyRoomIds() {
        return new ArrayList<>(emittersByRoom.keySet());
    }
}
