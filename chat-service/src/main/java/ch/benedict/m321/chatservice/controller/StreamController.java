package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.service.ReplayBuffer;
import ch.benedict.m321.chatservice.service.StreamRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.UUID;

/**
 * Der Anzeigeweg nach aussen: GET /stream?roomId=... öffnet eine
 * Server-Sent-Events-Verbindung, über die der Client die Nachrichten
 * seines Raums live bekommt (Spezifikation 2.2).
 *
 * Wie /messages ist der Endpunkt nur im Docker-Netz erreichbar und prüft
 * kein Token (Spezifikation 1.2).
 */
@RestController
@Slf4j
@RequiredArgsConstructor
public class StreamController {

    /**
     * 0 heisst: kein Timeout. Der Vorgabewert von Spring MVC würde jede
     * Verbindung nach 30 s schliessen, ein Chat-Stream bleibt aber offen.
     */
    private static final long NO_TIMEOUT = 0L;

    private final StreamRegistry streamRegistry;

    private final ReplayBuffer replayBuffer;

    /**
     * Öffnet einen Stream für einen Raum.
     *
     * roomId ist eine UUID. Fehlt sie oder ist sie keine UUID, antwortet
     * Spring selbst mit 400, bevor diese Methode aufgerufen wird.
     *
     * Die Verbindung meldet sich selbst wieder ab, sobald sie endet: der
     * Client trennt (Completion), ein Fehler tritt auf (Error) oder ein
     * Timeout greift (den es mit NO_TIMEOUT nicht gibt, aber sicher ist
     * sicher).
     *
     * Der Kommentar "connected" gleich zu Beginn ist kein Ereignis, der
     * Client sieht ihn nicht. Er sorgt dafür, dass Spring die Antwort
     * (Status 200 und Content-Type) sofort schickt und nicht erst mit der
     * ersten Nachricht. So weiss der Client, dass er verbunden ist.
     *
     * Last-Event-ID schickt ein Client mit, der sich nach einem Abbruch neu
     * verbindet (der Browser macht das von selbst). Dann wird zuerst
     * nachgeliefert, was er verpasst hat (Spezifikation 4.6). Ohne Header
     * bekommt er nur neue Nachrichten.
     *
     * Nachliefern und Anmelden passieren unter der Sperre des Raums. Der
     * Listener nimmt dieselbe Sperre, bevor er eine Nachricht in den Puffer
     * legt und verteilt. Darum ist jede Nachricht entweder schon im Puffer
     * (und wird nachgeliefert) oder kommt erst nach dem Anmelden (und kommt
     * live) — nie beides und nie keins.
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam UUID roomId,
                             @RequestHeader(name = "Last-Event-ID", required = false) String lastEventId) {
        SseEmitter emitter = new SseEmitter(NO_TIMEOUT);
        emitter.onCompletion(() -> streamRegistry.unregister(roomId, emitter));
        emitter.onTimeout(() -> streamRegistry.unregister(roomId, emitter));
        emitter.onError(exception -> streamRegistry.unregister(roomId, emitter));

        Object roomLock = streamRegistry.roomLock(roomId);
        synchronized (roomLock) {
            streamRegistry.sendComment(roomId, emitter, "connected");
            if (lastEventId != null) {
                replayMissedMessages(roomId, emitter, lastEventId);
            }
            streamRegistry.register(roomId, emitter);
        }
        return emitter;
    }

    /**
     * Schickt einem neu verbundenen Client alle Nachrichten nach
     * lastEventId aus dem Nachhol-Puffer, in ihrer Reihenfolge.
     *
     * Steht lastEventId nicht im Puffer, wird nichts nachgeliefert: alles zu
     * schicken würde Duplikate erzeugen. Der Client bekommt dann nur noch
     * neue Nachrichten; die Lücke muss er aus der Historie laden (nicht Teil
     * dieses Auftrags).
     */
    private void replayMissedMessages(UUID roomId, SseEmitter emitter, String lastEventId) {
        List<ReplayBuffer.BufferedMessage> missedMessages = replayBuffer.messagesAfter(roomId, lastEventId);
        if (missedMessages == null) {
            log.warn("Last-Event-ID {} for room {} not in replay buffer, nothing replayed", lastEventId, roomId);
            return;
        }

        for (ReplayBuffer.BufferedMessage message : missedMessages) {
            streamRegistry.sendMessage(roomId, emitter, message.id(), message.json());
        }
        log.debug("Replayed {} messages for room {} after {}", missedMessages.size(), roomId, lastEventId);
    }
}
