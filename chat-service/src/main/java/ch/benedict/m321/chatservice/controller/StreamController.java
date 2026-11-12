package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.service.StreamRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

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
@RequiredArgsConstructor
public class StreamController {

    /**
     * 0 heisst: kein Timeout. Der Vorgabewert von Spring MVC würde jede
     * Verbindung nach 30 s schliessen, ein Chat-Stream bleibt aber offen.
     */
    private static final long NO_TIMEOUT = 0L;

    private final StreamRegistry streamRegistry;

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
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam UUID roomId) {
        SseEmitter emitter = new SseEmitter(NO_TIMEOUT);
        emitter.onCompletion(() -> streamRegistry.unregister(roomId, emitter));
        emitter.onTimeout(() -> streamRegistry.unregister(roomId, emitter));
        emitter.onError(exception -> streamRegistry.unregister(roomId, emitter));

        streamRegistry.register(roomId, emitter);
        streamRegistry.sendComment(roomId, emitter, "connected");
        return emitter;
    }
}
