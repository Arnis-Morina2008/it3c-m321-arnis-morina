package ch.benedict.m321.batchwriter.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Repraesentiert eine Chat-Nachricht, wie sie aus der Queue chat.persist gelesen wird.
 *
 * Dies ist eine eigenstaendige Kopie der Datenstruktur fuer den batch-writer.
 * Wir teilen bewusst keine gemeinsame Java-Klasse mit dem chat-service, da der
 * Vertrag das JSON ueber RabbitMQ ist und Microservices unabhaengig bleiben sollen.
 *
 * @param id         die vom chat-service vergebene Nachrichten-ID
 * @param roomId     der Raum, zu dem die Nachricht gehoert
 * @param senderId   die Benutzerkennung (sub) aus Keycloak
 * @param senderName der Anzeigename des Absenders
 * @param content    der Nachrichtentext
 * @param sentAt     der vom Server gesetzte Zeitstempel
 */
public record ChatMessage(
        UUID id,
        UUID roomId,
        String senderId,
        String senderName,
        String content,
        Instant sentAt) {
}
