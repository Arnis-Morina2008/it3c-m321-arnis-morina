package ch.benedict.m321.chatservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Holt die Kopien vom Zustellweg ab: alles, was in der eigenen Queue
 * dieser Instanz an chat.delivery ankommt (Spezifikation 4.1).
 *
 * Die Nachricht wird bewusst NICHT in ein ChatMessage-Objekt umgewandelt.
 * Der Stream soll später den Originaltext unverändert weiterreichen, damit
 * das Format nie von chat.persist abweichen kann (Spezifikation 2.2).
 * Gelesen werden nur die zwei Felder, die die Zustellung braucht: roomId
 * (an wen) und id (für das id:-Feld im Stream).
 *
 * Weitergegeben wird die Nachricht an den Nachhol-Puffer (ReplayBuffer)
 * und an die StreamRegistry, die sie an die offenen Streams ihres Raums
 * schickt.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DeliveryListener {

    private final ObjectMapper objectMapper;

    private final StreamRegistry streamRegistry;

    private final ReplayBuffer replayBuffer;

    /**
     * Wird für jede Nachricht in der Queue dieser Instanz aufgerufen.
     *
     * Der Queue-Name entsteht erst beim Start (UUID). Darum steht hier kein
     * fester Name, sondern "#{deliveryQueue.name}": Spring fragt die Bean
     * deliveryQueue aus RabbitConfig nach ihrem Namen.
     *
     * Ungültiges JSON wird geloggt und verworfen. Kehrt die Methode normal
     * zurück, bestätigt Spring die Nachricht. Eine kaputte Nachricht wird
     * beim zweiten Versuch nicht besser, darum wird sie nicht wiederholt.
     */
    @RabbitListener(queues = "#{deliveryQueue.name}", containerFactory = "deliveryListenerFactory")
    public void onMessage(Message message) {
        byte[] bodyBytes = message.getBody();
        String json = new String(bodyBytes, StandardCharsets.UTF_8);

        JsonNode fields = readJson(json);
        if (fields == null) {
            return;
        }

        String roomIdText = readText(fields, "roomId");
        String messageId = readText(fields, "id");
        if (roomIdText == null || messageId == null) {
            log.warn("Delivery message without roomId or id dropped: {}", json);
            return;
        }

        UUID roomId = parseRoomId(roomIdText);
        if (roomId == null) {
            log.warn("Delivery message with invalid roomId dropped: {}", json);
            return;
        }

        deliver(roomId, messageId, json);
    }

    /**
     * Legt die Nachricht in den Nachhol-Puffer und schickt sie an die
     * offenen Streams des Raums — beides unter der Sperre des Raums
     * (Spezifikation 4.6). Ein Client, der sich gerade neu verbindet,
     * wartet so, bis die Nachricht fertig verteilt ist, und bekommt sie
     * danach entweder aus dem Puffer oder live, aber nie zweimal oder gar nicht.
     */
    private void deliver(UUID roomId, String messageId, String json) {
        Object roomLock = streamRegistry.roomLock(roomId);
        synchronized (roomLock) {
            replayBuffer.add(roomId, messageId, json);
            streamRegistry.sendToRoom(roomId, messageId, json);
        }
    }

    /**
     * Wandelt die roomId in eine UUID um, denn die Raumtabelle ist nach UUID
     * aufgebaut. So ist es egal, ob die ID gross oder klein geschrieben
     * ankommt. Gibt null zurück, wenn der Text keine UUID ist.
     */
    private UUID parseRoomId(String roomIdText) {
        try {
            return UUID.fromString(roomIdText);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    /**
     * Liest den Text als JSON. Gibt null zurück, wenn er kein gültiges JSON
     * ist, damit onMessage die Nachricht verwerfen kann.
     */
    private JsonNode readJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException exception) {
            log.warn("Delivery message is not valid JSON, dropped: {}", json);
            return null;
        }
    }

    /**
     * Holt ein Textfeld aus dem JSON. Gibt null zurück, wenn das Feld fehlt
     * oder leer ist.
     */
    private String readText(JsonNode fields, String fieldName) {
        JsonNode field = fields.get(fieldName);
        if (field == null) {
            return null;
        }
        String text = field.asText();
        if (text.isEmpty()) {
            return null;
        }
        return text;
    }
}
