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
 * In diesem Schritt wird die Nachricht nur geloggt. Das Senden an die
 * Clients kommt mit GET /stream dazu.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DeliveryListener {

    private final ObjectMapper objectMapper;

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

        String roomId = readText(fields, "roomId");
        String messageId = readText(fields, "id");
        if (roomId == null || messageId == null) {
            log.warn("Delivery message without roomId or id dropped: {}", json);
            return;
        }

        log.debug("Delivery message {} for room {} received", messageId, roomId);
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
