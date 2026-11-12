package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.config.QueueNames;
import ch.benedict.m321.chatservice.dto.ChatMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Der wichtigste Test dieses Dienstes: geht die Nachricht wirklich in
 * BEIDE Wege? Genau das ist der Kern der Entkopplung von Zustellung
 * und Speicherung.
 */
@SpringBootTest
@Testcontainers
class MessagePublisherIntegrationTest {

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private MessagePublisher messagePublisher;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Eine Queue ist gemeinsamer Zustand: sie überlebt die einzelne
     * Testmethode. Ohne dieses Leeren liest der zweite Test die
     * Nachricht des ersten und schlägt scheinbar grundlos fehl.
     */
    @BeforeEach
    void emptyPersistQueue() {
        rabbitAdmin.purgeQueue(QueueNames.PERSIST_QUEUE);
    }

    /** Der Schreibweg: die Nachricht liegt danach in chat.persist. */
    @Test
    void publishesToPersistQueue() {
        ChatMessage message = createMessage("Hallo Schreibweg");

        messagePublisher.publish(message);

        ChatMessage persisted = receiveFrom(QueueNames.PERSIST_QUEUE);
        assertNotNull(persisted);
        assertEquals(message.id(), persisted.id());
        assertEquals("Hallo Schreibweg", persisted.content());
    }

    /** Der Zustellweg: eine an chat.delivery gebundene Queue bekommt ihre Kopie. */
    @Test
    void publishesToDeliveryExchange() {
        // Wir spielen ein web-gateway: eigene Queue an den Fanout binden.
        Queue gatewayQueue = new AnonymousQueue();
        rabbitAdmin.declareQueue(gatewayQueue);

        FanoutExchange exchange = new FanoutExchange(QueueNames.DELIVERY_EXCHANGE);
        Binding binding = BindingBuilder.bind(gatewayQueue).to(exchange);
        rabbitAdmin.declareBinding(binding);

        ChatMessage message = createMessage("Hallo Zustellweg");

        messagePublisher.publish(message);

        String queueName = gatewayQueue.getName();
        ChatMessage delivered = receiveFrom(queueName);
        assertNotNull(delivered);
        assertEquals(message.id(), delivered.id());
        assertEquals("Hallo Zustellweg", delivered.content());
    }

    /**
     * Der Vertrag der Live-Zustellung (Spezifikation 2.1): am Exchange
     * chat.delivery kommt Byte für Byte derselbe Body an wie in
     * chat.persist. Der Stream reicht diesen Text später unverändert als
     * data: weiter. Ändert jemand das Format auf nur einem der beiden Wege,
     * wird dieser Test rot.
     */
    @Test
    void deliveryBodyEqualsPersistBody() throws Exception {
        Queue deliveryProbeQueue = bindProbeQueueToDeliveryExchange();
        ChatMessage message = createMessage("Hallo Vertrag");

        messagePublisher.publish(message);

        String deliveryQueueName = deliveryProbeQueue.getName();
        Message persistMessage = rabbitTemplate.receive(QueueNames.PERSIST_QUEUE, 5000);
        Message deliveryMessage = rabbitTemplate.receive(deliveryQueueName, 5000);
        assertNotNull(persistMessage);
        assertNotNull(deliveryMessage);

        byte[] persistBytes = persistMessage.getBody();
        byte[] deliveryBytes = deliveryMessage.getBody();
        String persistBody = new String(persistBytes, StandardCharsets.UTF_8);
        String deliveryBody = new String(deliveryBytes, StandardCharsets.UTF_8);
        assertEquals(persistBody, deliveryBody);

        MessageProperties deliveryProperties = deliveryMessage.getMessageProperties();
        String contentType = deliveryProperties.getContentType();
        assertEquals("application/json", contentType);

        assertContainsAllContractFields(deliveryBody);
    }

    /**
     * Prüft, dass der Body alle sechs Felder aus dem Vertrag enthält.
     * Gelesen wird als JsonNode und nicht als ChatMessage: der Test soll
     * das JSON selbst prüfen, nicht das, was ein Konverter daraus macht.
     */
    private void assertContainsAllContractFields(String body) throws Exception {
        JsonNode json = objectMapper.readTree(body);
        String[] contractFields = {"id", "roomId", "senderId", "senderName", "content", "sentAt"};
        for (String field : contractFields) {
            assertTrue(json.has(field), "Feld fehlt im Body: " + field);
        }
    }

    /**
     * Spielt eine Instanz der Live-Zustellung: legt eine eigene Queue an
     * und bindet sie an den Fanout-Exchange, damit die Kopie aus
     * chat.delivery nicht verworfen wird.
     */
    private Queue bindProbeQueueToDeliveryExchange() {
        Queue probeQueue = new AnonymousQueue();
        rabbitAdmin.declareQueue(probeQueue);

        FanoutExchange exchange = new FanoutExchange(QueueNames.DELIVERY_EXCHANGE);
        Binding binding = BindingBuilder.bind(probeQueue).to(exchange);
        rabbitAdmin.declareBinding(binding);
        return probeQueue;
    }

    /**
     * Holt eine Nachricht aus einer Queue.
     *
     * Der Zieltyp wird hier ausdrücklich mitgegeben. Grund: in der Queue
     * liegt JSON, nicht ein Java-Objekt. Wer liest, muss wissen, was er
     * erwartet — genau so wird es später auch der batch-writer machen,
     * der ja seine eigene Kopie der Klasse hat.
     */
    private ChatMessage receiveFrom(String queueName) {
        ParameterizedTypeReference<ChatMessage> targetType = new ParameterizedTypeReference<>() {
        };
        return rabbitTemplate.receiveAndConvert(queueName, 5000, targetType);
    }

    /** Baut eine vollständige Nachricht, damit die Tests kurz bleiben. */
    private ChatMessage createMessage(String content) {
        UUID messageId = UUID.randomUUID();
        UUID roomId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "anna", "Anna Muster", content, sentAt);
    }
}
