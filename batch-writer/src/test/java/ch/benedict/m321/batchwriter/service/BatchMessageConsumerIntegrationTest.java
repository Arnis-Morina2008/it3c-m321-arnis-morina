package ch.benedict.m321.batchwriter.service;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prueft das Zusammenspiel von RabbitMQ und PostgreSQL im batch-writer.
 *
 * Eine Nachricht wird in die Queue chat.persist geschickt. Der BatchMessageConsumer
 * liest sie, puffert sie, fuehrt nach maximal 200 ms den Bulk-Insert aus
 * und bestaetigt den Empfang.
 */
@SpringBootTest
@Testcontainers
class BatchMessageConsumerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final UUID TEST_ROOM_ID = UUID.fromString("3f2b1c4e-0000-0000-0000-000000000001");

    /**
     * Initialisiert vor jedem Test die Datenbanktabellen und leert die RabbitMQ-Queue.
     */
    @BeforeEach
    void setUp() {
        // Tabellen fuer den Test anlegen
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS room (
                    id UUID PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS message (
                    id UUID PRIMARY KEY,
                    room_id UUID NOT NULL REFERENCES room(id) ON DELETE CASCADE,
                    sender_id VARCHAR(255) NOT NULL,
                    sender_name VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    sent_at TIMESTAMPTZ NOT NULL
                );
                """);

        jdbcTemplate.execute("DELETE FROM message");
        jdbcTemplate.execute("DELETE FROM room");
        jdbcTemplate.update("INSERT INTO room (id, name) VALUES (?, ?)", TEST_ROOM_ID, "General");

        // Queue vor dem Test leeren
        rabbitAdmin.purgeQueue(QueueNames.PERSIST_QUEUE);
    }

    /**
     * Prueft den Regelfall: Eine Nachricht wird aus der Queue konsumiert und in die DB geschrieben.
     */
    @Test
    void consumesMessageFromQueueAndWritesToDatabase() {
        UUID messageId = UUID.randomUUID();
        ChatMessage message = new ChatMessage(
                messageId, TEST_ROOM_ID, "user-123", "Anna", "Live aus RabbitMQ", Instant.now());

        // Nachricht in die Queue schicken, genau wie es der chat-service tut
        rabbitTemplate.convertAndSend(QueueNames.PERSIST_QUEUE, message);

        // Mit Awaitility warten, bis der Consumer gepuffert, geflusht und committet hat (max. 5 Sekunden)
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM message WHERE id = ?", Integer.class, messageId);
            assertEquals(1, count);
        });

        String savedContent = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?", String.class, messageId);
        assertEquals("Live aus RabbitMQ", savedContent);
    }

    /**
     * Szenario S5: Dieselbe Nachricht zweimal nur mit content_type: application/json
     * in chat.persist gelegt.
     * Erwartet: Genau eine Zeile in message, 0 in chat.dlq.
     */
    @Test
    void handlesDuplicateMessagesWithoutErrorAndDoesNotRouteToDeadLetterQueue() {
        UUID messageId = UUID.randomUUID();
        String jsonPayload = String.format("""
                {
                  "id": "%s",
                  "roomId": "%s",
                  "senderId": "anna",
                  "senderName": "Anna Muster",
                  "content": "Duplikatstest",
                  "sentAt": "2026-09-30T14:00:00Z"
                }
                """, messageId, TEST_ROOM_ID);

        byte[] body = jsonPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        org.springframework.amqp.core.MessageProperties properties =
                new org.springframework.amqp.core.MessageProperties();
        properties.setContentType(org.springframework.amqp.core.MessageProperties.CONTENT_TYPE_JSON);

        org.springframework.amqp.core.Message amqpMessage =
                new org.springframework.amqp.core.Message(body, properties);

        // Erste Nachricht senden
        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, amqpMessage);

        // Identische Nachricht ein zweites Mal senden
        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, amqpMessage);

        // Warten, bis der Consumer den Batch verarbeitet hat
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM message WHERE id = ?", Integer.class, messageId);
            assertEquals(1, count);
        });

        // Sicherstellen, dass nichts in der Dead-Letter-Queue gelandet ist
        java.util.Properties dlqProperties = rabbitAdmin.getQueueProperties(QueueNames.DEAD_LETTER_QUEUE);
        int dlqCount = 0;
        if (dlqProperties != null) {
            Object countProp = dlqProperties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
            if (countProp instanceof Number number) {
                dlqCount = number.intValue();
            }
        }
        assertEquals(0, dlqCount);
    }

    /**
     * Szenario S7: Vorübergehender Ausfall der Datenbank.
     * Erwartet: Keine Nachricht geht verloren, nach Wiederverfügbarkeit landet sie
     * automatisch in der Tabelle, ohne dass der batch-writer manuell neugestartet werden muss.
     */
    @Test
    void recoversFromDatabaseOutageWithoutManualRestart() {
        UUID messageId = UUID.randomUUID();
        ChatMessage message = new ChatMessage(
                messageId, TEST_ROOM_ID, "user-outage", "Max", "Nachricht waehrend Ausfall", Instant.now());

        // Datenbankausfall simulieren durch Umbenennen der Tabelle
        jdbcTemplate.execute("ALTER TABLE message RENAME TO message_unavailable;");

        // Nachricht senden, waehrend DB nicht schreiben kann
        rabbitTemplate.convertAndSend(QueueNames.PERSIST_QUEUE, message);

        // Kurze Pause: Consumer versucht zu schreiben, scheitert, sendet NACK mit requeue
        try {
            Thread.sleep(1500);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }

        // Datenbank wiederherstellen
        jdbcTemplate.execute("ALTER TABLE message_unavailable RENAME TO message;");

        // Warten, bis der Consumer die Nachricht im naechsten Versuch erfolgreich speichert
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM message WHERE id = ?", Integer.class, messageId);
            assertEquals(1, count);
        });

        String savedContent = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?", String.class, messageId);
        assertEquals("Nachricht waehrend Ausfall", savedContent);
    }
}
