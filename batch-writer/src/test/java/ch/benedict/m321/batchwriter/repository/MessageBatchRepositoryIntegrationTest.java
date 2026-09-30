package ch.benedict.m321.batchwriter.repository;

import ch.benedict.m321.batchwriter.dto.ChatMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prueft das MessageBatchRepository gegen eine echte PostgreSQL-Datenbank.
 *
 * Hier wird verifiziert:
 * 1. Der Bulk-Insert speichert alle Nachrichten eines Batches korrekt ab.
 * 2. Nachrichten mit bereits vorhandener ID werden dank ON CONFLICT (id) DO NOTHING
 *    stillschweigend ignoriert und werfen keinen Fehler (Idempotenz).
 */
@SpringBootTest
@Testcontainers
class MessageBatchRepositoryIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private MessageBatchRepository messageBatchRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final UUID TEST_ROOM_ID = UUID.fromString("3f2b1c4e-0000-0000-0000-000000000001");

    @BeforeEach
    void setUpDatabaseSchema() {
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

        // Alte Daten aus frueheren Tests loeschen
        jdbcTemplate.execute("DELETE FROM message");
        jdbcTemplate.execute("DELETE FROM room");

        // Test-Raum anlegen, damit der Fremdschluessel erfuellt ist
        jdbcTemplate.update("INSERT INTO room (id, name) VALUES (?, ?)", TEST_ROOM_ID, "General");
    }

    @Test
    void insertsBatchOfMessagesSuccessfully() {
        List<ChatMessage> messages = new ArrayList<>();

        ChatMessage firstMessage = new ChatMessage(
                UUID.randomUUID(), TEST_ROOM_ID, "user-1", "Anna", "Hallo Welt", Instant.now());
        messages.add(firstMessage);

        ChatMessage secondMessage = new ChatMessage(
                UUID.randomUUID(), TEST_ROOM_ID, "user-2", "Ben", "Guten Morgen", Instant.now());
        messages.add(secondMessage);

        int savedCount = messageBatchRepository.saveBatch(messages);
        assertEquals(2, savedCount);

        Integer databaseCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM message", Integer.class);
        assertEquals(2, databaseCount);
    }

    @Test
    void ignoresDuplicateMessagesWithSameId() {
        UUID duplicateId = UUID.randomUUID();
        ChatMessage originalMessage = new ChatMessage(
                duplicateId, TEST_ROOM_ID, "user-1", "Anna", "Erste Version", Instant.now());

        List<ChatMessage> firstBatch = new ArrayList<>();
        firstBatch.add(originalMessage);
        messageBatchRepository.saveBatch(firstBatch);

        // Erneuter Insert mit derselben ID, aber anderem Inhalt
        ChatMessage duplicateMessage = new ChatMessage(
                duplicateId, TEST_ROOM_ID, "user-1", "Anna", "Zweite Version", Instant.now());
        List<ChatMessage> secondBatch = new ArrayList<>();
        secondBatch.add(duplicateMessage);
        messageBatchRepository.saveBatch(secondBatch);

        // Es darf weiterhin nur genau 1 Eintrag existieren
        Integer databaseCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM message", Integer.class);
        assertEquals(1, databaseCount);

        // Und der Inhalt muss der urspruengliche geblieben sein
        String savedContent = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?", String.class, duplicateId);
        assertEquals("Erste Version", savedContent);
    }
}
