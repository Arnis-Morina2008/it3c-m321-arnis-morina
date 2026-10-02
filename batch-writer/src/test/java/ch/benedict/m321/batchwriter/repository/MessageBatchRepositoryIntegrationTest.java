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
import org.testcontainers.utility.MountableFile;

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
 *
 * Das Schema kommt aus derselben Datei postgres/init.sql wie im Docker-Stack.
 * So prueft der Test genau die Tabelle, in die der Dienst spaeter schreibt.
 */
@SpringBootTest
@Testcontainers
class MessageBatchRepositoryIntegrationTest {

    /** Das Schema des Stacks, relativ zum Modulordner batch-writer/. */
    private static final MountableFile SCHEMA_FILE = MountableFile.forHostPath("../postgres/init.sql");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withCopyFileToContainer(SCHEMA_FILE, "/docker-entrypoint-initdb.d/init.sql");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private MessageBatchRepository messageBatchRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** Der Standard-Raum, den init.sql anlegt. */
    private static final UUID TEST_ROOM_ID = UUID.fromString("3f2b1c4e-0000-0000-0000-000000000001");

    /**
     * Leert vor jedem Test die Tabelle, damit jeder Test bei null Zeilen beginnt.
     */
    @BeforeEach
    void clearMessageTable() {
        jdbcTemplate.execute("DELETE FROM message");
    }

    /**
     * Stellt sicher, dass mehrere Chat-Nachrichten in einem einzigen Aufruf korrekt gespeichert werden.
     */
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

    /**
     * Prueft, dass doppelt gesendete Nachrichten mit identischer ID dank ON CONFLICT ignoriert werden.
     */
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

    /**
     * Szenario S4 zaehlt Transaktionen. Ein voller Stapel von 500 Nachrichten
     * muss deshalb in genau einer Transaktion landen.
     *
     * PostgreSQL schreibt in jede Zeile die Nummer der Transaktion, die sie
     * eingefuegt hat (Systemspalte xmin). Haben alle 500 Zeilen dieselbe
     * Nummer, war es genau eine Transaktion.
     */
    @Test
    void writesWholeBatchInOneTransaction() {
        List<ChatMessage> batch = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            ChatMessage message = new ChatMessage(
                    UUID.randomUUID(), TEST_ROOM_ID, "user-" + i, "Tester", "Nachricht " + i, Instant.now());
            batch.add(message);
        }

        messageBatchRepository.saveBatch(batch);

        Integer transactionCount = jdbcTemplate.queryForObject(
                "SELECT count(DISTINCT xmin::text) FROM message", Integer.class);
        assertEquals(1, transactionCount);
    }

    /**
     * Der chat-service nimmt jede roomId an, und Raeume legt noch niemand an.
     * Eine Nachricht fuer einen Raum, der nicht in der Tabelle room steht, muss
     * trotzdem gespeichert werden. Sonst scheitert ihr ganzer Stapel.
     */
    @Test
    void savesMessageForRoomThatIsNotInRoomTable() {
        UUID unknownRoomId = UUID.randomUUID();
        ChatMessage message = new ChatMessage(
                UUID.randomUUID(), unknownRoomId, "user-1", "Anna", "Hallo", Instant.now());

        List<ChatMessage> batch = new ArrayList<>();
        batch.add(message);
        messageBatchRepository.saveBatch(batch);

        Integer databaseCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM message WHERE room_id = ?", Integer.class, unknownRoomId);
        assertEquals(1, databaseCount);
    }
}
