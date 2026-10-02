package ch.benedict.m321.batchwriter.service;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
 *
 * Das Schema kommt aus derselben Datei postgres/init.sql wie im Docker-Stack.
 *
 * OutputCaptureExtension faengt die Log-Ausgabe ab. Der Ausfall-Test prueft damit,
 * dass der batch-writer den Ausfall wirklich bemerkt hat.
 */
@SpringBootTest
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class BatchMessageConsumerIntegrationTest {

    /** Das Schema des Stacks, relativ zum Modulordner batch-writer/. */
    private static final MountableFile SCHEMA_FILE = MountableFile.forHostPath("../postgres/init.sql");

    /** Der Klassenname, den der chat-service in den Header __TypeId__ schreibt. */
    private static final String CHAT_SERVICE_TYPE_ID = "ch.benedict.m321.chatservice.dto.ChatMessage";

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withCopyFileToContainer(SCHEMA_FILE, "/docker-entrypoint-initdb.d/init.sql");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    /** Ein Raum, den es in der Tabelle room nicht gibt, wie beim Pruefskript. */
    private static final UUID TEST_ROOM_ID = UUID.randomUUID();

    /**
     * Leert vor jedem Test die Tabelle und beide Queues, damit kein Test die Reste eines anderen sieht.
     */
    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("DELETE FROM message");
        rabbitAdmin.purgeQueue(QueueNames.PERSIST_QUEUE);
        rabbitAdmin.purgeQueue(QueueNames.DEAD_LETTER_QUEUE);
    }

    /**
     * Szenario S3: Eine Nachricht genau so, wie der chat-service sie schickt, also mit
     * seinem eigenen Klassennamen im Header __TypeId__. Sie muss in der Tabelle landen.
     */
    @Test
    void consumesMessageInChatServiceFormatAndWritesToDatabase() {
        UUID messageId = UUID.randomUUID();
        String json = String.format("""
                {"id":"%s","roomId":"%s","senderId":"user-123","senderName":"Anna",\
                "content":"Live aus RabbitMQ","sentAt":"2026-10-02T13:26:59.737112982Z"}
                """, messageId, TEST_ROOM_ID);

        MessageProperties properties = createJsonProperties();
        properties.setHeader("__TypeId__", CHAT_SERVICE_TYPE_ID);
        sendRawMessage(json, properties);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            int count = countMessagesWithId(messageId);
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
        String json = String.format("""
                {"id":"%s","roomId":"%s","senderId":"anna","senderName":"Anna Muster",\
                "content":"Duplikatstest","sentAt":"2026-09-30T14:00:00Z"}
                """, messageId, TEST_ROOM_ID);

        MessageProperties properties = createJsonProperties();
        sendRawMessage(json, properties);
        sendRawMessage(json, properties);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            int count = countMessagesWithId(messageId);
            assertEquals(1, count);
        });

        assertEquals(0, countDeadLetterMessages());
    }

    /**
     * Szenario S7: Vorübergehender Ausfall der Datenbank.
     * Erwartet: Keine Nachricht geht verloren, nach Wiederverfügbarkeit landet sie
     * automatisch in der Tabelle, ohne dass der batch-writer manuell neugestartet werden muss.
     * Waehrend des Ausfalls darf nichts in chat.dlq landen, denn die Nachricht ist in Ordnung.
     *
     * Der Ausfall ist echt: Alle Verbindungen werden getrennt und neue verboten,
     * wie bei "docker compose stop postgres".
     */
    @Test
    void recoversFromDatabaseOutageWithoutManualRestart(CapturedOutput output) throws Exception {
        UUID messageId = UUID.randomUUID();
        ChatMessage message = new ChatMessage(
                messageId, TEST_ROOM_ID, "user-outage", "Max", "Nachricht waehrend Ausfall", Instant.now());

        blockDatabaseConnections();
        try {
            sendChatMessage(message);

            // Der batch-writer muss den Ausfall bemerken und die Nachricht zurueck in die Queue geben
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                String log = output.getOut();
                return log.contains("Database not reachable");
            });
        } finally {
            // Auch wenn der Test scheitert: Die Datenbank muss fuer die anderen Tests wieder da sein
            allowDatabaseConnections();
        }

        // Ohne Neustart: Der naechste Versuch nach dem Ausfall muss gelingen.
        // ignoreExceptions: Auch der Test fragt ueber den Verbindungspool ab, und der kann
        // direkt nach dem Ausfall noch eine getrennte Verbindung liefern.
        await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            int count = countMessagesWithId(messageId);
            assertEquals(1, count);
        });

        String savedContent = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?", String.class, messageId);
        assertEquals("Nachricht waehrend Ausfall", savedContent);
        assertEquals(0, countDeadLetterMessages());
    }

    /**
     * Spezifikation 4.6: Fehlt ein Pflichtfeld (hier content), kann die Nachricht nie
     * gespeichert werden. Sie muss in chat.dlq landen statt endlos wiederholt zu werden.
     */
    @Test
    void movesIncompleteMessageToDeadLetterQueue() {
        UUID messageId = UUID.randomUUID();
        String json = String.format("""
                {"id":"%s","roomId":"%s","senderId":"anna","senderName":"Anna",\
                "sentAt":"2026-10-02T10:00:00Z"}
                """, messageId, TEST_ROOM_ID);

        MessageProperties properties = createJsonProperties();
        sendRawMessage(json, properties);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(1, countDeadLetterMessages());
        });
        assertEquals(0, countMessagesWithId(messageId));
    }

    /**
     * Spezifikation 4.6: Ein Body, der kein JSON ist, laesst sich nicht umwandeln.
     * Er muss in chat.dlq landen.
     */
    @Test
    void movesInvalidJsonToDeadLetterQueue() {
        MessageProperties properties = createJsonProperties();
        sendRawMessage("das ist kein JSON", properties);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(1, countDeadLetterMessages());
        });
    }

    /**
     * Spezifikation 4.7: Ein senderName mit 300 Zeichen passt nicht in VARCHAR(255).
     * Diese eine Nachricht muss in chat.dlq landen, die gueltige Nachricht aus demselben
     * Stapel muss trotzdem in der Tabelle landen.
     */
    @Test
    void movesUnsavableMessageToDeadLetterQueueAndSavesTheRest() throws JsonProcessingException {
        UUID validMessageId = UUID.randomUUID();
        ChatMessage validMessage = new ChatMessage(
                validMessageId, TEST_ROOM_ID, "anna", "Anna", "Ich bin gueltig", Instant.now());

        UUID invalidMessageId = UUID.randomUUID();
        String tooLongName = "x".repeat(300);
        ChatMessage invalidMessage = new ChatMessage(
                invalidMessageId, TEST_ROOM_ID, "ben", tooLongName, "Mein Name ist zu lang", Instant.now());

        sendChatMessage(invalidMessage);
        sendChatMessage(validMessage);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(1, countMessagesWithId(validMessageId));
            assertEquals(1, countDeadLetterMessages());
        });
        assertEquals(0, countMessagesWithId(invalidMessageId));
    }

    /**
     * Simuliert den Ausfall der Datenbank: Neue Verbindungen werden verboten, alle
     * bestehenden getrennt. Fuer den batch-writer sieht das aus wie ein gestoppter Server.
     */
    private void blockDatabaseConnections() throws IOException, InterruptedException {
        String databaseName = postgres.getDatabaseName();
        runAsDatabaseAdministrator("ALTER DATABASE " + databaseName + " ALLOW_CONNECTIONS false");
        runAsDatabaseAdministrator(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + databaseName + "'");
    }

    /**
     * Beendet den simulierten Ausfall: Neue Verbindungen sind wieder erlaubt.
     */
    private void allowDatabaseConnections() throws IOException, InterruptedException {
        String databaseName = postgres.getDatabaseName();
        runAsDatabaseAdministrator("ALTER DATABASE " + databaseName + " ALLOW_CONNECTIONS true");
    }

    /**
     * Fuehrt einen SQL-Befehl mit psql direkt im Container aus, verbunden mit der
     * Verwaltungsdatenbank "postgres". So funktioniert er auch, waehrend die
     * Chat-Datenbank keine Verbindungen annimmt.
     */
    private void runAsDatabaseAdministrator(String sql) throws IOException, InterruptedException {
        String databaseUser = postgres.getUsername();
        ExecResult result = postgres.execInContainer("psql", "-U", databaseUser, "-d", "postgres", "-c", sql);
        assertEquals(0, result.getExitCode(), result.getStderr());
    }

    /**
     * Header, wie sie in Szenario S5 gesetzt werden: nur content_type: application/json.
     */
    private MessageProperties createJsonProperties() {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        return properties;
    }

    /**
     * Schreibt eine ChatMessage als JSON und legt sie mit content_type: application/json
     * in chat.persist, wie es der chat-service tut.
     */
    private void sendChatMessage(ChatMessage message) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(message);
        MessageProperties properties = createJsonProperties();
        sendRawMessage(json, properties);
    }

    /**
     * Legt einen Text unveraendert in chat.persist, ohne den Konverter von Spring.
     * So kann der Test genau bestimmen, welcher Body und welche Header ankommen.
     */
    private void sendRawMessage(String body, MessageProperties properties) {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        Message amqpMessage = new Message(bodyBytes, properties);
        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, amqpMessage);
    }

    /**
     * Zaehlt die Zeilen mit dieser ID in der Tabelle message.
     */
    private int countMessagesWithId(UUID messageId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM message WHERE id = ?", Integer.class, messageId);
        return count;
    }

    /**
     * Zaehlt die Nachrichten, die in chat.dlq auf einen Menschen warten.
     */
    private int countDeadLetterMessages() {
        QueueInformation deadLetterQueue = rabbitAdmin.getQueueInfo(QueueNames.DEAD_LETTER_QUEUE);
        return deadLetterQueue.getMessageCount();
    }
}
