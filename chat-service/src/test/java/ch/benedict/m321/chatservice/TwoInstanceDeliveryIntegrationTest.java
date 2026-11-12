package ch.benedict.m321.chatservice;

import ch.benedict.m321.chatservice.SseTestClient.SseEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pflichttest für S3: zwei chat-service-Instanzen am selben RabbitMQ.
 *
 * Statt mit docker compose --scale laufen hier zwei Spring-Kontexte im
 * selben Test, jeder mit eigenem Webserver (zufälliger Port) und eigener
 * Queue an chat.delivery. Für RabbitMQ sind das zwei Instanzen wie im Stack.
 *
 * Aufbau wie im Szenario, nur kleiner (je 20 statt 200 Nachrichten):
 * an Instanz 1 ein Client in Raum A und einer in Raum B, an Instanz 2 ein
 * Client in Raum A. Absender s1 schickt über Instanz 1, s2 über Instanz 2.
 */
@Testcontainers
class TwoInstanceDeliveryIntegrationTest {

    /** Nachrichten pro Absender. */
    private static final int MESSAGES_PER_SENDER = 20;

    /** So lange warten die Clients höchstens auf alle Nachrichten. */
    private static final long MAX_WAIT_MILLIS = 15000;

    @Container
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    private static ConfigurableApplicationContext firstInstance;

    private static ConfigurableApplicationContext secondInstance;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newHttpClient();

    /**
     * Startet beide Instanzen. Der RabbitMQ-Container läuft zu diesem
     * Zeitpunkt schon (Testcontainers startet ihn vor @BeforeAll).
     */
    @BeforeAll
    static void startTwoInstances() {
        firstInstance = startInstance();
        secondInstance = startInstance();
    }

    /** Fährt beide Instanzen wieder herunter. */
    @AfterAll
    static void stopTwoInstances() {
        firstInstance.close();
        secondInstance.close();
    }

    /**
     * Beide Clients in Raum A bekommen alle 40 Nachrichten, jede genau
     * einmal, die eines Absenders in seiner Reihenfolge. Der Client in
     * Raum B bekommt keine.
     */
    @Test
    void deliversEveryMessageToAllClientsOfTheRoomOnly() throws Exception {
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        String firstUrl = baseUrlOf(firstInstance);
        String secondUrl = baseUrlOf(secondInstance);
        int expectedCount = 2 * MESSAGES_PER_SENDER;

        try (SseTestClient clientA1 = SseTestClient.connect(firstUrl, roomA);
             SseTestClient clientB1 = SseTestClient.connect(firstUrl, roomB);
             SseTestClient clientA2 = SseTestClient.connect(secondUrl, roomA)) {

            sendMessages(firstUrl, roomA, "s1");
            sendMessages(secondUrl, roomA, "s2");

            List<SseEvent> eventsA1 = clientA1.waitForEvents(expectedCount, MAX_WAIT_MILLIS);
            List<SseEvent> eventsA2 = clientA2.waitForEvents(expectedCount, MAX_WAIT_MILLIS);
            assertExactlyOnceAndInOrder(eventsA1, expectedCount);
            assertExactlyOnceAndInOrder(eventsA2, expectedCount);

            // Kein extra Warten nötig: Instanz 1 hat nur einen Listener-Thread
            // und arbeitet ihre Queue der Reihe nach ab. Hat Client A1 alle 40,
            // ist jede Nachricht schon an Instanz 1 vorbei — auch an Client B1.
            List<SseEvent> eventsB1 = clientB1.receivedEvents();
            assertEquals(0, eventsB1.size(), "Client in Raum B hat Nachrichten bekommen");
        }
    }

    /**
     * Prüft die Ereignisse eines Clients: richtige Anzahl, keine ID doppelt,
     * id: gleich der id im JSON, und pro Absender aufsteigende Inhalte.
     */
    private void assertExactlyOnceAndInOrder(List<SseEvent> events, int expectedCount) throws Exception {
        assertEquals(expectedCount, events.size(), "Anzahl Ereignisse");

        Set<String> distinctIds = new HashSet<>();
        Map<String, Integer> lastContentBySender = new HashMap<>();
        for (SseEvent event : events) {
            distinctIds.add(event.id());

            JsonNode json = objectMapper.readTree(event.data());
            String idInJson = readText(json, "id");
            assertEquals(event.id(), idInJson, "id: und id im JSON verschieden");

            String sender = readText(json, "senderId");
            String contentText = readText(json, "content");
            int content = Integer.parseInt(contentText);
            Integer lastContent = lastContentBySender.get(sender);
            if (lastContent != null) {
                assertTrue(content > lastContent, "Reihenfolge von " + sender + ": " + lastContent + " vor " + content);
            }
            lastContentBySender.put(sender, content);
        }
        assertEquals(expectedCount, distinctIds.size(), "Ereignisse doppelt");
    }

    /**
     * Schickt die Nachrichten eines Absenders nacheinander über POST
     * /messages. Inhalt ist eine laufende Nummer, damit die Reihenfolge
     * prüfbar ist. Jede Nachricht wartet auf ihre 202, wie ein echter Client.
     */
    private void sendMessages(String baseUrl, UUID roomId, String senderId) throws Exception {
        URI uri = URI.create(baseUrl + "/messages");
        for (int i = 1; i <= MESSAGES_PER_SENDER; i++) {
            String body = "{\"roomId\":\"" + roomId + "\",\"senderId\":\"" + senderId
                    + "\",\"senderName\":\"" + senderId + "\",\"content\":\"" + i + "\"}";
            HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(body);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);
            requestBuilder.header("Content-Type", "application/json");
            requestBuilder.POST(bodyPublisher);
            HttpRequest request = requestBuilder.build();

            HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();
            HttpResponse<String> response = httpClient.send(request, bodyHandler);
            assertEquals(202, response.statusCode(), "POST /messages: " + response.body());
        }
    }

    /** Holt ein Textfeld aus dem JSON eines Ereignisses. */
    private static String readText(JsonNode json, String fieldName) {
        JsonNode field = json.get(fieldName);
        return field.asText();
    }

    /**
     * Startet eine Instanz des chat-service gegen den Test-RabbitMQ.
     *
     * Die Einstellungen gehen als Kommandozeilen-Argumente hinein. Grund:
     * application.yml setzt spring.rabbitmq.host selbst; Argumente von der
     * Kommandozeile haben in Spring Vorrang vor application.yml.
     * server.port=0 heisst: freien Port wählen, damit beide nebeneinander laufen.
     */
    private static ConfigurableApplicationContext startInstance() {
        String host = "--spring.rabbitmq.host=" + rabbitMq.getHost();
        String port = "--spring.rabbitmq.port=" + rabbitMq.getAmqpPort();
        String username = "--spring.rabbitmq.username=" + rabbitMq.getAdminUsername();
        String password = "--spring.rabbitmq.password=" + rabbitMq.getAdminPassword();
        String serverPort = "--server.port=0";

        SpringApplicationBuilder builder = new SpringApplicationBuilder(ChatServiceApplication.class);
        return builder.run(host, port, username, password, serverPort);
    }

    /** Die Adresse, unter der eine Instanz erreichbar ist, mit ihrem zufälligen Port. */
    private static String baseUrlOf(ConfigurableApplicationContext instance) {
        Environment environment = instance.getEnvironment();
        String port = environment.getProperty("local.server.port");
        return "http://localhost:" + port;
    }
}
