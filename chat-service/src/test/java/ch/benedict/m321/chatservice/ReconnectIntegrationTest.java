package ch.benedict.m321.chatservice;

import ch.benedict.m321.chatservice.SseTestClient.SseEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pflichttest für S7: Ein Client trennt nach einer Nachricht, verpasst 50
 * und verbindet sich mit Last-Event-ID neu (Spezifikation 4.6).
 *
 * Erwartet: genau die 50 fehlenden, in ihrer Reihenfolge, keine doppelt,
 * danach kommen neue Nachrichten wieder live.
 *
 * Anders als im Szenario wartet der Test vor der Neuverbindung nicht 10 s;
 * die Wartezeit ändert am Ablauf nichts, sie macht den Test nur langsamer.
 *
 * Die Sperre zwischen Nachliefern und live prüft der erste Test nicht:
 * Bis der Client sich neu verbindet, sind alle 50 Nachrichten schon
 * verteilt. Dafür gibt es den zweiten Test, bei dem während der
 * Neuverbindung laufend Nachrichten ankommen.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ReconnectIntegrationTest {

    /** Nachrichten, die der Client während der Trennung verpasst. */
    private static final int MISSED_COUNT = 50;

    /** So lange warten die Clients höchstens auf ihre Nachrichten. */
    private static final long MAX_WAIT_MILLIS = 15000;

    /** Nachrichten, die im Naht-Test laufend gesendet werden. */
    private static final int CONTINUOUS_COUNT = 300;

    /** Nach so vielen gesendeten Nachrichten verbindet sich der Client neu, also mitten im Senden. */
    private static final int RECONNECT_AFTER_COUNT = 50;

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @LocalServerPort
    private int port;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newHttpClient();

    /** Der Ablauf aus S7, vom ersten Verbinden bis zur neuen Live-Nachricht. */
    @Test
    void replaysExactlyTheMissedMessagesThenContinuesLive() throws Exception {
        String baseUrl = "http://localhost:" + port;
        UUID roomId = UUID.randomUUID();

        String lastSeenId = receiveOneMessageAndDisconnect(baseUrl, roomId);

        for (int i = 1; i <= MISSED_COUNT; i++) {
            sendMessage(baseUrl, roomId, String.valueOf(i));
        }

        try (SseTestClient reconnected = SseTestClient.connect(baseUrl, roomId, lastSeenId)) {
            List<SseEvent> replayed = reconnected.waitForEvents(MISSED_COUNT, MAX_WAIT_MILLIS);
            assertContentsAreOneTo(replayed, MISSED_COUNT);

            sendMessage(baseUrl, roomId, String.valueOf(MISSED_COUNT + 1));
            List<SseEvent> withLive = reconnected.waitForEvents(MISSED_COUNT + 1, MAX_WAIT_MILLIS);
            assertContentsAreOneTo(withLive, MISSED_COUNT + 1);
        }
    }

    /**
     * Die Naht zwischen Nachliefern und live (Spezifikation 4.6, «Warum
     * unter einer Sperre»): Während sich der Client neu verbindet, kommen
     * laufend neue Nachrichten an. Erwartet: trotzdem genau 1 bis 300,
     * lückenlos, in Reihenfolge, keine doppelt.
     *
     * Der Test oben trifft diese Naht nicht: dort sind alle 50 Nachrichten
     * schon verteilt, bevor der Client sich neu verbindet. Er blieb grün,
     * als die Sperre im Listener fehlte. Dieser Test wird dann rot.
     */
    @Test
    void replaysWithoutGapOrDuplicateWhileMessagesKeepArriving() throws Exception {
        String baseUrl = "http://localhost:" + port;
        UUID roomId = UUID.randomUUID();

        String lastSeenId = receiveOneMessageAndDisconnect(baseUrl, roomId);

        AtomicInteger sentCount = new AtomicInteger(0);
        Thread senderThread = startSendingInBackground(baseUrl, roomId, sentCount);
        waitUntilSent(sentCount, RECONNECT_AFTER_COUNT);

        try (SseTestClient reconnected = SseTestClient.connect(baseUrl, roomId, lastSeenId)) {
            senderThread.join(MAX_WAIT_MILLIS);
            List<SseEvent> events = reconnected.waitForEvents(CONTINUOUS_COUNT, MAX_WAIT_MILLIS);
            assertEquals(CONTINUOUS_COUNT, sentCount.get(), "Nicht alle Nachrichten gesendet");
            assertContentsAreOneTo(events, CONTINUOUS_COUNT);
        }
    }

    /**
     * Schickt die Nachrichten 1 bis CONTINUOUS_COUNT in einem eigenen
     * Thread, damit der Test währenddessen neu verbinden kann. sentCount
     * zeigt, wie viele schon mit 202 angenommen wurden.
     */
    private Thread startSendingInBackground(String baseUrl, UUID roomId, AtomicInteger sentCount) {
        Runnable sending = () -> sendContinuously(baseUrl, roomId, sentCount);
        Thread senderThread = new Thread(sending);
        senderThread.start();
        return senderThread;
    }

    /**
     * Sendet eine Nachricht nach der anderen. Schlägt eine fehl, hört der
     * Thread auf; der Test merkt das an sentCount.
     */
    private void sendContinuously(String baseUrl, UUID roomId, AtomicInteger sentCount) {
        for (int i = 1; i <= CONTINUOUS_COUNT; i++) {
            try {
                sendMessage(baseUrl, roomId, String.valueOf(i));
            } catch (Exception exception) {
                return;
            }
            sentCount.set(i);
        }
    }

    /** Wartet, bis mindestens so viele Nachrichten gesendet sind. */
    private void waitUntilSent(AtomicInteger sentCount, int minimum) throws InterruptedException {
        long deadline = System.currentTimeMillis() + MAX_WAIT_MILLIS;
        while (sentCount.get() < minimum && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
    }

    /**
     * Erster Teil von S7: verbinden, eine Nachricht bekommen, trennen.
     * Gibt die ID dieser Nachricht zurück, also das, was der Browser als
     * Last-Event-ID mitschicken würde.
     */
    private String receiveOneMessageAndDisconnect(String baseUrl, UUID roomId) throws Exception {
        try (SseTestClient firstClient = SseTestClient.connect(baseUrl, roomId)) {
            sendMessage(baseUrl, roomId, "0");
            List<SseEvent> events = firstClient.waitForEvents(1, MAX_WAIT_MILLIS);
            assertEquals(1, events.size(), "Erste Nachricht nicht angekommen");
            SseEvent firstEvent = events.get(0);
            return firstEvent.id();
        }
    }

    /**
     * Prüft: genau so viele Ereignisse wie erwartet, keine ID doppelt, und
     * die Inhalte sind genau 1, 2, 3, ... in dieser Reihenfolge. Die "0" vor
     * der Trennung darf also nicht noch einmal kommen.
     */
    private void assertContentsAreOneTo(List<SseEvent> events, int expectedCount) throws Exception {
        assertEquals(expectedCount, events.size(), "Anzahl Ereignisse");

        Set<String> distinctIds = new HashSet<>();
        for (int i = 0; i < events.size(); i++) {
            SseEvent event = events.get(i);
            distinctIds.add(event.id());

            JsonNode json = objectMapper.readTree(event.data());
            JsonNode contentField = json.get("content");
            String content = contentField.asText();
            String expectedContent = String.valueOf(i + 1);
            assertEquals(expectedContent, content, "Ereignis Nummer " + (i + 1));
        }
        assertEquals(expectedCount, distinctIds.size(), "Ereignisse doppelt");
    }

    /** Schickt eine Nachricht über POST /messages und wartet auf die 202. */
    private void sendMessage(String baseUrl, UUID roomId, String content) throws Exception {
        URI uri = URI.create(baseUrl + "/messages");
        String body = "{\"roomId\":\"" + roomId + "\",\"senderId\":\"s7\","
                + "\"senderName\":\"s7\",\"content\":\"" + content + "\"}";
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
