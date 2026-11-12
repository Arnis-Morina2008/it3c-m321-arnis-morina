package ch.benedict.m321.chatservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Ein einfacher Client für GET /stream, nur für Tests.
 *
 * Er macht dasselbe wie curl -N im Prüfskript: Verbindung öffnen, Zeilen
 * lesen, aus id: und data: Ereignisse bauen. Gelesen wird in einem eigenen
 * Thread, damit der Test währenddessen Nachrichten senden kann.
 */
public class SseTestClient implements AutoCloseable {

    /** Ein empfangenes Ereignis: die id:-Zeile und die data:-Zeile. */
    public record SseEvent(String id, String data) {
    }

    private final List<SseEvent> events = new ArrayList<>();

    private final InputStream body;

    /** Erzeugt wird der Client nur über connect(), damit er immer verbunden ist. */
    private SseTestClient(InputStream body) {
        this.body = body;
    }

    /**
     * Öffnet einen Stream für einen Raum und wartet, bis der Server mit 200
     * geantwortet hat. Danach ist der Client in der Raumtabelle eingetragen
     * und bekommt jede neue Nachricht.
     */
    public static SseTestClient connect(String baseUrl, UUID roomId) throws Exception {
        return connect(baseUrl, roomId, null);
    }

    /**
     * Wie connect(baseUrl, roomId), aber als Neuverbindung: der Header
     * Last-Event-ID sagt dem Server, welche Nachricht der Client zuletzt
     * gesehen hat. So macht es auch der Browser nach einem Abbruch.
     * Mit lastEventId = null wird kein Header geschickt.
     */
    public static SseTestClient connect(String baseUrl, UUID roomId, String lastEventId) throws Exception {
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        clientBuilder.version(HttpClient.Version.HTTP_1_1);
        HttpClient httpClient = clientBuilder.build();

        URI uri = URI.create(baseUrl + "/stream?roomId=" + roomId);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);
        requestBuilder.GET();
        if (lastEventId != null) {
            requestBuilder.header("Last-Event-ID", lastEventId);
        }
        HttpRequest request = requestBuilder.build();

        HttpResponse.BodyHandler<InputStream> bodyHandler = HttpResponse.BodyHandlers.ofInputStream();
        HttpResponse<InputStream> response = httpClient.send(request, bodyHandler);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Stream not opened, status " + response.statusCode());
        }

        SseTestClient client = new SseTestClient(response.body());
        Thread readerThread = new Thread(client::readLines);
        readerThread.setDaemon(true);
        readerThread.start();
        return client;
    }

    /**
     * Wartet, bis mindestens so viele Ereignisse da sind, oder bis die Zeit
     * abgelaufen ist. Gibt in beiden Fällen alle bisherigen Ereignisse
     * zurück; ob es genug sind, prüft der Test.
     */
    public List<SseEvent> waitForEvents(int expectedCount, long maxWaitMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        while (System.currentTimeMillis() < deadline) {
            List<SseEvent> received = receivedEvents();
            if (received.size() >= expectedCount) {
                return received;
            }
            Thread.sleep(100);
        }
        return receivedEvents();
    }

    /** Kopie aller bisher empfangenen Ereignisse. */
    public List<SseEvent> receivedEvents() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    /** Trennt die Verbindung, wie wenn der Browser-Tab geschlossen wird. */
    @Override
    public void close() throws IOException {
        body.close();
    }

    /**
     * Liest Zeile für Zeile nach den Regeln von Server-Sent Events: id: und
     * data: sammeln, eine leere Zeile schliesst das Ereignis ab. Zeilen mit
     * ":" am Anfang sind Kommentare (connected, keepalive) und werden
     * übersprungen.
     */
    private void readLines() {
        InputStreamReader streamReader = new InputStreamReader(body, StandardCharsets.UTF_8);
        try (BufferedReader reader = new BufferedReader(streamReader)) {
            String id = null;
            String data = null;
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith("id:")) {
                    id = valueAfter(line, "id:");
                } else if (line.startsWith("data:")) {
                    data = valueAfter(line, "data:");
                } else if (line.isEmpty() && data != null) {
                    addEvent(id, data);
                    id = null;
                    data = null;
                }
                line = reader.readLine();
            }
        } catch (IOException exception) {
            // Verbindung wurde geschlossen, das Lesen endet hier.
        }
    }

    /** Speichert ein fertiges Ereignis. */
    private void addEvent(String id, String data) {
        SseEvent event = new SseEvent(id, data);
        synchronized (events) {
            events.add(event);
        }
    }

    /**
     * Der Wert nach dem Feldnamen. Ein Leerzeichen direkt nach dem
     * Doppelpunkt gehört laut SSE-Standard nicht zum Wert.
     */
    private static String valueAfter(String line, String fieldName) {
        String value = line.substring(fieldName.length());
        if (value.startsWith(" ")) {
            value = value.substring(1);
        }
        return value;
    }
}
