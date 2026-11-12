package ch.benedict.m321.chatservice.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Der Nachhol-Puffer (Spezifikation 4.6): pro Raum die letzten
 * Nachrichten im Speicher, damit ein Client nach einem Verbindungsabbruch
 * genau das bekommt, was ihm fehlt.
 *
 * Warum im Speicher und nicht aus der Datenbank: die Live-Zustellung soll
 * ohne Datenbank laufen (S5), und die Datenbank hinkt dem Live-Strom
 * hinterher. Jede Instanz bekommt über den Fanout jede Nachricht, darum
 * hat jede Instanz denselben Puffer.
 *
 * Grenzen: höchstens 1000 Nachrichten pro Raum und höchstens 100 Räume.
 * Kommt ein 101. Raum dazu, fällt der Raum heraus, der am längsten nicht
 * benutzt wurde. So bleibt der Speicher bei etwa 30 MB.
 *
 * Alle Methoden sind synchronized: eine LinkedHashMap mit
 * Zugriffsreihenfolge verändert sich schon beim Lesen.
 */
@Service
public class ReplayBuffer {

    /** Nachrichten pro Raum, danach fällt die älteste heraus. */
    public static final int MAX_MESSAGES_PER_ROOM = 1000;

    /** Räume im Puffer, danach fällt der am längsten unbenutzte heraus. */
    public static final int MAX_ROOMS = 100;

    /** Eine Nachricht im Puffer: ihre ID (für id:) und der unveränderte JSON-Text (für data:). */
    public record BufferedMessage(String id, String json) {
    }

    private final Map<UUID, List<BufferedMessage>> messagesByRoom = new RoomMap();

    /**
     * Hängt eine Nachricht hinten an den Puffer ihres Raums an. Ist der
     * Raum voll, fällt die älteste Nachricht vorne heraus.
     */
    public synchronized void add(UUID roomId, String messageId, String json) {
        List<BufferedMessage> messages = messagesByRoom.get(roomId);
        if (messages == null) {
            messages = new ArrayList<>();
            messagesByRoom.put(roomId, messages);
        }
        BufferedMessage message = new BufferedMessage(messageId, json);
        messages.add(message);
        if (messages.size() > MAX_MESSAGES_PER_ROOM) {
            messages.remove(0);
        }
    }

    /**
     * Alle Nachrichten eines Raums NACH der Nachricht mit lastEventId, in
     * der Reihenfolge, in der sie angekommen sind.
     *
     * Gibt null zurück, wenn lastEventId nicht im Puffer steht (zu alt,
     * Raum herausgefallen oder Instanz neu gestartet). Das ist etwas anderes
     * als eine leere Liste: leer heisst "gefunden, aber seither kam nichts".
     */
    public synchronized List<BufferedMessage> messagesAfter(UUID roomId, String lastEventId) {
        List<BufferedMessage> messages = messagesByRoom.get(roomId);
        if (messages == null) {
            return null;
        }

        int lastEventIndex = indexOf(messages, lastEventId);
        if (lastEventIndex < 0) {
            return null;
        }

        List<BufferedMessage> missedMessages = new ArrayList<>();
        for (int i = lastEventIndex + 1; i < messages.size(); i++) {
            BufferedMessage message = messages.get(i);
            missedMessages.add(message);
        }
        return missedMessages;
    }

    /** Sucht die Stelle einer Nachricht im Puffer, -1 wenn sie nicht drin ist. */
    private int indexOf(List<BufferedMessage> messages, String messageId) {
        for (int i = 0; i < messages.size(); i++) {
            BufferedMessage message = messages.get(i);
            if (message.id().equals(messageId)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Die Tabelle Raum → Nachrichten mit der Grenze von MAX_ROOMS.
     *
     * accessOrder = true: die LinkedHashMap ordnet die Räume nach dem
     * letzten Zugriff, nicht nach dem Einfügen. Ganz vorne steht also der
     * Raum, der am längsten nicht benutzt wurde.
     *
     * removeEldestEntry ruft die LinkedHashMap nach jedem put selbst auf.
     * Gibt die Methode true zurück, wirft sie diesen vordersten Raum heraus.
     */
    private static class RoomMap extends LinkedHashMap<UUID, List<BufferedMessage>> {

        /** Startgrösse und Ladefaktor sind die Vorgabewerte von Java, nur accessOrder ist neu. */
        RoomMap() {
            super(16, 0.75f, true);
        }

        /** Wahr, sobald mehr als MAX_ROOMS Räume im Puffer sind. */
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, List<BufferedMessage>> eldest) {
            return size() > MAX_ROOMS;
        }
    }
}
