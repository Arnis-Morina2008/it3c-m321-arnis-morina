package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.service.ReplayBuffer.BufferedMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit-Test des Nachhol-Puffers (Spezifikation 4.6): was nach einer ID
 * kommt, was bei unbekannter ID passiert und wie die beiden Grenzen
 * (1000 Nachrichten pro Raum, 100 Räume) greifen. Ohne Spring, ohne Broker.
 */
class ReplayBufferTest {

    private final ReplayBuffer replayBuffer = new ReplayBuffer();

    /** Alles nach der letzten gesehenen ID, in der Reihenfolge des Eingangs. */
    @Test
    void returnsMessagesAfterLastEventIdInOrder() {
        UUID roomId = UUID.randomUUID();
        replayBuffer.add(roomId, "m1", "{\"content\":\"1\"}");
        replayBuffer.add(roomId, "m2", "{\"content\":\"2\"}");
        replayBuffer.add(roomId, "m3", "{\"content\":\"3\"}");

        List<BufferedMessage> missed = replayBuffer.messagesAfter(roomId, "m1");

        assertNotNull(missed);
        assertEquals(2, missed.size());
        BufferedMessage first = missed.get(0);
        BufferedMessage second = missed.get(1);
        assertEquals("m2", first.id());
        assertEquals("{\"content\":\"2\"}", first.json());
        assertEquals("m3", second.id());
    }

    /** War die letzte gesehene die neueste Nachricht, fehlt nichts: leere Liste, nicht null. */
    @Test
    void returnsEmptyListWhenNothingWasMissed() {
        UUID roomId = UUID.randomUUID();
        replayBuffer.add(roomId, "m1", "{}");

        List<BufferedMessage> missed = replayBuffer.messagesAfter(roomId, "m1");

        assertNotNull(missed);
        assertTrue(missed.isEmpty());
    }

    /** Unbekannte ID oder unbekannter Raum: null, also "nichts nachliefern". */
    @Test
    void returnsNullForUnknownIdOrRoom() {
        UUID roomId = UUID.randomUUID();
        replayBuffer.add(roomId, "m1", "{}");

        List<BufferedMessage> unknownId = replayBuffer.messagesAfter(roomId, "gibt-es-nicht");
        UUID otherRoomId = UUID.randomUUID();
        List<BufferedMessage> unknownRoom = replayBuffer.messagesAfter(otherRoomId, "m1");

        assertNull(unknownId);
        assertNull(unknownRoom);
    }

    /** Die 1001. Nachricht eines Raums drängt die älteste hinaus. */
    @Test
    void dropsOldestMessageWhenRoomIsFull() {
        UUID roomId = UUID.randomUUID();
        int messageCount = ReplayBuffer.MAX_MESSAGES_PER_ROOM + 1;
        for (int i = 1; i <= messageCount; i++) {
            replayBuffer.add(roomId, "m" + i, "{}");
        }

        List<BufferedMessage> afterOldest = replayBuffer.messagesAfter(roomId, "m1");
        List<BufferedMessage> afterSecond = replayBuffer.messagesAfter(roomId, "m2");

        assertNull(afterOldest, "m1 hätte herausfallen müssen");
        assertNotNull(afterSecond);
        assertEquals(ReplayBuffer.MAX_MESSAGES_PER_ROOM - 1, afterSecond.size());
    }

    /**
     * Der 101. Raum drängt den Raum hinaus, der am längsten nicht benutzt
     * wurde. Raum 1 wurde zuletzt noch gelesen, darum fällt Raum 2 heraus.
     */
    @Test
    void dropsLeastRecentlyUsedRoomWhenTooManyRooms() {
        List<UUID> roomIds = new ArrayList<>();
        for (int i = 0; i < ReplayBuffer.MAX_ROOMS; i++) {
            UUID roomId = UUID.randomUUID();
            roomIds.add(roomId);
            replayBuffer.add(roomId, "m-" + i, "{}");
        }
        UUID firstRoom = roomIds.get(0);
        UUID secondRoom = roomIds.get(1);
        replayBuffer.messagesAfter(firstRoom, "m-0");

        UUID extraRoom = UUID.randomUUID();
        replayBuffer.add(extraRoom, "m-extra", "{}");

        List<BufferedMessage> firstRoomMessages = replayBuffer.messagesAfter(firstRoom, "m-0");
        List<BufferedMessage> secondRoomMessages = replayBuffer.messagesAfter(secondRoom, "m-1");
        List<BufferedMessage> extraRoomMessages = replayBuffer.messagesAfter(extraRoom, "m-extra");
        assertNotNull(firstRoomMessages, "Raum 1 wurde zuletzt benutzt und muss bleiben");
        assertNull(secondRoomMessages, "Raum 2 war am längsten unbenutzt und muss heraus");
        assertNotNull(extraRoomMessages);
    }
}
