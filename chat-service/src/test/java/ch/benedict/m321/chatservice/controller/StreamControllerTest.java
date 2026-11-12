package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.service.StreamRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Prüft nur die HTTP-Seite von GET /stream: welche Anfragen angenommen
 * werden und welcher Content-Type zurückkommt (Spezifikation 2.2).
 *
 * WebMvcTest startet nur die Web-Schicht, ohne RabbitMQ. Die echte
 * StreamRegistry wird dazugeladen und kein Mock verwendet: Spring schickt
 * Status und Content-Type erst mit dem ersten Schreiben, und geschrieben
 * wird nur mit der echten Raumtabelle (Kommentar "connected"). Ob
 * Nachrichten wirklich ankommen, prüft der TwoInstanceDeliveryIntegrationTest
 * mit echtem Broker.
 */
@WebMvcTest(StreamController.class)
@Import(StreamRegistry.class)
class StreamControllerTest {

    @Autowired
    private MockMvc mockMvc;

    /** Ohne roomId weiss der Dienst nicht, welche Nachrichten er schicken soll. */
    @Test
    void rejectsMissingRoomId() throws Exception {
        mockMvc.perform(get("/stream"))
                .andExpect(status().isBadRequest());
    }

    /** Eine roomId, die keine UUID ist, kann zu keinem Raum gehören. */
    @Test
    void rejectsRoomIdThatIsNoUuid() throws Exception {
        mockMvc.perform(get("/stream").param("roomId", "kein-raum"))
                .andExpect(status().isBadRequest());
    }

    /**
     * Gültige roomId: 200, Content-Type text/event-stream, und die
     * Verbindung bleibt offen (asyncStarted heisst: Spring hat die Antwort
     * nicht abgeschlossen, sondern hält sie für spätere Ereignisse offen).
     */
    @Test
    void opensEventStreamForValidRoomId() throws Exception {
        UUID roomId = UUID.randomUUID();

        MvcResult result = mockMvc.perform(get("/stream").param("roomId", roomId.toString()))
                .andExpect(status().isOk())
                .andExpect(request().asyncStarted())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        String contentType = response.getContentType();
        assertNotNull(contentType);
        assertTrue(contentType.startsWith(MediaType.TEXT_EVENT_STREAM_VALUE), "Content-Type: " + contentType);
    }
}
