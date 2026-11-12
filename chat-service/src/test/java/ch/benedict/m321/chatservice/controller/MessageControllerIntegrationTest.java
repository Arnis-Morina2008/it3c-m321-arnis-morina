package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.config.QueueNames;
import ch.benedict.m321.chatservice.dto.ChatMessage;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Der Weg von aussen nach innen, einmal ganz durch: HTTP rein,
 * Nachricht in der Queue raus.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class MessageControllerIntegrationTest {

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * Eine gültige Nachricht wird mit 202, ID und Zeitstempel angenommen und
     * liegt danach mit unverändertem Inhalt in chat.persist.
     */
    @Test
    void acceptsMessageAndPutsItOnTheQueue() throws Exception {
        String requestBody = """
                {
                  "roomId": "3f2b1c4e-0000-0000-0000-000000000001",
                  "senderId": "anna",
                  "senderName": "Anna Muster",
                  "content": "Hallo zusammen"
                }
                """;

        mockMvc.perform(post("/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.sentAt").exists());

        ParameterizedTypeReference<ChatMessage> targetType = new ParameterizedTypeReference<>() {
        };
        ChatMessage message = rabbitTemplate.receiveAndConvert(
                QueueNames.PERSIST_QUEUE, 5000, targetType);

        assertNotNull(message);
        assertEquals("Hallo zusammen", message.content());
        assertEquals("Anna Muster", message.senderName());
    }

    /** Eine Nachricht nur aus Leerzeichen ist keine Nachricht: der Dienst antwortet mit 400. */
    @Test
    void rejectsEmptyContent() throws Exception {
        String requestBody = """
                {
                  "roomId": "3f2b1c4e-0000-0000-0000-000000000001",
                  "senderId": "anna",
                  "senderName": "Anna Muster",
                  "content": "   "
                }
                """;

        mockMvc.perform(post("/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }
}
