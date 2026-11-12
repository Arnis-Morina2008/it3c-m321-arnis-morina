package ch.benedict.m321.chatservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Startpunkt des chat-service.
 *
 * Dieser Dienst ist die einzige Stelle im System, die eine Nachricht vom
 * Benutzer entgegennimmt. Er speichert nichts selbst — er reicht die
 * Nachricht an RabbitMQ weiter.
 *
 * EnableScheduling schaltet @Scheduled ein. Gebraucht wird das für das
 * Lebenszeichen an die offenen Streams (StreamRegistry.sendKeepalive).
 */
@SpringBootApplication
@EnableScheduling
public class ChatServiceApplication {

    /** Startet Spring Boot mit dem eingebetteten Webserver. */
    public static void main(String[] args) {
        SpringApplication.run(ChatServiceApplication.class, args);
    }
}
