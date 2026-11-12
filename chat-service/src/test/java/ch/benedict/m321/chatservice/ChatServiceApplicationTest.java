package ch.benedict.m321.chatservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Prüft, dass der Spring-Kontext überhaupt hochfährt.
 * Ein RabbitMQ wird dafür nicht gebraucht: Der Listener am Zustellweg
 * versucht zwar beim Start zu verbinden, findet er keinen Broker, versucht
 * er es aber im Hintergrund weiter. Der Kontext fährt trotzdem hoch.
 */
@SpringBootTest
class ChatServiceApplicationTest {

    /** Startet den Kontext. Schlägt der Start fehl, wird der Test rot. */
    @Test
    void contextLoads() {
        // Kein Assert nötig. Fährt der Kontext nicht hoch, wirft Spring
        // eine Exception und der Test wird rot.
    }
}
