package ch.benedict.m321.batchwriter;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Prueft, dass der Spring-Kontext des batch-writer mit echten
 * Testcontainers-Verbindungen fuer RabbitMQ und PostgreSQL sauber hochfaehrt.
 */
@SpringBootTest
@Testcontainers
class BatchWriterApplicationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Test
    void contextLoads() {
        // Faellt der Kontextstart fehl, wirft Spring eine Exception und der Test wird rot.
    }
}
