package ch.benedict.m321.batchwriter;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Prueft, dass der Spring-Kontext des batch-writer mit echten
 * Testcontainers-Verbindungen fuer RabbitMQ und PostgreSQL sauber hochfaehrt.
 */
@SpringBootTest
@Testcontainers
class BatchWriterApplicationTest {

    /** Das Schema des Stacks, relativ zum Modulordner batch-writer/. */
    private static final MountableFile SCHEMA_FILE = MountableFile.forHostPath("../postgres/init.sql");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withCopyFileToContainer(SCHEMA_FILE, "/docker-entrypoint-initdb.d/init.sql");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    /**
     * Stellt sicher, dass alle Spring-Beans erfolgreich initialisiert werden koennen.
     */
    @Test
    void contextLoads() {
        // Faellt der Kontextstart fehl, wirft Spring eine Exception und der Test wird rot.
    }
}
