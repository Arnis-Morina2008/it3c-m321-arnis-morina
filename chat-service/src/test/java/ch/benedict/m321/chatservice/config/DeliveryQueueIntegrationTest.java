package ch.benedict.m321.chatservice.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft die eigene Queue einer chat-service-Instanz am Zustellweg
 * (Spezifikation 3.3 und 3.5) gegen einen ECHTEN RabbitMQ.
 *
 * Geprüft wird mit rabbitmqctl im Container, also mit dem, was der Broker
 * wirklich angelegt hat. Die Queue-Bean in Java zeigt nur, was wir anlegen
 * WOLLTEN — ob RabbitMQ die Argumente angenommen hat, sieht man nur dort.
 */
@SpringBootTest
@Testcontainers
class DeliveryQueueIntegrationTest {

    /** So lange warten die Tests höchstens, bis der Broker den erwarteten Zustand zeigt. */
    private static final int MAX_WAIT_MILLIS = 15000;

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    @Qualifier("deliveryQueue")
    private Queue deliveryQueue;

    @Autowired
    private CachingConnectionFactory connectionFactory;

    /**
     * Der Normalfall nach dem Start: eine Queue mit dem richtigen Namen,
     * den richtigen Eigenschaften und genau einem Consumer.
     */
    @Test
    void declaresInstanceQueueWithLimits() throws Exception {
        String queueName = deliveryQueue.getName();
        assertTrue(queueName.startsWith(QueueNames.DELIVERY_QUEUE_PREFIX), "Name: " + queueName);

        String queueLine = waitForQueueWithOneConsumer(queueName);
        assertNotNull(queueLine, "Queue fehlt oder hat nicht genau einen Consumer");

        // Spalten: name, durable, auto_delete, exclusive, arguments, consumers
        String[] columns = queueLine.split("\t");
        assertEquals("false", columns[1], "durable");
        assertEquals("false", columns[2], "auto_delete");
        assertEquals("true", columns[3], "exclusive");
        assertTrue(columns[4].contains("\"x-max-length\",900"), "Argumente: " + columns[4]);
        assertTrue(columns[4].contains("\"x-overflow\",\"drop-head\""), "Argumente: " + columns[4]);
        assertEquals("1", columns[5], "consumers");

        assertTrue(isBoundToDeliveryExchange(queueName), "Bindung an chat.delivery fehlt");
    }

    /**
     * Spezifikation 3.5: Bricht die Verbindung zu RabbitMQ ab, löscht der
     * Broker die exclusive Queue. Spring baut die Verbindung neu auf und
     * legt die Queue wieder an — mit DEMSELBEN Namen, wieder gebunden und
     * wieder mit einem Consumer. Genau das passiert in S6 nach einer langen
     * Pause der Instanz.
     *
     * Dass die Queue wirklich NEU angelegt wurde, zeigt owner_pid: Eine
     * exclusive Queue gehört genau einer Verbindung. Nach dem Neuaufbau muss
     * eine andere Verbindung die Besitzerin sein. Ohne diese Prüfung bliebe
     * der Test auch grün, wenn die Queue den Abbruch einfach überlebt hätte.
     */
    @Test
    void redeclaresSameQueueAfterConnectionLoss() throws Exception {
        String queueName = deliveryQueue.getName();
        String queueLineBefore = waitForQueueWithOneConsumer(queueName);
        assertNotNull(queueLineBefore, "Queue vor dem Abbruch fehlt");
        String ownerBefore = readOwnerConnection(queueName);

        connectionFactory.resetConnection();

        String queueLineAfter = waitForQueueWithOneConsumer(queueName);
        assertNotNull(queueLineAfter, "Queue nach dem Abbruch nicht wieder da");
        assertTrue(isBoundToDeliveryExchange(queueName), "Bindung nach dem Abbruch fehlt");

        String ownerAfter = readOwnerConnection(queueName);
        assertFalse(ownerBefore.isEmpty(), "Queue hat keine Besitzer-Verbindung, ist also nicht exclusive");
        assertNotEquals(ownerBefore, ownerAfter, "Queue wurde nicht neu angelegt");
    }

    /**
     * Liest mit rabbitmqctl, welcher Verbindung die Queue gehört (owner_pid).
     * Bei einer Queue, die nicht exclusive ist, ist das Feld leer.
     */
    private String readOwnerConnection(String queueName) throws Exception {
        ExecResult result = rabbitMq.execInContainer("rabbitmqctl", "-q", "list_queues",
                "name", "owner_pid");
        String output = result.getStdout();
        String[] lines = output.split("\n");
        String linePrefix = queueName + "\t";
        for (String line : lines) {
            if (line.startsWith(linePrefix)) {
                // Erst abschneiden, dann trimmen: Bei leerem owner_pid würde
                // trim() sonst den Tab entfernen und substring abstürzen.
                String owner = line.substring(linePrefix.length());
                return owner.trim();
            }
        }
        return "";
    }

    /**
     * Fragt den Broker so lange ab, bis die Queue mit genau einem Consumer
     * da ist. Der Listener verbindet sich im Hintergrund, darum ist das
     * nicht sofort nach dem Start der Fall. Gibt die Zeile von rabbitmqctl
     * zurück oder null, wenn die Zeit abgelaufen ist.
     */
    private String waitForQueueWithOneConsumer(String queueName) throws Exception {
        long deadline = System.currentTimeMillis() + MAX_WAIT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            String queueLine = findQueueLine(queueName);
            if (queueLine != null && queueLine.endsWith("\t1")) {
                return queueLine;
            }
            Thread.sleep(200);
        }
        return null;
    }

    /** Sucht die Zeile der Queue in der Ausgabe von rabbitmqctl list_queues. */
    private String findQueueLine(String queueName) throws Exception {
        ExecResult result = rabbitMq.execInContainer("rabbitmqctl", "-q", "list_queues",
                "name", "durable", "auto_delete", "exclusive", "arguments", "consumers");
        String output = result.getStdout();
        String[] lines = output.split("\n");
        for (String line : lines) {
            if (line.startsWith(queueName + "\t")) {
                return line.trim();
            }
        }
        return null;
    }

    /** Prüft mit rabbitmqctl list_bindings, ob die Queue an chat.delivery hängt. */
    private boolean isBoundToDeliveryExchange(String queueName) throws Exception {
        ExecResult result = rabbitMq.execInContainer("rabbitmqctl", "-q", "list_bindings",
                "source_name", "destination_name");
        String output = result.getStdout();
        String expectedLine = QueueNames.DELIVERY_EXCHANGE + "\t" + queueName;
        String[] lines = output.split("\n");
        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.equals(expectedLine)) {
                return true;
            }
        }
        return false;
    }
}
