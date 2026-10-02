package ch.benedict.m321.batchwriter.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Konfiguration von RabbitMQ fuer den batch-writer.
 *
 * Richtet die Queues ein und erstellt eine ListenerContainerFactory mit manuellem
 * Acknowledgement und prefetch=500, damit Nachrichten erst nach dem erfolgreichen
 * Datenbank-Commit bestaetigt werden.
 *
 * Bewusst ohne JSON-Konverter: Der BatchMessageConsumer liest das JSON selbst,
 * damit er bei einem Fehler genau eine Nachricht ablehnen kann (Spezifikation 2.4).
 */
@Configuration
public class RabbitConfig {

    /**
     * So viele unbestaetigte Nachrichten liefert RabbitMQ hoechstens auf Vorrat.
     * Gleich gross wie ein Stapel: genug fuer einen vollen Stapel, aber nie mehr,
     * als bei einem Absturz erneut zugestellt werden muss.
     */
    private static final int PREFETCH_COUNT = 500;

    /**
     * Deklariert die Schreib-Queue chat.persist mit Dead-Letter-Routing.
     *
     * Die Argumente muessen genau gleich sein wie im chat-service. Sonst lehnt
     * RabbitMQ die zweite Deklaration mit PRECONDITION_FAILED ab.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /**
     * Deklariert die Dead-Letter-Queue fuer Nachrichten, die nie gespeichert werden koennen.
     */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Erstellt die ContainerFactory fuer den BatchMessageConsumer.
     *
     * Wichtig:
     * - AcknowledgeMode.MANUAL: Wir bestaetigen (ACK) erst nach dem DB-Commit.
     * - prefetchCount=500: RabbitMQ liefert bis zu 500 unbestaetigte Nachrichten auf Vorrat.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory batchContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(PREFETCH_COUNT);

        return factory;
    }
}
