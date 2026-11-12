package ch.benedict.m321.chatservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Legt beim Start alles an, was der Broker braucht.
 *
 * Spring meldet diese Beans beim Verbindungsaufbau an RabbitMQ. Existiert
 * eine Queue schon, passiert nichts — das Anlegen ist wiederholbar.
 */
@Configuration
public class RabbitConfig {

    /** Wartende Nachrichten in der Queue einer Instanz, bevor die älteste herausfällt. */
    public static final int DELIVERY_QUEUE_MAX_LENGTH = 900;

    /** Unbestätigte Nachrichten, die RabbitMQ einer Instanz vorab schicken darf. */
    public static final int DELIVERY_PREFETCH = 100;

    /**
     * Der Schreibweg. "durable" heisst: die Queue überlebt einen Neustart
     * des Brokers. Was der Verbraucher endgültig ablehnt, landet über den
     * Standard-Exchange ("") in der Dead-Letter-Queue.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /** Das Abstellgleis für Nachrichten, die niemand verarbeiten konnte. */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Der Zustellweg. Fanout heisst: JEDE gebundene Queue bekommt eine
     * Kopie. Das ist genau richtig, weil jede chat-service-Instanz jede
     * Nachricht sehen muss — nur sie weiss, welche ihrer Stream-Clients
     * im betroffenen Raum sitzen.
     */
    @Bean
    public FanoutExchange deliveryExchange() {
        return new FanoutExchange(QueueNames.DELIVERY_EXCHANGE, true, false);
    }

    /**
     * Die eigene Queue dieser Instanz am Zustellweg (Spezifikation 3.3).
     *
     * Jede Instanz braucht eine eigene Queue, sonst wären die Instanzen
     * Competing Consumers und jede sähe nur einen Teil der Nachrichten.
     * Die UUID im Namen entsteht genau einmal, weil Spring diese Methode
     * nur einmal aufruft. Baut Spring nach einem Verbindungsabbruch die
     * Queue neu auf, bekommt sie darum wieder denselben Namen.
     *
     * - nicht durable: ohne Instanz gibt es keine Clients für diese Kopien.
     * - exclusive: RabbitMQ löscht die Queue, sobald die Verbindung der
     *   Instanz weg ist (S4).
     * - kein auto-delete: sonst wäre die Queue samt Bindung weg, während
     *   Spring einen abgestürzten Consumer neu startet.
     * - höchstens 900 wartende Nachrichten, danach fällt die älteste heraus
     *   (drop-head). Mit dem Prefetch von 100 sind das nie mehr als 1000
     *   Nachrichten (S6, Spezifikation 3.5).
     */
    @Bean
    public Queue deliveryQueue() {
        UUID instanceId = UUID.randomUUID();
        String queueName = QueueNames.DELIVERY_QUEUE_PREFIX + instanceId;
        return QueueBuilder.nonDurable(queueName)
                .exclusive()
                .maxLength(DELIVERY_QUEUE_MAX_LENGTH)
                .overflow(QueueBuilder.Overflow.dropHead)
                .build();
    }

    /**
     * Hängt die eigene Queue an den Fanout-Exchange. Ohne diese Bindung
     * verwirft RabbitMQ die Kopie für die Live-Zustellung.
     */
    @Bean
    public Binding deliveryBinding(Queue deliveryQueue, FanoutExchange deliveryExchange) {
        return BindingBuilder.bind(deliveryQueue).to(deliveryExchange);
    }

    /**
     * Eigene Einstellungen für den Listener am Zustellweg (Spezifikation 3.3, 3.5).
     *
     * - genau ein Consumer: nur so kommen die Nachrichten in der Reihenfolge
     *   der Queue bei den Clients an (S3).
     * - Prefetch 100: so viele unbestätigte Nachrichten darf RabbitMQ der
     *   Instanz vorab schicken. Zusammen mit den 900 aus deliveryQueue()
     *   bleibt die Queue einer hängenden Instanz bei höchstens 1000.
     * - nicht zurücklegen: wirft der Listener unerwartet eine Exception,
     *   würde RabbitMQ die Nachricht sonst endlos neu zustellen.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory deliveryListenerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setPrefetchCount(DELIVERY_PREFETCH);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }

    /**
     * Nachrichten gehen als JSON über die Leitung, nicht als serialisiertes
     * Java-Objekt. Nur so kann später ein Dienst in einer anderen Sprache
     * mitlesen.
     *
     * Der ObjectMapper kommt von Spring Boot und kann bereits Instant im
     * ISO-8601-Format schreiben.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
