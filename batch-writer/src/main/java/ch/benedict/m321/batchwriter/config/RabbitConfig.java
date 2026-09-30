package ch.benedict.m321.batchwriter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Konfiguration von RabbitMQ fuer den batch-writer.
 *
 * Richtet die Queues ein, konfiguriert den JSON-Konverter und erstellt eine
 * ListenerContainerFactory mit manuellem Acknowledgement und prefetch=500,
 * damit Nachrichten erst nach dem erfolgreichen Datenbank-Commit bestaetigt werden.
 */
@Configuration
public class RabbitConfig {

    /**
     * Deklariert die Schreib-Queue chat.persist mit Dead-Letter-Routing.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /**
     * Deklariert die Dead-Letter-Queue fuer fehlerhafte Nachrichten.
     */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Konvertiert eingehende JSON-Nachrichten in ChatMessage-Records.
     *
     * Mit TypePrecedence.INFERRED wird der Zieltyp automatisch aus der Methodensignatur
     * des @RabbitListener abgeleitet. Dadurch koennen auch Nachrichten verarbeitet werden,
     * die keinen Spring-spezifischen __TypeId__-Header besitzen (Szenario S5).
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
        org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper typeMapper =
                new org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(org.springframework.amqp.support.converter.Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    /**
     * Erstellt die ContainerFactory fuer den BatchMessageConsumer.
     *
     * Wichtig:
     * - AcknowledgeMode.MANUAL: Wir bestaetigen (ACK) erst nach dem DB-Commit.
     * - prefetchCount=500: RabbitMQ liefert bis zu 500 unbestaetigte Nachrichten auf Vorrat.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory batchContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter jsonMessageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(500);

        return factory;
    }
}
