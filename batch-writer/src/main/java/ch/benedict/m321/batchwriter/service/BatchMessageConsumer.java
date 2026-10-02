package ch.benedict.m321.batchwriter.service;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import ch.benedict.m321.batchwriter.repository.MessageBatchRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Konsumiert Nachrichten aus der Queue chat.persist und sammelt sie in einem Puffer.
 *
 * Sobald 500 Nachrichten erreicht sind oder 200 Millisekunden vergangen sind,
 * wird der gesamte Puffer in einem einzigen Bulk-Insert in die Datenbank geschrieben.
 *
 * Erst NACH erfolgreichem Schreiben bestaetigen wir den Erhalt bei RabbitMQ (basicAck).
 * Dadurch wird At-least-once gewaehrleistet: Stuerzt der Dienst vor dem Schreiben ab,
 * liefert RabbitMQ die Nachrichten erneut aus.
 *
 * Was nie gespeichert werden kann (kein JSON, Pflichtfeld fehlt, Datenbank lehnt die
 * Daten ab), lehnt er mit requeue=false ab. RabbitMQ legt es dann in chat.dlq.
 * Sonst wuerde eine einzige kaputte Nachricht ihren Stapel fuer immer blockieren.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BatchMessageConsumer {

    /** Maximale Anzahl Nachrichten in einem Stapel vor dem Schreiben. */
    private static final int BATCH_SIZE = 500;

    /** Pause nach einem Datenbankfehler, damit der Dienst nicht im Millisekundentakt Fehler produziert. */
    private static final long RETRY_PAUSE_MILLISECONDS = 1000;

    private final MessageBatchRepository messageBatchRepository;

    /** Der JSON-Leser von Spring Boot. Er kennt UUID und Instant bereits. */
    private final ObjectMapper objectMapper;

    /**
     * Interner Zwischenspeicher fuer empfangene Nachrichten zusammen mit
     * den Informationen zur RabbitMQ-Bestaetigung.
     *
     * Der Kanal gehoert dazu, weil RabbitMQ eine Nachricht nur auf dem Kanal
     * bestaetigen laesst, auf dem sie zugestellt wurde.
     */
    private record QueuedMessage(ChatMessage message, Channel channel, long deliveryTag) {
    }

    /** Die Nachrichten, die noch nicht in der Datenbank sind. Zugriff nur in synchronized (buffer). */
    private final List<QueuedMessage> buffer = new ArrayList<>();

    /**
     * Nimmt eine einzelne Nachricht aus der Queue chat.persist entgegen und
     * legt sie in den Puffer.
     *
     * Die Methode bekommt die rohe Nachricht und liest das JSON selbst. Scheitert
     * das, lehnt sie genau diese eine Nachricht ab. Der JSON-Konverter von Spring
     * wuerde bei einem Fehler alle unbestaetigten Nachrichten des Kanals ablehnen,
     * also auch gueltige, die noch im Puffer warten (Spezifikation 2.4).
     *
     * @param amqpMessage die Nachricht, wie sie aus der Queue kommt
     * @param channel     der AMQP-Kanal fuer das spaetere Bestaetigen
     */
    @RabbitListener(queues = QueueNames.PERSIST_QUEUE, containerFactory = "batchContainerFactory")
    public void onMessage(Message amqpMessage, Channel channel) {
        MessageProperties properties = amqpMessage.getMessageProperties();
        long deliveryTag = properties.getDeliveryTag();

        ChatMessage message = readChatMessage(amqpMessage);
        boolean complete = isComplete(message);
        if (!complete) {
            rejectToDeadLetterQueue(channel, deliveryTag);
            return;
        }

        boolean shouldFlush = false;

        synchronized (buffer) {
            QueuedMessage queued = new QueuedMessage(message, channel, deliveryTag);
            buffer.add(queued);

            int currentSize = buffer.size();
            if (currentSize >= BATCH_SIZE) {
                shouldFlush = true;
            }
        }

        if (shouldFlush) {
            flush();
        }
    }

    /**
     * Liest den Body als JSON in eine ChatMessage.
     *
     * Header wie __TypeId__ spielen keine Rolle: Der Zieltyp steht hier im Code.
     *
     * @return die Nachricht, oder null, wenn der Body kein passendes JSON ist
     */
    private ChatMessage readChatMessage(Message amqpMessage) {
        byte[] body = amqpMessage.getBody();
        try {
            return objectMapper.readValue(body, ChatMessage.class);
        } catch (IOException invalidJson) {
            log.warn("Message is not valid JSON, moving it to {}: {}",
                    QueueNames.DEAD_LETTER_QUEUE, invalidJson.getMessage());
            return null;
        }
    }

    /**
     * Prueft, ob alle sechs Pflichtfelder vorhanden sind.
     *
     * Der chat-service validiert zwar, aber jeder mit Zugang zum Broker kann direkt
     * in chat.persist schreiben (Szenario S5 tut genau das). Eine Nachricht ohne
     * Pflichtfeld wuerde an NOT NULL scheitern, und zwar bei jedem Versuch.
     */
    private boolean isComplete(ChatMessage message) {
        if (message == null) {
            return false;
        }
        boolean complete = message.id() != null
                && message.roomId() != null
                && message.senderId() != null
                && message.senderName() != null
                && message.content() != null
                && message.sentAt() != null;
        if (!complete) {
            log.warn("Message {} is missing a required field, moving it to {}",
                    message.id(), QueueNames.DEAD_LETTER_QUEUE);
        }
        return complete;
    }

    /**
     * Zeitgesteuertes Entleeren des Puffers alle 200 Millisekunden.
     * Stellt sicher, dass Nachrichten auch bei geringer Last zeitnah
     * in der Datenbank landen und nicht ewig im Puffer warten.
     */
    @Scheduled(fixedRate = 200)
    public void scheduledFlush() {
        flush();
    }

    /**
     * Schreibt den aktuellen Inhalt des Puffers in die Datenbank und
     * bestaetigt alle enthaltenen Nachrichten bei RabbitMQ.
     *
     * synchronized, weil zwei Threads flush() aufrufen: der Timer und der
     * Listener bei vollem Puffer. Es soll immer nur ein Stapel gleichzeitig
     * geschrieben werden.
     */
    public synchronized void flush() {
        List<QueuedMessage> batch = takeAllFromBuffer();
        if (batch.isEmpty()) {
            return;
        }

        List<ChatMessage> chatMessages = extractChatMessages(batch);
        log.debug("Flushing batch of {} messages to database", batch.size());

        try {
            messageBatchRepository.saveBatch(chatMessages);
        } catch (DataIntegrityViolationException dataError) {
            // Spezifikation 4.7: Die Datenbank lehnt mindestens eine Nachricht ab. Welche, sagt
            // sie nicht. Deshalb einzeln schreiben, damit nur die kaputte in die DLQ geht.
            log.warn("Batch of {} messages contains data the database rejects, saving them one by one",
                    batch.size());
            saveOneByOne(batch);
            return;
        } catch (RuntimeException databaseError) {
            // Szenario S7: Datenbank nicht erreichbar. Nichts bestaetigen, alles zurueck in die Queue.
            log.warn("Database not reachable, {} messages go back to the queue: {}",
                    batch.size(), databaseError.getMessage());
            pauseBeforeRetry();
            requeueAll(batch);
            return;
        }

        acknowledgeAll(batch);
    }

    /**
     * Schreibt die Nachrichten eines gescheiterten Stapels einzeln, jede in ihrer
     * eigenen Transaktion. Das ist teuer, kommt aber nur bei kaputten Daten vor.
     */
    private void saveOneByOne(List<QueuedMessage> batch) {
        for (QueuedMessage queued : batch) {
            saveSingleMessage(queued);
        }
    }

    /**
     * Schreibt eine einzelne Nachricht und entscheidet, was RabbitMQ erfahren soll:
     * gespeichert (ACK), nie speicherbar (DLQ) oder Datenbank gerade weg (zurueck in die Queue).
     */
    private void saveSingleMessage(QueuedMessage queued) {
        ChatMessage message = queued.message();
        Channel channel = queued.channel();
        long deliveryTag = queued.deliveryTag();

        List<ChatMessage> singleMessage = new ArrayList<>();
        singleMessage.add(message);

        try {
            messageBatchRepository.saveBatch(singleMessage);
        } catch (DataIntegrityViolationException dataError) {
            Throwable cause = dataError.getMostSpecificCause();
            log.error("Message {} cannot be saved, moving it to {}: {}",
                    message.id(), QueueNames.DEAD_LETTER_QUEUE, cause.getMessage());
            rejectToDeadLetterQueue(channel, deliveryTag);
            return;
        } catch (RuntimeException databaseError) {
            log.warn("Database not reachable, message {} goes back to the queue: {}",
                    message.id(), databaseError.getMessage());
            requeueSingle(channel, deliveryTag);
            return;
        }

        acknowledgeSingle(channel, deliveryTag);
    }

    /**
     * Nimmt alle Nachrichten aus dem Puffer heraus und leert ihn.
     *
     * Danach kann der Listener sofort neue Nachrichten puffern, waehrend
     * dieser Stapel noch geschrieben wird.
     */
    private List<QueuedMessage> takeAllFromBuffer() {
        synchronized (buffer) {
            List<QueuedMessage> batch = new ArrayList<>(buffer);
            buffer.clear();
            return batch;
        }
    }

    /**
     * Holt aus den gepufferten Eintraegen nur die Nachrichten heraus,
     * denn das Repository braucht weder Kanal noch Delivery-Tag.
     */
    private List<ChatMessage> extractChatMessages(List<QueuedMessage> batch) {
        List<ChatMessage> chatMessages = new ArrayList<>(batch.size());
        for (QueuedMessage queued : batch) {
            ChatMessage message = queued.message();
            chatMessages.add(message);
        }
        return chatMessages;
    }

    /**
     * Bestaetigt einen gespeicherten Stapel bei RabbitMQ.
     *
     * Mit multiple=true bestaetigt ein einziger Aufruf alle Nachrichten bis zum
     * hoechsten Tag des Kanals. Das ist richtig, weil der Stapel alle Nachrichten
     * enthaelt, die dieser Kanal bis dahin geliefert hat.
     */
    private void acknowledgeAll(List<QueuedMessage> batch) {
        Map<Channel, Long> highestTagPerChannel = findHighestTagPerChannel(batch);

        for (Map.Entry<Channel, Long> entry : highestTagPerChannel.entrySet()) {
            Channel channel = entry.getKey();
            long highestTag = entry.getValue();
            try {
                channel.basicAck(highestTag, true);
            } catch (IOException acknowledgeError) {
                // RabbitMQ liefert die Nachrichten erneut, ON CONFLICT verwirft die Duplikate.
                log.error("Failed to send ACK to RabbitMQ", acknowledgeError);
            }
        }
    }

    /**
     * Gibt einen Stapel, der nicht gespeichert werden konnte, an RabbitMQ zurueck.
     *
     * requeue=true: RabbitMQ stellt die Nachrichten erneut zu, und der naechste
     * Versuch beginnt. So geht bei einem Datenbankausfall nichts verloren.
     */
    private void requeueAll(List<QueuedMessage> batch) {
        Map<Channel, Long> highestTagPerChannel = findHighestTagPerChannel(batch);

        for (Map.Entry<Channel, Long> entry : highestTagPerChannel.entrySet()) {
            Channel channel = entry.getKey();
            long highestTag = entry.getValue();
            try {
                channel.basicNack(highestTag, true, true);
            } catch (IOException requeueError) {
                // Bricht die Verbindung ab, stellt RabbitMQ unbestaetigte Nachrichten ohnehin erneut zu.
                log.error("Failed to send NACK to RabbitMQ", requeueError);
            }
        }
    }

    /**
     * Bestaetigt genau eine Nachricht (multiple=false), weil im Einzelmodus jede
     * Nachricht einen anderen Ausgang haben kann.
     */
    private void acknowledgeSingle(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (IOException acknowledgeError) {
            log.error("Failed to send ACK to RabbitMQ", acknowledgeError);
        }
    }

    /**
     * Gibt genau eine Nachricht an die Queue zurueck, damit sie spaeter erneut versucht wird.
     */
    private void requeueSingle(Channel channel, long deliveryTag) {
        try {
            channel.basicNack(deliveryTag, false, true);
        } catch (IOException requeueError) {
            log.error("Failed to send NACK to RabbitMQ", requeueError);
        }
    }

    /**
     * Lehnt genau eine Nachricht endgueltig ab.
     *
     * requeue=false: RabbitMQ stellt sie nicht erneut zu, sondern schickt sie ueber
     * das Dead-Letter-Routing der Queue chat.persist nach chat.dlq.
     */
    private void rejectToDeadLetterQueue(Channel channel, long deliveryTag) {
        try {
            channel.basicReject(deliveryTag, false);
        } catch (IOException rejectError) {
            log.error("Failed to reject message to the dead letter queue", rejectError);
        }
    }

    /**
     * Sucht pro Kanal den hoechsten Delivery-Tag im Stapel.
     *
     * Delivery-Tags zaehlen pro Kanal hoch. Mit zwei Kanaelen (zum Beispiel nach
     * einem Verbindungsabbruch) braucht jeder Kanal seine eigene Bestaetigung.
     */
    private Map<Channel, Long> findHighestTagPerChannel(List<QueuedMessage> batch) {
        Map<Channel, Long> highestTagPerChannel = new HashMap<>();

        for (QueuedMessage queued : batch) {
            Channel channel = queued.channel();
            long tag = queued.deliveryTag();
            Long highestSoFar = highestTagPerChannel.get(channel);
            if (highestSoFar == null || tag > highestSoFar) {
                highestTagPerChannel.put(channel, tag);
            }
        }
        return highestTagPerChannel;
    }

    /**
     * Wartet kurz nach einem Datenbankfehler.
     *
     * Ohne Pause wuerde RabbitMQ die Nachrichten sofort wieder liefern, und der
     * Dienst wuerde bei einem Ausfall die CPU mit Fehlversuchen auslasten.
     */
    private void pauseBeforeRetry() {
        try {
            Thread.sleep(RETRY_PAUSE_MILLISECONDS);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
