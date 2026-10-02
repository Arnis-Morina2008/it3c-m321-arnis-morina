package ch.benedict.m321.batchwriter.service;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import ch.benedict.m321.batchwriter.repository.MessageBatchRepository;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
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
     * @param message     die Chat-Nachricht
     * @param channel     der AMQP-Kanal fuer das spaetere Bestaetigen
     * @param deliveryTag die Nachrichtennummer auf dem Kanal
     */
    @RabbitListener(queues = QueueNames.PERSIST_QUEUE, containerFactory = "batchContainerFactory")
    public void onMessage(
            ChatMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

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
