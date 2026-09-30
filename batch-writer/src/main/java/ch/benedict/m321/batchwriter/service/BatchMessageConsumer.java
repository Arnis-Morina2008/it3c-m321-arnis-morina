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

    private final MessageBatchRepository messageBatchRepository;

    /**
     * Interner Zwischenspeicher fuer empfangene Nachrichten zusammen mit
     * den Informationen zur RabbitMQ-Bestaetigung.
     */
    private record QueuedMessage(ChatMessage message, Channel channel, long deliveryTag) {
    }

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
     */
    public synchronized void flush() {
        List<QueuedMessage> messagesToFlush;

        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            messagesToFlush = new ArrayList<>(buffer);
            buffer.clear();
        }

        int count = messagesToFlush.size();
        List<ChatMessage> chatMessages = new ArrayList<>(count);

        for (QueuedMessage queued : messagesToFlush) {
            ChatMessage message = queued.message();
            chatMessages.add(message);
        }

        log.debug("Flushing batch of {} messages to database", count);

        try {
            messageBatchRepository.saveBatch(chatMessages);

            // Nach erfolgreichem Insert ermitteln wir den hoechsten DeliveryTag pro Kanal
            Map<Channel, Long> maxTagPerChannel = new HashMap<>();
            for (QueuedMessage queued : messagesToFlush) {
                Channel channel = queued.channel();
                long tag = queued.deliveryTag();
                Long existingMax = maxTagPerChannel.get(channel);
                if (existingMax == null || tag > existingMax) {
                    maxTagPerChannel.put(channel, tag);
                }
            }

            // Mit multiple=true bestaetigen wir alle Nachrichten bis zu diesem Tag auf einmal
            for (Map.Entry<Channel, Long> entry : maxTagPerChannel.entrySet()) {
                Channel channel = entry.getKey();
                long maxTag = entry.getValue();
                channel.basicAck(maxTag, true);
            }

        } catch (Exception exception) {
            log.error("Failed to persist batch of {} messages, requeuing via NACK", count, exception);

            try {
                // Kurze Pause gegen CPU-Spins, wenn die Datenbank voruebergehend offline ist (Szenario S7)
                Thread.sleep(1000);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }

            Map<Channel, Long> maxTagPerChannel = new HashMap<>();
            for (QueuedMessage queued : messagesToFlush) {
                Channel channel = queued.channel();
                long tag = queued.deliveryTag();
                Long existingMax = maxTagPerChannel.get(channel);
                if (existingMax == null || tag > existingMax) {
                    maxTagPerChannel.put(channel, tag);
                }
            }

            for (Map.Entry<Channel, Long> entry : maxTagPerChannel.entrySet()) {
                Channel channel = entry.getKey();
                long maxTag = entry.getValue();
                try {
                    // requeue = true, damit RabbitMQ die Nachrichten erneut zulaesst
                    channel.basicNack(maxTag, true, true);
                } catch (IOException ioException) {
                    log.error("Failed to send NACK to RabbitMQ", ioException);
                }
            }
        }
    }
}
