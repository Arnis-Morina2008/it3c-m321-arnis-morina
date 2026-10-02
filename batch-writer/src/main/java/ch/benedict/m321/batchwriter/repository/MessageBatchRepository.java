package ch.benedict.m321.batchwriter.repository;

import ch.benedict.m321.batchwriter.dto.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Zustaendig fuer das effiziente Schreiben von Chat-Nachrichten in die PostgreSQL-Datenbank.
 *
 * Im batch-writer verwenden wir bewusst Spring JdbcTemplate statt JPA, da wir mit
 * batchUpdate echte Bulk-Inserts ausfuehren wollen. Dies reduziert bei hohen Lasten
 * (z.B. 100k Nachrichten/Minute) die Datenbanktransaktionen um einen Faktor von bis zu 500.
 *
 * Das SQL-Statement enthaelt "ON CONFLICT (id) DO NOTHING", wodurch Nachrichten bei
 * wiederholter Zustellung (At-least-once Garantie) idempotent verarbeitet werden.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class MessageBatchRepository {

    private static final String INSERT_SQL = """
            INSERT INTO message (id, room_id, sender_id, sender_name, content, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Schreibt eine Liste von Nachrichten in einem einzigen Datenbank-Aufruf (Bulk-Insert).
     *
     * Warum @Transactional: Ohne diese Annotation laeuft batchUpdate im Autocommit,
     * und der PostgreSQL-Treiber teilt einen Stapel von 500 Nachrichten in mehrere
     * Transaktionen auf. Mit @Transactional ist ein Stapel genau eine Transaktion:
     * entweder sind alle Nachrichten gespeichert oder keine. Nur dann darf der
     * Consumer danach den ganzen Stapel bestaetigen (Szenario S4).
     *
     * @param messages die Liste der zu speichernden Nachrichten
     * @return Anzahl der uebergebenen Nachrichten
     */
    @Transactional
    public int saveBatch(List<ChatMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }

        int messageCount = messages.size();

        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int index) throws SQLException {
                ChatMessage message = messages.get(index);

                preparedStatement.setObject(1, message.id());
                preparedStatement.setObject(2, message.roomId());
                preparedStatement.setString(3, message.senderId());
                preparedStatement.setString(4, message.senderName());
                preparedStatement.setString(5, message.content());

                Timestamp sentTimestamp = Timestamp.from(message.sentAt());
                preparedStatement.setTimestamp(6, sentTimestamp);
            }

            @Override
            public int getBatchSize() {
                return messageCount;
            }
        });

        log.info("Batch inserted into database with {} messages", messageCount);
        return messageCount;
    }
}
