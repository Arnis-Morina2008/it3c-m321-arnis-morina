package ch.benedict.m321.batchwriter.config;

/**
 * Die Namen der Queues an genau einer zentralen Stelle.
 *
 * Verhindert Tippfehler in Queue-Namen, die sonst erst zur Laufzeit
 * als fehlende Nachrichten auffallen wuerden.
 */
public final class QueueNames {

    /** Queue fuer den Schreibweg in die Datenbank. */
    public static final String PERSIST_QUEUE = "chat.persist";

    /** Dead-Letter-Queue fuer endgueltig nicht verarbeitbare Nachrichten. */
    public static final String DEAD_LETTER_QUEUE = "chat.dlq";

    private QueueNames() {
        // Reine Namenssammlung, wird nie instanziiert.
    }
}
