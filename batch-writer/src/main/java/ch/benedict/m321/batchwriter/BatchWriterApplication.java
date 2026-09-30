package ch.benedict.m321.batchwriter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Startpunkt des batch-writer Dienstes.
 *
 * Dieser Dienst ist der einzige Baustein im System, der Schreibzugriff auf die
 * Datenbank hat. Er konsumiert Nachrichten aus RabbitMQ (chat.persist) und speichert
 * sie in Stapeln (Bulk-Insert) in PostgreSQL.
 *
 * Die Annotation @EnableScheduling wird benoetigt, damit der Puffer-Timer (z.B. alle
 * 200 ms flushen) automatisch im Hintergrund ausgefuehrt werden kann.
 */
@SpringBootApplication
@EnableScheduling
public class BatchWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchWriterApplication.class, args);
    }
}
