package ch.benedict.m321.chatservice.dto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft die Feldregeln der eingehenden Nachricht.
 * Diese Regeln sind die Grenze des Dienstes: was hier durchkommt,
 * gilt danach als gültig.
 */
class SendMessageRequestTest {

    private Validator validator;

    /** Baut vor jedem Test den Validator, der die Annotationen wie Spring auswertet. */
    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        this.validator = factory.getValidator();
    }

    /** Eine vollständige Nachricht hat keine Regelverletzung. */
    @Test
    void acceptsCompleteRequest() {
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID(), "anna", "Anna Muster", "Hallo zusammen");

        Set<ConstraintViolation<SendMessageRequest>> violations = validator.validate(request);

        assertTrue(violations.isEmpty());
    }

    /** Nur Leerzeichen als Inhalt verletzt genau eine Regel (@NotBlank). */
    @Test
    void rejectsBlankContent() {
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID(), "anna", "Anna Muster", "   ");

        Set<ConstraintViolation<SendMessageRequest>> violations = validator.validate(request);

        assertEquals(1, violations.size());
    }

    /** Ohne Raum weiss niemand, wohin die Nachricht gehört: genau eine Regelverletzung. */
    @Test
    void rejectsMissingRoomId() {
        SendMessageRequest request = new SendMessageRequest(
                null, "anna", "Anna Muster", "Hallo");

        Set<ConstraintViolation<SendMessageRequest>> violations = validator.validate(request);

        assertEquals(1, violations.size());
    }
}
