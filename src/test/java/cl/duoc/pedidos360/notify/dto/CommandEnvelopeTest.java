package cl.duoc.pedidos360.notify.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class CommandEnvelopeTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void seLeeDesdeJsonIgnorandoCamposDesconocidos() {
        String json = "{\"type\":\"EmailNotification\",\"eventId\":\"evt-1\",\"timestamp\":\"2026-09-14T12:00:00Z\","
                + "\"traceId\":\"trace-1\",\"correlationId\":\"order-1\",\"campoNuevo\":true,"
                + "\"payload\":{\"orderId\":1,\"status\":\"ACEPTADO\"}}";

        CommandEnvelope envelope = mapper.readValue(json, CommandEnvelope.class);

        assertThat(envelope.type()).isEqualTo("EmailNotification");
        assertThat(envelope.eventId()).isEqualTo("evt-1");
        assertThat(envelope.timestamp()).isEqualTo(Instant.parse("2026-09-14T12:00:00Z"));
        assertThat(envelope.payload()).containsEntry("status", "ACEPTADO");
    }
}
