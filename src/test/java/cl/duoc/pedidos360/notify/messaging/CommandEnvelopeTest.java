package cl.duoc.pedidos360.notify.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class CommandEnvelopeTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void seLeeDesdeJsonIgnorandoCamposDesconocidos() {
        String json = "{\"type\":\"EmailNotification\",\"eventId\":\"evt-1\",\"timestamp\":\"2026-10-17T12:00:00Z\","
                + "\"traceId\":\"trace-1\",\"correlationId\":\"order-1\",\"campoNuevo\":true,"
                + "\"payload\":{\"orderId\":1,\"status\":\"ACEPTADO\"}}";

        CommandEnvelope envelope = mapper.readValue(json, CommandEnvelope.class);

        assertThat(envelope.eventId()).isEqualTo("evt-1");
        assertThat(envelope.timestamp()).isEqualTo(Instant.parse("2026-10-17T12:00:00Z"));
        assertThat(envelope.require("status")).isEqualTo("ACEPTADO");
    }

    @Test
    void campoObligatorioFaltanteEsErrorNoRecuperable() {
        CommandEnvelope envelope = new CommandEnvelope("KitchenTicket", "evt-2", Instant.now(), null, null,
                Map.of("orderId", 1));

        assertThatThrownBy(() -> envelope.require("items"))
                .isInstanceOf(NonRecoverableMessageException.class)
                .hasMessageContaining("items");
    }

    @Test
    void fallasSimuladasDistinguenRecuperableDeNoRecuperable() {
        CommandEnvelope transient_ = new CommandEnvelope("t", "e", null, null, null,
                Map.of("simulateFailure", "transient"));
        CommandEnvelope fatal = new CommandEnvelope("t", "e", null, null, null, Map.of("simulateFailure", "fatal"));

        assertThatThrownBy(() -> SimulatedFailure.check(transient_, "SMTP"))
                .isInstanceOf(RecoverableMessageException.class);
        assertThatThrownBy(() -> SimulatedFailure.check(fatal, "SMTP"))
                .isInstanceOf(NonRecoverableMessageException.class);
    }
}
