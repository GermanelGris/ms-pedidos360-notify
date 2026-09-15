package cl.duoc.pedidos360.notify.messaging;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Envelope común de todos los comandos: type, eventId, timestamp, traceId, correlationId + payload. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommandEnvelope(String type, String eventId, Instant timestamp, String traceId, String correlationId,
                              Map<String, Object> payload) {
}
